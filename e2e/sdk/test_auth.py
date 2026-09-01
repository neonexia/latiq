# Copyright 2026 Neonexia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""SDK end-to-end against a REAL OIDC provider (Keycloak) — the AUTH mode.

The Rust suite already proves the verification *logic* exhaustively against an
in-process fake IdP (wrong audience, wrong issuer, expired, foreign signature,
`alg:none`, …). Those tests are fast and deterministic and a container cannot
write them better, so this file deliberately does NOT re-test them.

What a container CAN prove, and what this file is for: that we work against a
real provider — real discovery documents, a real `client_credentials` grant,
real JWKS key resolution, and the actual token shapes Keycloak emits (notably
`aud` as an ARRAY, which is exactly where a naive audience check breaks).

Skipped unless the stack was started with auth on (`LATIQ_AUTH_ISSUER` set), so
the ordinary EMBEDDED and REMOTE runs are untouched. Run it either:
  - in-network:  cd deploy/cluster && docker compose --env-file auth.env up -d
                 && docker compose --env-file auth.env run --rm auth-tests-sdk
  - locally:     ./dev.sh --nodes 2 --auth, then export the addresses it prints
                 (LATIQ_AUTH_ISSUER=http://localhost:8080/realms/latiq).
Every address comes from the environment, so there is one issuer URL either way.
"""
import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import pytest

import latiq

ISSUER = os.environ.get("LATIQ_AUTH_ISSUER")
pytestmark = pytest.mark.skipif(not ISSUER, reason="cluster not running with auth")

AUDIENCE = os.environ.get("LATIQ_AUTH_AUDIENCE", "latiq")
CLIENT_ID = "latiq-agent"
CLIENT_SECRET = "latiq-agent-secret"

# The control plane (Admin gRPC) and the query front door (Data/Stream gRPC).
# The container runner sets LATIQ_SERVER + LATIQ_GATEWAY; `dev.sh` prints its own.
SERVER = os.environ.get("LATIQ_SERVER") or os.environ.get(
    "LATIQ_CONTROL", "http://localhost:51400"
)
GATEWAY = os.environ.get("LATIQ_GATEWAY", SERVER)

# The claimed leaf the SDK always sends in `latiq-agent-id`. Once a token is
# verified this must NOT be what history records as the author.
CLAIMED_LEAF = "sdk"


def _name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _post_form(url: str, form: dict) -> dict:
    body = urllib.parse.urlencode(form).encode()
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def mint_token() -> str:
    """A real `client_credentials` grant against Keycloak's token endpoint.

    Retries while the container boots — the compose runner only `depends_on`
    keycloak's start, not its readiness, and Keycloak takes a few seconds to
    import the realm.
    """
    url = f"{ISSUER}/protocol/openid-connect/token"
    form = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    deadline = time.monotonic() + 180
    last = None
    while time.monotonic() < deadline:
        try:
            return _post_form(url, form)["access_token"]
        except (urllib.error.URLError, OSError, KeyError, ValueError) as e:
            last = e
            time.sleep(2)
    raise AssertionError(f"could not mint a token from {url}: {last}")


def _b64url(segment: str) -> dict:
    pad = "=" * (-len(segment) % 4)
    return json.loads(base64.urlsafe_b64decode(segment + pad))


def decode(token: str) -> tuple[dict, dict]:
    """Header + claims of a JWT. Local decode only — we never verify here; the
    server is the verifier under test."""
    header, claims, _sig = token.split(".")
    return _b64url(header), _b64url(claims)


@pytest.fixture(scope="session")
def token() -> str:
    return mint_token()


@pytest.fixture(scope="session")
def claims(token) -> dict:
    return decode(token)[1]


@pytest.fixture(scope="session")
def db(token):
    """An authenticated client: control plane + query gateway, real bearer token."""
    return latiq.connect(server=SERVER, query_gateway=GATEWAY, token=token)


@pytest.fixture(scope="session")
def anon_db():
    """The same client with no token at all.

    `token=""` — NOT `token=None`, and not an assertion about the environment.
    `None` falls back to `$LATIQ_TOKEN`, so a stray env var would quietly turn
    these negative tests into passes; a blank token means "no token" in the SDK
    (`latiq-sdk`'s `connect_with_token` trims and drops empties, precisely so an
    empty `Authorization: Bearer ` is never sent) and never consults the
    environment. This used to assert `$LATIQ_TOKEN` was unset, which was correct
    but contradicted `./dev.sh --auth`, whose banner tells you to export exactly
    that variable — following both documented workflows failed at fixture setup.
    Asking for no token is stronger than asserting nobody set one."""
    return latiq.connect(server=SERVER, query_gateway=GATEWAY, token="")


def _refused(exc: Exception) -> None:
    msg = str(exc).lower()
    assert any(
        k in msg for k in ("unauthenticated", "bearer", "token", "unauthorized")
    ), f"the rejection must say a token was the problem, got: {exc}"


# ── the real provider's discovery + token shape ──────────────────────────────
# What the fake IdP cannot exercise: a live discovery document and the claim set
# Keycloak actually emits.


def test_auth_discovery_document_is_real():
    """The issuer serves a real OIDC discovery document whose `issuer` matches
    the configured one exactly (a mismatch is what breaks verification in the
    field) and which advertises the JWKS the server fetches keys from."""
    with urllib.request.urlopen(
        f"{ISSUER}/.well-known/openid-configuration", timeout=30
    ) as r:
        doc = json.loads(r.read())
    assert doc["issuer"] == ISSUER, f"issuer mismatch: {doc['issuer']} != {ISSUER}"
    assert doc["jwks_uri"].startswith(ISSUER)
    assert "client_credentials" in doc.get("grant_types_supported", [])


def test_auth_real_token_has_array_audience(token, claims):
    """The shape assertion this whole mode exists for: Keycloak emits `aud` as a
    JSON ARRAY with our audience among several values. A naive `aud == "latiq"`
    string compare passes every fake-IdP test and fails here."""
    header, _ = decode(token)
    assert header["alg"] == "RS256" and header.get("kid"), header
    aud = claims["aud"]
    assert isinstance(aud, list), f"expected a real array audience, got {aud!r}"
    assert AUDIENCE in aud, f"{AUDIENCE!r} must be in {aud!r}"
    assert claims["iss"] == ISSUER
    assert claims["sub"], "a real token carries a subject"
    assert claims["sub"] != CLAIMED_LEAF
    assert claims["exp"] > claims["iat"]


# ── the Data/Query surface ───────────────────────────────────────────────────


# The negative Data/Stream paths (no token, garbage token) are NOT here. They are
# proven in milliseconds against the fake IdP by
# `crates/latiq/tests/query_grpc.rs::auth_rejects_an_invalid_token_when_configured`,
# `crates/latiq-auth/tests/verify.rs::auth_rejects_garbage` and
# `crates/latiq/tests/sdk_auth.rs::auth_sdk_token_is_required_and_sufficient`.
# A garbage token never reaches the IdP, so a real Keycloak adds nothing to it;
# and a missing-token test is structurally incapable of proving the SDK threads
# tokens at all — a binding that discarded every token would still pass it.


def test_auth_verified_identity_reaches_attribution(db, claims):
    """The assertion that separates "the call succeeded" from "the call was
    genuinely VERIFIED": DuckLake's commit author must be the token's subject,
    not the claimed leaf the SDK also sends. Without this the suite would pass
    on a silent fallback to claimed identity.

    This is also the tier's one positive allocate → write → read-back path, so
    the data is read back as well as the history."""
    p = db.create_pond(name=_name("attrib"), description="real-token e2e")
    try:
        p.query(sql="CREATE TABLE t(id INTEGER, label VARCHAR)")
        p.query(sql="INSERT INTO t VALUES (1,'a'),(2,'b')")
        got = p.query(sql="SELECT id, label FROM t ORDER BY id")
        assert got.num_rows == 2
        assert got.column("label").to_pylist() == ["a", "b"]

        hist = p.query(
            sql=f"SELECT author, commit_extra_info FROM ducklake_snapshots('{p.name}')"
        )
        authors = [a for a in hist.column("author").to_pylist() if a]
        assert claims["sub"] in authors, (
            f"the DuckLake author must be the token subject {claims['sub']!r}, "
            f"got {authors!r}"
        )
        assert CLAIMED_LEAF not in authors, (
            "the CLAIMED leaf must never be the author once a token is verified: "
            f"{authors!r}"
        )
        # The evidence beside the author: verified, from this issuer, with the
        # claim recorded separately so history can tell the two apart.
        extras = [
            json.loads(x) for x in hist.column("commit_extra_info").to_pylist() if x
        ]
        assert any(
            x.get("verified") is True
            and x.get("issuer") == ISSUER
            and x.get("agent_id") == CLAIMED_LEAF
            for x in extras
        ), f"commit_extra_info must carry the verified evidence: {extras!r}"
    finally:
        db.drop_pond(pond=p.name, confirm=True)


def test_auth_streaming_read_also_carries_the_token(db):
    """Stream gRPC is a second service on the front door with its own interceptor
    — easy to wire auth into Data and forget here."""
    p = db.create_pond(name=_name("stream"))
    try:
        p.query(sql="CREATE TABLE t AS SELECT range AS i FROM range(5000)")
        reader = p.query(sql="SELECT i FROM t", stream=True)
        assert sum(b.num_rows for b in reader) == 5000
    finally:
        db.drop_pond(pond=p.name, confirm=True)


def test_auth_cross_node_forwarding_replays_the_token(db, claims):
    """Token replay across the greeter hop, against the REAL gatewayed topology.

    A query for a pond this node does not own is forwarded to the owner, and the
    caller's token has to ride along — if it did not, the owner would refuse the
    hop (or, far worse, accept it as an unverified internal call). That is proven
    in-process by the Rust forwarding tests, but never before against a real
    multi-node cluster behind nginx with a real Keycloak, which is the only place
    a gateway that strips `authorization`, or an internal channel that forgets to
    re-attach it, can show up.

    Placement is random, so allocate enough ponds to spread across nodes (ported
    from `test_sdk_cluster.py::test_multi_node_placement_and_forwarding`), then
    write + read EACH through the one gateway address with a token, and check the
    attribution on a representative pond per node — a forwarded write must be
    authored by the TOKEN's subject on the far node too, not by the claimed leaf
    and not by the forwarding node.
    """
    names = [_name("authshard") for _ in range(12)]
    for nm in names:
        db.create_pond(name=nm)
    try:
        listed = db.list_ponds()
        by_node: dict[str, str] = {}
        for nm in names:
            assert nm in listed, f"{nm} missing from the tokened Admin listing"
            by_node.setdefault(listed[nm]["node_id"], nm)
        if len(by_node) < 2:
            pytest.skip(
                "single-node auth cluster — no cross-node hop to prove "
                f"(nodes seen: {sorted(by_node)})"
            )

        # Every pond is reachable + correct through the single gateway address,
        # with the token surviving whichever hop the greeter takes.
        for nm in names:
            h = db.get_pond(pond=nm)
            h.query(sql="CREATE TABLE t AS SELECT 7 AS v")
            got = h.query(sql="SELECT v FROM t")
            assert got.column("v")[0].as_py() == 7, f"forwarded query failed for {nm}"

        # One pond per node: the forwarded write is attributed to the token.
        for node_id, nm in by_node.items():
            hist = db.get_pond(pond=nm).query(
                sql=f"SELECT author FROM ducklake_snapshots('{nm}')"
            )
            authors = [a for a in hist.column("author").to_pylist() if a]
            assert claims["sub"] in authors, (
                f"on node {node_id}, pond {nm}: a forwarded write must be authored "
                f"by the token subject {claims['sub']!r}, got {authors!r}"
            )
            assert CLAIMED_LEAF not in authors, (
                f"on node {node_id}, pond {nm}: the claimed leaf must never be the "
                f"author of a verified write: {authors!r}"
            )
    finally:
        for nm in names:
            try:
                db.drop_pond(pond=nm, confirm=True)
            except RuntimeError:
                pass


# ── what this tier CANNOT reach: the gRPC 401 challenge ──────────────────────
#
# The agent suite asserts the MCP 401's `WWW-Authenticate` advertises the origin
# the client dialled — the config assertion that nginx and `LATIQ_PUBLIC_MCP_URL`
# agree. The Data/Stream equivalent exists on the wire: `data_service.rs`'s
# `unauthenticated()` attaches the same challenge to the tonic `Status`'
# trailing metadata, and `crates/latiq/tests/query_grpc.rs::auth_rejection_carries_
# the_discovery_challenge` pins it — but only in-process on loopback, where the
# dialled origin and the advertised one coincide by construction.
#
# We deliberately do NOT assert it here, because the Python SDK cannot see it.
# `latiq-sdk` maps every gRPC failure with `anyhow!("read: {}", s.message())`,
# keeping ONLY `Status::message()`; the metadata (and so the challenge) is
# dropped before the pyo3 layer turns it into a `RuntimeError`. There is nothing
# to assert on from Python short of adding a raw grpcio client and generated
# stubs to this suite, which would test grpcio's trailer handling rather than
# ours. Surfacing the challenge through the SDK is an SDK change, not a test
# change; when it lands, the assertion belongs right here.


# ── the Admin surface (operators — a different audience, same token) ─────────


def test_auth_admin_metadata_read_requires_a_token(db, anon_db):
    """`list_ponds` is Admin gRPC on the control plane, not the pond node. It is
    the operator surface and the easiest one to leave unguarded — on either side:
    the server must demand a token, and the client must actually send one on the
    Admin channel too, not just on Data/Stream.

    KEEP: this is the direct regression pin for the `pond_list` missing-token
    bug — the Admin channel was built without the token interceptor while
    Data/Stream had it, so every other auth test stayed green. Do not fold this
    into a Data/Stream test; the whole point is that it rides a different
    channel."""
    p = db.create_pond(name=_name("admin"))
    try:
        listed = db.list_ponds()
        assert p.name in listed, "a tokened operator read sees the pond"
        with pytest.raises(RuntimeError) as e:
            anon_db.list_ponds()
        _refused(e.value)
    finally:
        db.drop_pond(pond=p.name, confirm=True)
