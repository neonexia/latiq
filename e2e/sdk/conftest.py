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

"""Fixtures for the SDK end-to-end suite.

Runs in two modes, same assertions:
  - REMOTE (CI): set LATIQ_CONTROL + LATIQ_GATEWAY to the dockerized cluster's
    control plane and query gateway. Exercises multi-node + forwarding + the nginx
    front door for real.
  - EMBEDDED (local dev): unset → an in-process single-node cluster. Validates the
    SDK-call + Arrow/pandas logic without docker. Multi-node-only tests skip.

(`test_auth.py` adds a third, AUTH, which is run on its own against the
Keycloak-backed cluster — see that file.)

CI runs must pass `--latiq-mode=<embedded|remote|auth>`; see the guard below.
"""
import os

import pytest

import latiq

REMOTE = bool(os.environ.get("LATIQ_GATEWAY"))


# ── the anti-vacuous-green guard ─────────────────────────────────────────────
#
# Most of this suite is conditional: the multi-node test skips without a cluster
# and the whole of `test_auth.py` skips without an issuer. So `pytest e2e/sdk`
# can skip EVERY test and still exit 0 — a green job that proved nothing. We have
# shipped exactly that bug before: the iceberg e2e invoked a test target that had
# been renamed, `cargo test --exact` matched nothing, exited 0, and the job stayed
# green for weeks (`verify.yml`'s `iceberg` job now greps the count for the same
# reason).
#
# `--latiq-mode` makes each CI invocation state what it is there to prove, and
# this table says what that means. It is deliberately NOT a count:
#   * `requires` — node ids that MUST have been collected AND passed. The
#     anti-vacuity lower bound (crates/latiq/tests/CLAUDE.md rule 3): a run that
#     collected nothing, or where the load-bearing test was renamed, deleted or
#     silently skipped, fails loudly.
#   * `may_skip` — the ONLY tests this mode is allowed to skip. Anything else
#     that skips is a bug (a missing dependency, an env var that stopped being
#     set, a `pytest.skip` added without thinking about the modes).
# Both are matched as `<file>::<test>` prefixes, so a whole file can be named.
# Adding an ordinary test never touches this table; adding one that SKIPS does,
# which is the point — a new skip should be a conscious, per-mode decision.
MODES = {
    "embedded": {
        # The `pip install latiq` + connect("local") user: allocate, write, read
        # back as Arrow, hand it to pandas. If that did not run, the job proved
        # nothing about the wheel.
        "requires": (
            "test_sdk_cluster.py::test_pond_lifecycle_and_handle_metadata",
            "test_sdk_cluster.py::test_arrow_to_pandas_analysis_matches_sql",
        ),
        "may_skip": (
            "test_auth.py",  # no issuer configured — the whole file self-skips
            "test_sdk_cluster.py::test_multi_node_placement_and_forwarding",
        ),
    },
    "remote": {
        # Everything embedded proves, PLUS the reason a cluster was started at
        # all: placement across nodes and greeter forwarding through the gateway.
        # Requiring it here is what stops the remote job from quietly degrading
        # into a second embedded run.
        "requires": (
            "test_sdk_cluster.py::test_pond_lifecycle_and_handle_metadata",
            "test_sdk_cluster.py::test_arrow_to_pandas_analysis_matches_sql",
            "test_sdk_cluster.py::test_multi_node_placement_and_forwarding",
        ),
        "may_skip": ("test_auth.py",),
    },
    "auth": {
        # The in-network Keycloak runner (deploy/cluster/docker-compose.yml's
        # `auth-tests-sdk`), which runs test_auth.py alone.
        "requires": (
            "test_auth.py::test_auth_discovery_document_is_real",
            "test_auth.py::test_auth_real_token_has_array_audience",
            "test_auth.py::test_auth_verified_identity_reaches_attribution",
            "test_auth.py::test_auth_streaming_read_also_carries_the_token",
            "test_auth.py::test_auth_admin_metadata_read_requires_a_token",
        ),
        # Cross-node token replay skips on a single-node auth cluster.
        "may_skip": (
            "test_auth.py::test_auth_cross_node_forwarding_replays_the_token",
        ),
    },
}


def pytest_addoption(parser):
    parser.addoption(
        "--latiq-mode",
        choices=sorted(MODES),
        default=None,
        help="What this invocation must prove (see MODES in conftest.py). CI "
        "passes it so a run that skipped everything cannot exit 0.",
    )


def pytest_configure(config):
    config._latiq_outcomes = {}


def _key(nodeid: str) -> str:
    """`…/e2e/sdk/test_x.py::test_y` → `test_x.py::test_y`, so the guard reads the
    same whether pytest was given a relative path, an absolute one, or /repo."""
    path, _, rest = nodeid.partition("::")
    return f"{path.rsplit('/', 1)[-1]}::{rest}" if rest else path.rsplit("/", 1)[-1]


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    seen = item.config._latiq_outcomes
    key = _key(item.nodeid)
    # setup-skips (module-level skipif) report at `setup`; real outcomes at `call`.
    if report.when == "call" or (report.when == "setup" and report.outcome != "passed"):
        seen[key] = report.outcome


def pytest_sessionfinish(session, exitstatus):
    mode = session.config.getoption("--latiq-mode")
    if mode is None:
        return  # local dev: run whatever you like
    spec = MODES[mode]
    seen = session.config._latiq_outcomes
    problems = []
    for want in spec["requires"]:
        got = seen.get(want)
        if got != "passed":
            problems.append(
                f"{want} must PASS in --latiq-mode={mode} but was "
                f"{got or 'never collected'}"
            )
    for key, outcome in sorted(seen.items()):
        if outcome == "skipped" and not any(
            key.startswith(ok) for ok in spec["may_skip"]
        ):
            problems.append(
                f"{key} skipped, which --latiq-mode={mode} does not sanction "
                "(add it to MODES[...]['may_skip'] only if the skip is right)"
            )
    if problems:
        for p in problems:
            print(f"::error::SDK e2e ({mode}): {p}")
        print(
            "::error::This guard exists because a suite that skips everything "
            "still exits 0 — see conftest.py."
        )
        session.exitstatus = 1


@pytest.fixture(scope="session")
def is_remote() -> bool:
    return REMOTE


@pytest.fixture(scope="session")
def db():
    if REMOTE:
        # The client knows ONE address per plane and never a pod IP: control/admin
        # on LATIQ_CONTROL, data/stream on the gateway. The greeter forwards by pond.
        return latiq.connect(
            server=os.environ["LATIQ_CONTROL"],
            query_gateway=os.environ["LATIQ_GATEWAY"],
        )
    return latiq.connect(server="local")
