"""Type stubs for `latiq` — the Python SDK (a compiled PyO3 extension). Keep in
sync with `src/lib.rs`."""
import os
from typing import Any

import pyarrow

def connect(
    server: str = ...,
    root: str | os.PathLike[str] | None = ...,
    query_gateway: str | None = ...,
) -> Database:
    """Connect to Latiq.

    `server="local"` starts an in-process single-node cluster backed by `root`
    (default `~/.latiq/local`); any other value is a remote control-plane endpoint
    (e.g. `"grpc://host:51400"`). `query_gateway` overrides the Data/Stream front
    door when it differs from `server` (else `server` is reused).
    """

class Database:
    """A connection handle. Mints/derives pond handles and drops ponds."""

    @property
    def server(self) -> str:
        """The control-plane endpoint this client is bound to."""

    def create_pond(
        self,
        name: str | None = ...,
        tier: str = ...,
        description: str = ...,
    ) -> Pond:
        """Allocate a pond and return a handle. `description` is agent-discovery text."""

    def get_pond(self, pond: str) -> Pond:
        """Fetch an existing pond's metadata and return a handle (one round-trip)."""

    def list_ponds(self) -> dict[str, dict[str, Any]]:
        """Ponds keyed by name: `{name: {pond_id, tier, node_id, description}}`."""

    def drop_pond(self, pond: str, confirm: bool = ...) -> None:
        """Drop a pond and all its data (`confirm` must be true)."""

class Pond:
    """A handle to one pond: metadata attributes + SQL."""

    @property
    def name(self) -> str: ...
    @property
    def id(self) -> str: ...
    @property
    def tier(self) -> str: ...
    @property
    def description(self) -> str: ...
    def query(self, sql: str) -> pyarrow.Table:
        """Run SQL. Reads stream back as a `pyarrow.Table` (uncapped); writes
        execute (attributed/snapshotted server-side) and return an empty table."""

    def describe(self) -> Any:
        """The pond's structured schema (tables/columns) as JSON."""

    def drop(self, confirm: bool = ...) -> None:
        """Drop this pond (`confirm` must be true)."""
