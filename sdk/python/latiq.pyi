"""Type stubs for `latiq` — the Python SDK (a compiled PyO3 extension). Keep in
sync with `src/lib.rs`."""
import os
from typing import Any, Literal, overload

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
    """A connection handle. Mints/derives pond handles, drops ponds, and reads the
    dataset/catalog metadata."""

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

    def list_datasets(self, query: str | None = ...) -> dict[str, dict[str, Any]]:
        """Curated datasets keyed by name. `query`: None/`""` = all, `"#tag"`,
        `"prefix*"`, or a substring."""

    def list_catalogs(self, query: str | None = ...) -> dict[str, dict[str, Any]]:
        """External catalogs keyed by name (same `query` filter as `list_datasets`)."""

    def drop_pond(self, pond: str, confirm: bool = ...) -> None:
        """Drop a pond and all its data (`confirm` must be true)."""

class Pond:
    """A handle to one pond: metadata attributes + SQL + datasets/catalogs."""

    @property
    def name(self) -> str: ...
    @property
    def id(self) -> str: ...
    @property
    def tier(self) -> str: ...
    @property
    def description(self) -> str: ...
    @overload
    def query(self, sql: str, stream: Literal[False] = ...) -> pyarrow.Table:
        """Run SQL. Reads stream back as a `pyarrow.Table` (uncapped); writes
        execute (attributed/snapshotted server-side) and return an empty table."""
    @overload
    def query(self, sql: str, stream: Literal[True]) -> pyarrow.RecordBatchReader:
        """`stream=True` returns a `pyarrow.RecordBatchReader` over the batches."""

    def explain(self, sql: str) -> Any:
        """Explain a query plan (no execution)."""

    def snapshots(self) -> pyarrow.Table:
        """This pond's DuckLake snapshot history (who wrote what) as a Table."""

    def load_dataset(self, dataset: str) -> Any:
        """Load a curated dataset (by name, from `db.list_datasets()`) into this pond."""

    def describe_catalog(self, catalog: str, set: dict[str, str] | None = ...) -> Any:
        """Describe an external catalog's tables. `set`: runtime config +
        credentials (e.g. `{"token": "…"}`); never stored."""

    def pull_catalog(
        self, catalog: str, query: str, set: dict[str, str] | None = ...
    ) -> Any:
        """Pull a subset of an external catalog into a pond table. `query` is the
        materialization SQL. `set`: runtime config + credentials; never stored."""

    def describe(self) -> Any:
        """The pond's structured schema (tables/columns) as JSON."""

    def drop(self, confirm: bool = ...) -> None:
        """Drop this pond (`confirm` must be true)."""
