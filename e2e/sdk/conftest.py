"""Fixtures for the SDK end-to-end suite.

Runs in two modes, same assertions:
  - REMOTE (CI): set LATIQ_CONTROL + LATIQ_GATEWAY to the dockerized cluster's
    control plane and query gateway. Exercises multi-node + forwarding + the nginx
    front door for real.
  - EMBEDDED (local dev): unset → an in-process single-node cluster. Validates the
    SDK-call + Arrow/pandas logic without docker. Multi-node-only tests skip.
"""
import os

import pytest

import latiq

REMOTE = bool(os.environ.get("LATIQ_GATEWAY"))


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
