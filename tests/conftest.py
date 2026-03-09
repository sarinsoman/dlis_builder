"""
Shared fixtures for the dlis_builder test suite.
"""
from __future__ import annotations

import numpy as np
import pytest

from dlis_builder.models import (
    ChannelDef,
    OriginConfig,
    ParameterDef,
    Resolution,
    WellDataset,
    WellMetadata,
)


# ── Tiny in-memory dataset used across multiple test modules ──────────────────

N = 100           # depth samples
M = 8             # image elements per depth sample


@pytest.fixture
def depth_array() -> np.ndarray:
    return np.linspace(300.0, 314.85, N)


@pytest.fixture
def gr_array() -> np.ndarray:
    rng = np.random.default_rng(42)
    return rng.uniform(20.0, 120.0, N)


@pytest.fixture
def image_array() -> np.ndarray:
    rng = np.random.default_rng(7)
    return rng.uniform(0.1, 10.0, (N, M))


@pytest.fixture
def well_metadata() -> WellMetadata:
    return WellMetadata(
        well_name="TEST_WELL",
        company="Acme Energy",
        field_name="NORTH_BLOCK",
        null_value=-999.25,
    )


@pytest.fixture
def origin_config() -> OriginConfig:
    return OriginConfig(
        producer_name="Test Producer",
        product_name="dlis-builder-test",
        frame_name="TEST-FRAME",
    )


@pytest.fixture
def simple_dataset(
    depth_array, gr_array, well_metadata, origin_config
) -> WellDataset:
    return WellDataset(
        metadata=well_metadata,
        origin=origin_config,
        channels=[
            ChannelDef(name="DEPT", unit="m",    data=depth_array),
            ChannelDef(name="GR",   unit="gAPI", data=gr_array),
        ],
        parameters=[
            ParameterDef.numeric("BHT", 85.0, "degC", "Bottom Hole Temperature"),
        ],
    )


@pytest.fixture
def image_dataset(
    depth_array, gr_array, image_array, well_metadata, origin_config
) -> WellDataset:
    return WellDataset(
        metadata=well_metadata,
        origin=origin_config,
        channels=[
            ChannelDef(name="DEPT",  unit="m",     data=depth_array),
            ChannelDef(name="GR",    unit="gAPI",   data=gr_array),
            ChannelDef(name="IMAGE", unit="ohm.m",  data=image_array,
                       dimension=[M], resolution=Resolution.LOW),
        ],
    )
