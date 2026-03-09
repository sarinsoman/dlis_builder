"""Smoke test — run with: PYTHONPATH=src python tests/_smoke.py"""
import os
import sys
import tempfile

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dlis_builder import DLISBuilder, ChannelDef, WellMetadata, ParameterDef, Resolution

N = 200
rng = np.random.default_rng(42)
depth = np.linspace(300.0, 329.85, N)
gr    = rng.uniform(20, 120, N)
image = rng.uniform(0.1, 10.0, (N, 16))

with tempfile.TemporaryDirectory() as d:
    out = os.path.join(d, "test.dlis")
    path = (
        DLISBuilder()
        .set_origin(WellMetadata(well_name="API_WELL", company="Acme", null_value=-9999.0))
        .add_parameter(ParameterDef.numeric("BHT", 85.0, "degC", "Bottom Hole Temperature"))
        .add_channel(ChannelDef("DEPT",  unit="m",     data=depth))
        .add_channel(ChannelDef("GR",    unit="gAPI",  data=gr))
        .add_channel(ChannelDef("IMAGE", unit="ohm.m", data=image, dimension=[16], resolution=Resolution.LOW))
        .build(out)
    )
    size = os.path.getsize(path)
    print(f"Built: {path}  ({size:,} bytes)")

    # DataSource protocol test
    class FakeRepo:
        def get_metadata(self):
            return WellMetadata(well_name="REPO_WELL")
        def get_channels(self):
            return [
                ChannelDef("DEPT", unit="m", data=depth),
                ChannelDef("GR",   unit="gAPI", data=gr),
            ]
        def get_parameters(self):
            return []

    out2 = os.path.join(d, "from_source.dlis")
    path2 = DLISBuilder.from_source(FakeRepo()).build(out2)
    print(f"From source: {path2}  ({os.path.getsize(path2):,} bytes)")

    # Flatten mode
    out3 = os.path.join(d, "flat.dlis")
    path3 = (
        DLISBuilder()
        .set_origin(WellMetadata(well_name="FLAT_WELL"))
        .add_channel(ChannelDef("DEPT",  unit="m",    data=depth))
        .add_channel(ChannelDef("IMAGE", unit="ohm.m", data=image, dimension=[16]))
        .set_flatten_arrays(True)
        .build(out3)
    )
    print(f"Flatten mode: {path3}  ({os.path.getsize(path3):,} bytes)")

print("SMOKE TEST PASSED")
