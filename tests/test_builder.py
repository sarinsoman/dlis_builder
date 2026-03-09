"""
Tests for DLISBuilder — programmatic API.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import pytest

from dlis_builder import DLISBuilder
from dlis_builder.models import (
    ChannelDef,
    FrameDef,
    OriginConfig,
    ParameterDef,
    Resolution,
    WellMetadata,
)


class TestDLISBuilderBasic:

    def test_scalar_channels_build(self, tmp_path, simple_dataset):
        """Builder writes a valid DLIS file from scalar channels."""
        from dlis_builder import DLISBuilder  # noqa: PLC0415
        builder = DLISBuilder()
        builder.set_origin(simple_dataset.metadata)
        builder.set_origin_config(simple_dataset.origin)
        for ch in simple_dataset.channels:
            builder.add_channel(ch)
        for p in simple_dataset.parameters:
            builder.add_parameter(p)

        out = str(tmp_path / "scalar.dlis")
        result = builder.build(out)

        assert Path(result).exists()
        assert Path(result).stat().st_size > 0

    def test_image_channel_low_resolution(self, tmp_path, image_dataset):
        """Image channel is written with float32 in low-res mode."""
        builder = DLISBuilder()
        builder.set_origin(image_dataset.metadata)
        for ch in image_dataset.channels:
            builder.add_channel(ch)

        out = str(tmp_path / "image_low.dlis")
        result = builder.build(out)
        assert Path(result).exists()

    def test_image_channel_high_resolution(self, tmp_path, depth_array, gr_array, image_array, well_metadata):
        """Builder respects Resolution.HIGH for array channels."""
        out = str(tmp_path / "image_high.dlis")
        result = (
            DLISBuilder()
            .set_origin(well_metadata)
            .add_channel(ChannelDef(name="DEPT", unit="m", data=depth_array))
            .add_channel(ChannelDef(name="GR", unit="gAPI", data=gr_array))
            .add_channel(ChannelDef(
                name="IMAGE", unit="ohm.m", data=image_array,
                dimension=[image_array.shape[1]], resolution=Resolution.HIGH,
            ))
            .build(out)
        )
        assert Path(result).exists()

    def test_fluent_chaining(self, tmp_path, depth_array, gr_array):
        """All builder methods return self for fluent chaining."""
        out = str(tmp_path / "chain.dlis")
        b = DLISBuilder()
        result_b = (
            b
            .set_origin(WellMetadata(well_name="CHAIN_WELL"))
            .set_origin_config(OriginConfig(producer_name="Test"))
            .add_channel(ChannelDef(name="DEPT", unit="m", data=depth_array))
            .add_channel(ChannelDef(name="GR", unit="gAPI", data=gr_array))
            .add_parameter(ParameterDef.text("WELL", "CHAIN_WELL"))
            .set_flatten_arrays(False)
        )
        assert result_b is b   # returns self
        result = b.build(out)
        assert Path(result).exists()

    def test_to_dataset_does_not_write(self, tmp_path, simple_dataset):
        """to_dataset() returns a WellDataset without creating any file."""
        builder = DLISBuilder()
        builder.set_origin(simple_dataset.metadata)
        for ch in simple_dataset.channels:
            builder.add_channel(ch)
        ds = builder.to_dataset()
        assert len(ds.channels) == len(simple_dataset.channels)
        # No file should exist
        for path in tmp_path.iterdir():
            pytest.fail(f"Unexpected file created: {path}")

    def test_validation_no_channels(self, tmp_path):
        """build() raises ValueError when no channels are defined."""
        with pytest.raises(ValueError, match="no channels"):
            DLISBuilder().build(str(tmp_path / "empty.dlis"))

    def test_validation_mismatched_length(self, tmp_path):
        """build() raises ValueError for inconsistent channel lengths."""
        with pytest.raises(ValueError, match="same number of depth samples"):
            (
                DLISBuilder()
                .add_channel(ChannelDef("DEPT", data=np.linspace(0, 100, 100), unit="m"))
                .add_channel(ChannelDef("GR",   data=np.zeros(50), unit="gAPI"))
                .build(str(tmp_path / "mismatch.dlis"))
            )

    def test_flatten_arrays(self, tmp_path, image_dataset):
        """Flatten mode creates a file (scalar elements instead of array channel)."""
        out = str(tmp_path / "flat.dlis")
        result = (
            DLISBuilder()
            .set_origin(image_dataset.metadata)
            .add_channels(image_dataset.channels)
            .set_flatten_arrays(True)
            .build(out)
        )
        assert Path(result).exists()

    def test_from_source_protocol(self, tmp_path, depth_array, gr_array, well_metadata):
        """from_source() works with any object satisfying DataSource protocol."""
        class FakeRepository:
            """Simulates a database/API source — no explicit inheritance."""
            def get_metadata(self):
                return well_metadata
            def get_channels(self):
                return [
                    ChannelDef(name="DEPT", unit="m", data=depth_array),
                    ChannelDef(name="GR", unit="gAPI", data=gr_array),
                ]
            def get_parameters(self):
                return []

        out = str(tmp_path / "from_source.dlis")
        result = DLISBuilder.from_source(FakeRepository()).build(out)
        assert Path(result).exists()

    def test_from_source_wrong_type(self):
        """from_source() raises TypeError for non-DataSource objects."""
        with pytest.raises(TypeError, match="DataSource"):
            DLISBuilder.from_source({"not": "a valid source"})

    def test_output_directory_created(self, tmp_path, simple_dataset):
        """Builder auto-creates the parent directory of the output path."""
        nested = tmp_path / "a" / "b" / "c" / "out.dlis"
        builder = (
            DLISBuilder()
            .set_origin(simple_dataset.metadata)
            .add_channels(simple_dataset.channels)
        )
        result = builder.build(str(nested))
        assert Path(result).exists()

    def test_scalar_channel_helper(self, tmp_path):
        """DLISBuilder.scalar_channel() convenience factory works."""
        depth = np.linspace(0, 100, 50)
        gr    = np.ones(50) * 42.0
        ch_d  = DLISBuilder.scalar_channel("DEPT", depth, unit="m")
        ch_gr = DLISBuilder.scalar_channel("GR", gr, unit="gAPI")
        assert ch_d.is_array is False
        assert ch_gr.is_array is False

    def test_image_channel_helper(self, tmp_path):
        """DLISBuilder.image_channel() convenience factory works."""
        data = np.ones((50, 16))
        ch = DLISBuilder.image_channel("IMG", data, unit="ohm.m")
        assert ch.is_array is True
        assert ch.array_size == 16

    def test_nan_handling(self, tmp_path, depth_array):
        """NaN values in channel data are replaced with the null sentinel."""
        gr = np.full(len(depth_array), np.nan)
        out = str(tmp_path / "nan.dlis")
        result = (
            DLISBuilder()
            .set_origin(WellMetadata(null_value=-9999.0))
            .add_channel(ChannelDef("DEPT", data=depth_array, unit="m"))
            .add_channel(ChannelDef("GR",   data=gr, unit="gAPI"))
            .build(out)
        )
        assert Path(result).exists()


class TestFromLASFactory:
    """DLISBuilder.from_las() must build correctly without TypeError/ValueError."""

    @pytest.fixture
    def simple_las(self, tmp_path):
        import textwrap  # noqa: PLC0415
        content = textwrap.dedent("""\
            ~VERSION INFORMATION
             VERS. 2.0 :
             WRAP.  NO :
            ~WELL INFORMATION
             WELL.  LAS_WELL :
             NULL.  -999.25  :
             COMP.  TestCo   :
            ~CURVE INFORMATION
             DEPT .M    :
             GR   .GAPI :
            ~A
             300.00  45.2
             300.15  47.8
             300.30  44.1
        """)
        p = tmp_path / "builder_test.las"
        p.write_text(content)
        return str(p)

    def test_from_las_default_args(self, simple_las, tmp_path):
        """from_las() with defaults must not raise TypeError/ValueError."""
        out = str(tmp_path / "from_las.dlis")
        result = DLISBuilder.from_las(simple_las).build(out)
        assert Path(result).exists()

    def test_from_las_array_map_passed(self, simple_las, tmp_path):
        """array_map kwarg is forwarded to LASConverter.read() without error."""
        out = str(tmp_path / "from_las_map.dlis")
        result = DLISBuilder.from_las(
            simple_las, array_map={}, image_resolution="low"
        ).build(out)
        assert Path(result).exists()

    def test_from_las_flatten_arrays(self, simple_las, tmp_path):
        """flatten_arrays=True is applied to the builder, not the converter."""
        out = str(tmp_path / "from_las_flat.dlis")
        result = DLISBuilder.from_las(simple_las, flatten_arrays=True).build(out)
        assert Path(result).exists()

    def test_from_las_explicit_resolution(self, simple_las, tmp_path):
        """Explicit 'low'/'high' resolution is forwarded to LASConverter."""
        out = str(tmp_path / "from_las_res.dlis")
        result = DLISBuilder.from_las(simple_las, image_resolution="high").build(out)
        assert Path(result).exists()


class TestParameterDef:
    """ParameterDef.numeric() stores float; both float and str values write correctly."""

    def test_numeric_stores_float(self):
        p = ParameterDef.numeric("BHT", 85.0, "degC", "Bottom Hole Temp")
        assert isinstance(p.value, float)
        assert p.value == 85.0

    def test_text_stores_str(self):
        p = ParameterDef.text("WELL", "WELL_A")
        assert isinstance(p.value, str)

    def test_numeric_param_written_to_dlis(self, tmp_path):
        """build() should not raise when parameters contain float values."""
        depth = np.linspace(0, 100, 10)
        out = str(tmp_path / "param_float.dlis")
        result = (
            DLISBuilder()
            .set_origin(WellMetadata())
            .add_channel(ChannelDef("DEPT", data=depth, unit="m"))
            .add_parameter(ParameterDef.numeric("BHT", 85.0, "degC"))
            .add_parameter(ParameterDef.text("TOOL", "FMI"))
            .build(out)
        )
        assert Path(result).exists()


class TestStringChannel:
    """String / categorical channels must be encoded as int32 category codes."""

    @pytest.fixture
    def depth(self):
        return np.linspace(300.0, 400.0, 50)

    def test_object_dtype_writes_without_error(self, tmp_path, depth):
        """Object-dtype channel with categorical values builds a valid DLIS file."""
        cats = np.array(
            (["Sedimentary"] * 20 + ["Igneous"] * 15 + ["Metamorphic"] * 15),
            dtype=object,
        )
        out = str(tmp_path / "str_obj.dlis")
        result = (
            DLISBuilder()
            .add_channel(ChannelDef("DEPT", data=depth, unit="m"))
            .add_channel(ChannelDef("LITH", data=cats, unit=""))
            .build(out)
        )
        assert Path(result).exists()

    def test_string_channel_mixed_with_numeric(self, tmp_path, depth):
        """String channel alongside float channels produces a valid DLIS file."""
        gr = np.random.default_rng(0).uniform(20, 150, len(depth)).astype(np.float32)
        cats = np.array(["BHI-Vug-Iso-SE"] * len(depth), dtype=object)
        out = str(tmp_path / "str_mixed.dlis")
        result = (
            DLISBuilder()
            .add_channel(ChannelDef("DEPT", data=depth, unit="m"))
            .add_channel(ChannelDef("GR",   data=gr,   unit="gAPI"))
            .add_channel(ChannelDef("TYPE", data=cats, unit=""))
            .build(out)
        )
        assert Path(result).exists()

    def test_null_entries_in_string_channel(self, tmp_path, depth):
        """None values in a string channel are encoded as category code 0."""
        cats = np.empty(len(depth), dtype=object)
        cats[:] = None
        cats[10:20] = "Carbonate"
        out = str(tmp_path / "str_null.dlis")
        result = (
            DLISBuilder()
            .add_channel(ChannelDef("DEPT", data=depth, unit="m"))
            .add_channel(ChannelDef("LITH", data=cats, unit=""))
            .build(out)
        )
        assert Path(result).exists()

    def test_all_null_string_channel(self, tmp_path, depth):
        """All-None string channel produces a valid DLIS file (all codes = 0)."""
        cats = np.empty(len(depth), dtype=object)
        cats[:] = None
        out = str(tmp_path / "str_all_null.dlis")
        result = (
            DLISBuilder()
            .add_channel(ChannelDef("DEPT", data=depth, unit="m"))
            .add_channel(ChannelDef("LITH", data=cats, unit=""))
            .build(out)
        )
        assert Path(result).exists()

    def test_unicode_fixed_dtype_channel(self, tmp_path, depth):
        """Fixed-width unicode (dtype='U20') channels are also handled."""
        cats = np.array(
            ["Sandstone"] * 25 + ["Limestone"] * 25, dtype="U20"
        )
        out = str(tmp_path / "str_unicode.dlis")
        result = (
            DLISBuilder()
            .add_channel(ChannelDef("DEPT", data=depth, unit="m"))
            .add_channel(ChannelDef("ROCK", data=cats, unit=""))
            .build(out)
        )
        assert Path(result).exists()

    def test_is_string_property(self):
        """ChannelDef.is_string returns True for object/unicode/bytes dtypes."""
        arr_obj = np.array(["a", "b"], dtype=object)
        arr_uni = np.array(["a", "b"], dtype="U10")
        arr_flt = np.array([1.0, 2.0], dtype=np.float32)

        assert ChannelDef("X", data=arr_obj, unit="").is_string is True
        assert ChannelDef("X", data=arr_uni, unit="").is_string is True
        assert ChannelDef("X", data=arr_flt, unit="").is_string is False


class TestMultiFrame:
    """Channels with different depth samplings are auto-grouped into separate DLIS frames."""

    @pytest.fixture
    def scalar_channels(self):
        n = 100
        rng = np.random.default_rng(0)
        return [
            ChannelDef("DEPT", data=np.linspace(0.0, 100.0, n), unit="m"),
            ChannelDef("GR",   data=rng.uniform(20, 150, n).astype(np.float32), unit="gAPI"),
        ]

    @pytest.fixture
    def image_channels(self):
        n = 500
        rng = np.random.default_rng(1)
        img = rng.uniform(0, 10, (n, 16)).astype(np.float32)
        return [
            ChannelDef("DEPT_IMG", data=np.linspace(0.0, 100.0, n), unit="m"),
            ChannelDef("BHI",      data=img, unit="ohm.m", dimension=[16]),
        ]

    def test_auto_group_by_depth(self, tmp_path, scalar_channels, image_channels):
        """Channels with different row counts are auto-grouped into separate DLIS frames."""
        out = str(tmp_path / "multi_frame.dlis")
        result = (
            DLISBuilder()
            .add_channels(scalar_channels)
            .add_channels(image_channels)
            .build(out)
        )
        assert Path(result).exists()
        assert Path(result).stat().st_size > 0

    def test_multi_frame_single_output_file(self, tmp_path, scalar_channels, image_channels):
        """Multi-frame export produces a single .dlis file, not multiple files."""
        out = str(tmp_path / "single_out.dlis")
        result = (
            DLISBuilder()
            .add_channels(scalar_channels)
            .add_channels(image_channels)
            .build(out)
        )
        assert result == str(Path(out).resolve())
        assert len(list(tmp_path.iterdir())) == 1

    def test_group_by_depth_false_raises(self, tmp_path, scalar_channels, image_channels):
        """group_by_depth=False raises ValueError when row counts differ."""
        out = str(tmp_path / "fail.dlis")
        with pytest.raises(ValueError, match="same number of depth samples"):
            (
                DLISBuilder()
                .add_channels(scalar_channels)
                .add_channels(image_channels)
                .build(out, group_by_depth=False)
            )

    def test_backward_compat_single_frame(self, tmp_path):
        """Single-frame builds still work without any changes (backward compat)."""
        depth = np.linspace(0.0, 100.0, 50)
        gr    = np.ones(50, dtype=np.float64)
        out   = str(tmp_path / "single.dlis")
        result = (
            DLISBuilder()
            .add_channel(ChannelDef("DEPT", data=depth, unit="m"))
            .add_channel(ChannelDef("GR",   data=gr,   unit="gAPI"))
            .build(out)
        )
        assert Path(result).exists()

    def test_framedef_dataclass_accessible(self):
        """FrameDef is publicly importable and its attributes work as expected."""
        frame = FrameDef(name="TEST_FRAME")
        assert frame.name == "TEST_FRAME"
        assert frame.channels == []

