"""
Tests for LASConverter.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import numpy as np
import pytest

from dlis_builder.converters import LASConverter
from dlis_builder.models import WellMetadata


@pytest.fixture
def simple_las(tmp_path) -> Path:
    """Minimal LAS 2.0 file with three scalar channels."""
    content = textwrap.dedent("""\
        ~VERSION INFORMATION
         VERS.                          2.0 : CWLS LOG ASCII STANDARD-VERSION 2.0
         WRAP.                           NO : ONE LINE PER DEPTH STEP
        ~WELL INFORMATION
         WELL.                      WELL_A  : Well Name
         NULL.                     -999.25  : Null Value
         STRT.M                     300.00  :
         STOP.M                     300.60  :
         STEP.M                       0.15  :
         COMP.                 Acme Energy  :
         FLD .                NORTH_BLOCK   :
        ~CURVE INFORMATION
         DEPT .M                           : Depth
         GR   .GAPI                        : Gamma Ray
         RHOB .G/CC                        : Bulk Density
        ~A
         300.00  45.2  2.31
         300.15  47.8  2.29
         300.30  44.1  2.33
         300.45 -999.25 2.27
         300.60  50.1  2.30
    """)
    p = tmp_path / "simple.las"
    p.write_text(content)
    return p


@pytest.fixture
def numbered_array_las(tmp_path) -> Path:
    """LAS 2.0 file with numbered-suffix image columns (AMP01…AMP08)."""
    curves = "\n".join(f" AMP{i:02d} .MV                           : Amplitude" for i in range(1, 9))
    rows = "\n".join(
        " {:.2f}  {}".format(300.0 + i * 0.15, "  ".join(f"{0.5 + i * 0.01:.3f}" for _ in range(8)))
        for i in range(5)
    )
    content = textwrap.dedent(f"""\
        ~VERSION INFORMATION
         VERS. 2.0 :
         WRAP.  NO :
        ~WELL INFORMATION
         WELL.  TEST_WELL  :
         NULL.  -999.25    :
        ~CURVE INFORMATION
         DEPT .M  :
        {curves}
        ~A
        {rows}
    """)
    p = tmp_path / "array.las"
    p.write_text(content)
    return p


class TestLASConverterRead:

    def test_scalar_metadata(self, simple_las):
        """Metadata extracted from LAS ~Well section."""
        conv = LASConverter()
        ds = conv.read(str(simple_las))
        assert ds.metadata.well_name == "WELL_A"
        assert ds.metadata.company == "Acme Energy"
        assert ds.metadata.field_name == "NORTH_BLOCK"

    def test_scalar_channels(self, simple_las):
        """All scalar channels are parsed with correct units."""
        ds = LASConverter().read(str(simple_las))
        names = [c.name for c in ds.channels]
        assert "DEPT" in names
        assert "GR" in names
        assert "RHOB" in names

    def test_unit_normalisation(self, simple_las):
        """LAS units are normalised to DLIS canonical values."""
        ds = LASConverter().read(str(simple_las))
        gr_ch = ds.get_channel("GR")
        assert gr_ch.unit == "gAPI"
        rhob_ch = ds.get_channel("RHOB")
        assert rhob_ch.unit == "g/cm3"

    def test_numbered_suffix_array_detection(self, numbered_array_las):
        """Numbered-suffix columns (AMP01…AMP08) are grouped into an array channel."""
        ds = LASConverter().read(str(numbered_array_las))
        array_channels = [c for c in ds.channels if c.is_array]
        assert len(array_channels) == 1
        assert array_channels[0].array_size == 8

    def test_missing_file(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            LASConverter().read(str(tmp_path / "missing.las"))


class TestLASConverterConvert:

    def test_convert_produces_dlis(self, simple_las, tmp_path):
        out = str(tmp_path / "out.dlis")
        result = LASConverter().convert(str(simple_las), out)
        assert Path(result).exists()
        assert Path(result).stat().st_size > 0

    def test_convert_metadata_override(self, simple_las, tmp_path):
        """Caller-supplied metadata overrides what was in the LAS file."""
        out = str(tmp_path / "override.dlis")
        result = LASConverter().convert(
            str(simple_las), out,
            metadata=WellMetadata(well_name="OVERRIDDEN", company="Custom Co"),
        )
        assert Path(result).exists()

    def test_convert_resolution_high(self, numbered_array_las, tmp_path):
        """High resolution produces a larger file than low resolution."""
        low_path  = str(tmp_path / "low.dlis")
        high_path = str(tmp_path / "high.dlis")
        LASConverter(image_resolution="low").convert(str(numbered_array_las), low_path)
        LASConverter(image_resolution="high").convert(str(numbered_array_las), high_path)
        assert Path(low_path).stat().st_size <= Path(high_path).stat().st_size

    def test_invalid_resolution(self):
        """'auto', 'low', 'high' are valid; anything else raises."""
        with pytest.raises(ValueError, match="image_resolution"):
            LASConverter(image_resolution="medium")

    def test_auto_resolution_accepted(self):
        """'auto' is a valid image_resolution value."""
        conv = LASConverter(image_resolution="auto")
        assert conv is not None

    def test_batch_convert(self, tmp_path, simple_las):
        """Batch mode converts all .las files in a directory."""
        las_dir = tmp_path / "las"
        las_dir.mkdir()
        for name in ["a.las", "b.las"]:
            (las_dir / name).write_text(simple_las.read_text())
        out_dir = tmp_path / "dlis"

        results = LASConverter().convert_batch(str(las_dir), output_dir=str(out_dir))
        assert len(results) == 2
        assert all(r["success"] for r in results)
        assert all(Path(r["dlis_path"]).exists() for r in results)


class TestCommonNullMasking:
    """All common LAS null sentinels must be masked to NaN, not just the declared one."""

    @pytest.fixture
    def multi_null_las(self, tmp_path) -> Path:
        """LAS file where different null variants appear across channels."""
        content = textwrap.dedent("""\
            ~VERSION INFORMATION
             VERS. 2.0 :
             WRAP.  NO :
            ~WELL INFORMATION
             WELL.  NULL_TEST :
             NULL.  -999.25   :
            ~CURVE INFORMATION
             DEPT .M    :
             GR   .GAPI : Uses -999 as null
             RHOB .G/CC : Uses -9999 as null
             SP   .MV   : Uses -285.43 as null
            ~A
             300.00   -999.00  2.31   -285.43
             300.15   47.80   -9999.0  10.5
             300.30   44.10    2.33    12.0
        """)
        p = tmp_path / "multi_null.las"
        p.write_text(content)
        return p

    def test_common_nulls_masked_to_nan(self, multi_null_las):
        """Values matching any common null sentinel are replaced with NaN."""
        ds = LASConverter().read(str(multi_null_las))
        gr_ch   = ds.get_channel("GR")
        rhob_ch = ds.get_channel("RHOB")
        sp_ch   = ds.get_channel("SP")
        assert np.isnan(gr_ch.data[0]),   "-999 must be masked"
        assert np.isnan(rhob_ch.data[1]), "-9999 must be masked"
        assert np.isnan(sp_ch.data[0]),   "-285.43 must be masked"

    def test_real_data_not_masked(self, multi_null_las):
        """Values that are NOT null sentinels must not become NaN."""
        ds = LASConverter().read(str(multi_null_las))
        gr_ch = ds.get_channel("GR")
        # 47.80 must survive unmolested
        assert not np.isnan(gr_ch.data[1])
        assert abs(gr_ch.data[1] - 47.80) < 0.01


class TestLASConverterInstanceArrayMap:
    """Instance-level array_map is applied on every read()/convert() call."""

    @pytest.fixture
    def numbered_las(self, tmp_path) -> Path:
        curves = "\n".join(f" CH{i:02d} .MV : ch" for i in range(1, 5))
        rows = "\n".join(
            f" {300 + i * 0.15:.2f}  " + "  ".join(f"{float(i):.1f}" for _ in range(4))
            for i in range(3)
        )
        content = textwrap.dedent(f"""\
            ~VERSION INFORMATION
             VERS. 2.0 :
             WRAP.  NO :
            ~WELL INFORMATION
             WELL. AMAP_TEST :
             NULL. -999.25   :
            ~CURVE INFORMATION
             DEPT .M :
            {curves}
            ~A
            {rows}
        """)
        p = tmp_path / "amap.las"
        p.write_text(content)
        return p

    def test_instance_array_map_groups_channels(self, numbered_las):
        """array_map on __init__ groups columns on every call without repeating it."""
        conv = LASConverter(array_map={"MYARRAY": ["CH01", "CH02", "CH03", "CH04"]})
        ds = conv.read(str(numbered_las))
        array_chs = [c for c in ds.channels if c.is_array]
        assert len(array_chs) == 1
        assert array_chs[0].name == "MYARRAY"
        assert array_chs[0].array_size == 4


class TestAutoResolution:
    """'auto' resolution uses float32 for wide arrays, float64 for narrow ones."""

    @pytest.fixture
    def wide_array_las(self, tmp_path) -> Path:
        """LAS with 64 image columns — should auto-select float32."""
        width = 64
        curves = "\n".join(f" IMG{i:03d} .OHM : img" for i in range(1, width + 1))
        rows = "\n".join(
            f" {300 + i * 0.15:.2f}  " + "  ".join(f"1.0" for _ in range(width))
            for i in range(3)
        )
        content = textwrap.dedent(f"""\
            ~VERSION INFORMATION
             VERS. 2.0 :
             WRAP.  NO :
            ~WELL INFORMATION
             WELL. WIDE :
             NULL. -999.25 :
            ~CURVE INFORMATION
             DEPT .M :
            {curves}
            ~A
            {rows}
        """)
        p = tmp_path / "wide.las"
        p.write_text(content)
        return p

    def test_auto_wide_array_uses_float32(self, wide_array_las):
        """Arrays with dimension >= 32 should be float32 under 'auto'."""
        from dlis_builder.models import Resolution  # noqa: PLC0415
        ds = LASConverter(image_resolution="auto").read(str(wide_array_las))
        array_chs = [c for c in ds.channels if c.is_array]
        assert len(array_chs) == 1
        assert array_chs[0].resolution == Resolution.LOW  # float32

