"""
Tests for CSVConverter.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import numpy as np
import pytest

from dlis_builder.converters import CSVConverter
from dlis_builder.converters.csv import CSVLayout, detect_csv_layout
from dlis_builder.models import WellMetadata


@pytest.fixture
def simple_csv(tmp_path) -> Path:
    """A minimal depth + GR + RHOB CSV file."""
    content = textwrap.dedent("""\
        DEPTH,GR,RHOB
        300.00,45.2,2.31
        300.15,47.8,2.29
        300.30,44.1,2.33
        300.45,-999.25,2.27
        300.60,50.1,2.30
    """)
    p = tmp_path / "simple.csv"
    p.write_text(content)
    return p


@pytest.fixture
def image_csv(tmp_path) -> Path:
    """CSV with a depth column and 8 image columns."""
    header = "DEPTH," + ",".join(f"IMG_{i:03d}" for i in range(1, 9))
    rows = "\n".join(
        f"{300 + i * 0.15:.2f}," + ",".join(f"{0.5 + i * 0.01:.3f}" for _ in range(8))
        for i in range(10)
    )
    p = tmp_path / "image.csv"
    p.write_text(header + "\n" + rows)
    return p


class TestCSVConverterRead:

    def test_scalar_read(self, simple_csv):
        conv = CSVConverter(
            depth_column="DEPTH",
            column_units={"DEPTH": "m", "GR": "gAPI", "RHOB": "g/cm3"},
            null_value=-999.25,
        )
        ds = conv.read(str(simple_csv))
        assert len(ds.channels) == 3
        ch_names = [c.name for c in ds.channels]
        assert "DEPTH" in ch_names
        # NaN was substituted for null
        gr_ch = next(c for c in ds.channels if c.name == "GR")
        assert np.isnan(gr_ch.data[3])

    def test_unit_normalisation(self, simple_csv):
        conv = CSVConverter(
            depth_column="DEPTH",
            column_units={"DEPTH": "M", "GR": "GAPI", "RHOB": "G/CC"},
        )
        ds = conv.read(str(simple_csv))
        depth_ch = ds.get_channel("DEPTH")
        assert depth_ch.unit == "m"
        gr_ch = ds.get_channel("GR")
        assert gr_ch.unit == "gAPI"
        rhob_ch = ds.get_channel("RHOB")
        assert rhob_ch.unit == "g/cm3"

    def test_array_grouping(self, image_csv):
        members = [f"IMG_{i:03d}" for i in range(1, 9)]
        conv = CSVConverter(
            depth_column="DEPTH",
            array_columns={"IMAGE": members},
        )
        ds = conv.read(str(image_csv))
        img_ch = ds.get_channel("IMAGE")
        assert img_ch is not None
        assert img_ch.is_array is True
        assert img_ch.array_size == 8

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            CSVConverter().read(str(tmp_path / "nonexistent.csv"))

    def test_missing_depth_column_raises(self, simple_csv):
        with pytest.raises(ValueError, match="not found in CSV"):
            CSVConverter(depth_column="NONEXISTENT").read(str(simple_csv))


class TestCSVConverterConvert:

    def test_convert_produces_dlis(self, simple_csv, tmp_path):
        out = str(tmp_path / "out.dlis")
        result = CSVConverter(
            depth_column="DEPTH",
            column_units={"DEPTH": "m", "GR": "gAPI"},
        ).convert(
            str(simple_csv), out,
            metadata=WellMetadata(well_name="CSV_WELL", company="Acme"),
        )
        assert Path(result).exists()
        assert Path(result).stat().st_size > 0

    def test_batch_convert(self, tmp_path):
        """Batch convert finds all .csv files and converts them."""
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        out_dir = tmp_path / "dlis"

        for name in ["a.csv", "b.csv", "c.csv"]:
            (csv_dir / name).write_text("DEPTH,GR\n300.0,50.0\n300.15,55.0\n")

        conv = CSVConverter(depth_column="DEPTH")
        results = conv.convert_batch(str(csv_dir), output_dir=str(out_dir))

        assert len(results) == 3
        assert all(r["success"] for r in results)
        assert all(Path(r["dlis_path"]).exists() for r in results)

    def test_batch_max_workers_param(self, tmp_path):
        """max_workers is accepted and does not change the result set."""
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        out_dir = tmp_path / "dlis"

        for name in ["x.csv", "y.csv"]:
            (csv_dir / name).write_text("DEPTH,GR\n300.0,50.0\n300.15,55.0\n")

        conv = CSVConverter(depth_column="DEPTH")
        results = conv.convert_batch(str(csv_dir), output_dir=str(out_dir), max_workers=2)

        assert len(results) == 2
        assert all(r["success"] for r in results)


@pytest.fixture
def csv_with_units(tmp_path) -> Path:
    """CSV where row 1 is column names and row 2 is a unit row."""
    content = textwrap.dedent("""\
        DEPTH,GR,RHOB
        m,gAPI,g/cm3
        300.00,45.2,2.31
        300.15,47.8,2.29
        300.30,44.1,2.33
    """)
    p = tmp_path / "units.csv"
    p.write_text(content)
    return p


@pytest.fixture
def csv_no_units(tmp_path) -> Path:
    """Standard CSV with no unit row."""
    content = textwrap.dedent("""\
        DEPTH,GR,RHOB
        300.00,45.2,2.31
        300.15,47.8,2.29
    """)
    p = tmp_path / "no_units.csv"
    p.write_text(content)
    return p


class TestDetectCSVLayout:

    def test_detects_unit_row(self, csv_with_units):
        layout = detect_csv_layout(str(csv_with_units))
        assert layout.unit_row == 1
        assert layout.first_data_row == 2
        assert layout.detected_units.get("DEPTH") == "m"
        assert layout.detected_units.get("GR") == "gAPI"

    def test_no_unit_row(self, csv_no_units):
        layout = detect_csv_layout(str(csv_no_units))
        assert layout.unit_row is None
        assert layout.first_data_row == 1

    def test_detects_depth_column_by_name(self, csv_with_units):
        layout = detect_csv_layout(str(csv_with_units))
        assert layout.depth_column == "DEPTH"
        assert layout.detection_confidence == "high"

    def test_returns_csylayout_instance(self, csv_with_units):
        layout = detect_csv_layout(str(csv_with_units))
        assert isinstance(layout, CSVLayout)

    def test_all_columns_populated(self, csv_with_units):
        layout = detect_csv_layout(str(csv_with_units))
        assert set(layout.all_columns) == {"DEPTH", "GR", "RHOB"}


class TestCSVConverterAutoDetect:

    def test_auto_detect_unit_row_reads_correct_data(self, csv_with_units):
        """When unit_row=None the unit row must NOT appear as data rows."""
        conv = CSVConverter(depth_column="DEPTH")
        ds = conv.read(str(csv_with_units))
        depth_ch = ds.get_channel("DEPTH")
        # All values must be numeric (no 'm' in data)
        assert np.isfinite(depth_ch.data).all()
        assert len(depth_ch.data) == 3

    def test_auto_detected_units_applied(self, csv_with_units):
        """Units from the auto-detected unit row must be applied to channels."""
        conv = CSVConverter(depth_column="DEPTH")
        ds = conv.read(str(csv_with_units))
        gr_ch = ds.get_channel("GR")
        # Unit from unit row: "gAPI"
        assert gr_ch.unit == "gAPI"

    def test_explicit_unit_row_minus1_disables_detection(self, csv_with_units):
        """unit_row=-1 disables auto-detection; unit row parsed as numeric data."""
        conv = CSVConverter(depth_column="DEPTH", unit_row=-1)
        # The unit row ('m', 'gAPI', 'g/cm3') cannot be parsed as float → error
        with pytest.raises(Exception):
            conv.read(str(csv_with_units))


class TestCSVConverterColumns:
    """columns parameter restricts which columns are included."""

    def test_columns_whitelist(self, simple_csv):
        """Only whitelisted columns (plus depth) appear in output channels."""
        conv = CSVConverter(
            depth_column="DEPTH",
            columns=["GR"],  # exclude RHOB
        )
        ds = conv.read(str(simple_csv))
        names = [c.name for c in ds.channels]
        assert "DEPTH" in names
        assert "GR" in names
        assert "RHOB" not in names

    def test_columns_unknown_column_ignored_with_warning(self, simple_csv):
        """Columns not present in the CSV are silently skipped (only a warning)."""
        conv = CSVConverter(
            depth_column="DEPTH",
            columns=["GR", "NONEXISTENT"],
        )
        ds = conv.read(str(simple_csv))
        names = [c.name for c in ds.channels]
        assert "GR" in names
        assert "NONEXISTENT" not in names

