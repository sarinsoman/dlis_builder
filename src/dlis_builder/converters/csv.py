"""
dlis_builder.converters.csv
-----------------------------
:class:`CSVConverter` — converts CSV / TSV files to DLIS.

CSV files have no built-in concept of well metadata, units, or array channels.
This converter provides two operating modes:

**Auto-detection mode** (default)
    The converter reads the first few rows and intelligently detects:

    * The **depth / index column** by matching against known mnemonics
      (DEPTH, DEPT, MD, TVD, TVDSS, TWT, TIME, etc.).
    * A **unit row** — a second header row containing physical unit strings
      (e.g. "m", "ft", "gAPI", "ohm.m").  This is a common format in CSV
      exports from Petrel, LogView, and other oilfield software.

    Detected values can be inspected with :func:`detect_csv_layout` before
    converting.

**Explicit mode**
    Pass ``depth_column``, ``column_units``, and/or ``unit_row`` to suppress
    auto-detection and provide exact configuration.

Batch operations are parallelised with :class:`concurrent.futures.ThreadPoolExecutor`
to exploit I/O concurrency when converting many files at once.

Usage
-----
::

    from dlis_builder.converters import CSVConverter, detect_csv_layout
    from dlis_builder.models import WellMetadata

    # Inspect what would be auto-detected before converting:
    layout = detect_csv_layout("well_data.csv")
    print(layout)

    # Minimal — auto-detect depth column and units:
    conv = CSVConverter()
    path = conv.convert(
        "well_data.csv", "output.dlis",
        metadata=WellMetadata(well_name="WELL_A", company="Acme"),
    )

    # Explicit configuration:
    conv = CSVConverter(
        depth_column="DEPTH",
        column_units={"DEPTH": "m", "GR": "gAPI", "RHOB": "g/cm3"},
        array_columns={"IMAGE": [f"IMG_{i:03d}" for i in range(1, 121)]},
        null_value=-9999.0,
    )

    # Batch with parallelism:
    results = CSVConverter(depth_column="MD").convert_batch(
        "/data/csv/", output_dir="/data/dlis/", max_workers=4,
    )

Pandas dependency
-----------------
``pandas`` is required for CSV reading::

    pip install "dlis-builder[csv]"
"""
from __future__ import annotations

import gc
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from dlis_builder.contracts.converter import DLISConverter
from dlis_builder.models.channel import ChannelDef, Resolution
from dlis_builder.models.frame import WellDataset
from dlis_builder.models.metadata import OriginConfig, WellMetadata
from dlis_builder.utils.naming import deduplicate_names, sanitize_channel_name
from dlis_builder.utils.units import normalize_unit
from dlis_builder.utils.validation import validate_csv_config

logger = logging.getLogger(__name__)

# ── Known depth-column mnemonics (case-insensitive) ───────────────────────────
_DEPTH_MNEMONICS: list[str] = [
    "DEPTH", "DEPT", "MD", "TVD", "TVDSS", "TVDSD", "TWT", "TIME",
    "INDEX", "MDEPTH", "MEASURED_DEPTH", "TRUE_VERTICAL_DEPTH",
]

# Pattern: short alphanumeric token that looks like a physical unit
# (e.g. "m", "ft", "gAPI", "ohm.m", "g/cm3", "degC", "psi", "m/s")
_UNIT_LIKE = re.compile(
    r"^(m|ft|in|cm|dm|km|mm|"
    r"s|ms|us|min|h|"
    r"gAPI|dAPI|nAPI|"
    r"ohm\.m|ohm|mohm|"
    r"g/cm3|g/cc|kg/m3|"
    r"degC|degF|degK|"
    r"psi|kpa|mpa|bar|"
    r"m/s|ft/s|"
    r"[a-zA-Z]{1,6}|[a-zA-Z]{1,4}/[a-zA-Z]{1,4})$",
    re.IGNORECASE,
)


# ── CSVLayout ─────────────────────────────────────────────────────────────────

@dataclass
class CSVLayout:
    """
    Auto-detected structural layout of a CSV file.

    Returned by :func:`detect_csv_layout`.  Can be inspected before converting
    to confirm the detection result is correct.

    Attributes
    ----------
    depth_column : str
        Name of the detected (or defaulted) depth / index column.
    unit_row : int or None
        0-based row index of the unit row (``None`` if not detected).
    first_data_row : int
        0-based row index where numeric data begins.
    detected_units : dict[str, str]
        Unit strings extracted from the unit row (column name → unit),
        after normalisation.  Empty if no unit row was detected.
    depth_column_index : int
        0-based column index of the depth column.
    all_columns : list[str]
        All column names from the CSV header.
    detection_confidence : str
        ``"high"`` / ``"low"`` indicating how reliable the detection was.

    Examples
    --------
    ::

        layout = detect_csv_layout("petrel_export.csv")
        print(layout.depth_column)       # → "MD"
        print(layout.unit_row)           # → 1
        print(layout.detected_units)     # → {"MD": "m", "GR": "gAPI", ...}
        print(layout.first_data_row)     # → 2
    """

    depth_column: str = ""
    unit_row: int | None = None
    first_data_row: int = 1
    detected_units: dict[str, str] = field(default_factory=dict)
    depth_column_index: int = 0
    all_columns: list[str] = field(default_factory=list)
    detection_confidence: str = "low"

    def __repr__(self) -> str:
        parts = [f"depth_column={self.depth_column!r}"]
        if self.unit_row is not None:
            parts.append(f"unit_row={self.unit_row}")
        parts.append(f"first_data_row={self.first_data_row}")
        parts.append(f"columns={len(self.all_columns)}")
        parts.append(f"confidence={self.detection_confidence!r}")
        return f"CSVLayout({', '.join(parts)})"


def detect_csv_layout(
    path: str,
    delimiter: str | None = None,
    encoding: str = "utf-8",
    max_scan_rows: int = 10,
) -> CSVLayout:
    """
    Inspect the first few rows of a CSV and infer its layout.

    This is the same detection logic that :class:`CSVConverter` uses internally
    when ``depth_column`` and ``unit_row`` are not set explicitly.

    Parameters
    ----------
    path :
        Path to the CSV file.
    delimiter :
        Column separator.  ``None`` lets the sniffer auto-detect.
    encoding :
        File encoding.
    max_scan_rows :
        Maximum rows to read for detection.

    Returns
    -------
    CSVLayout
        Detected layout.  Inspect before building a :class:`CSVConverter`.

    Examples
    --------
    ::

        layout = detect_csv_layout("petrel_export.csv")
        print(layout)
        # → CSVLayout(depth_column='MD', unit_row=1, first_data_row=2, ...)

        conv = CSVConverter(
            depth_column=layout.depth_column,
            column_units=layout.detected_units,
            unit_row=layout.unit_row,            # skip unit row when reading
        )
    """
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            "detect_csv_layout requires 'pandas'.  Install with:  pip install pandas"
        ) from exc

    csv_path = Path(path).resolve()
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    sep = delimiter or ","
    # Read first max_scan_rows rows as raw strings (no type inference)
    raw = pd.read_csv(
        csv_path,
        sep=sep,
        nrows=max_scan_rows,
        header=0,
        dtype=str,
        encoding=encoding,
        engine="python" if delimiter is None else "c",
    )

    columns = list(raw.columns)
    if not columns:
        return CSVLayout(all_columns=columns)

    # ── 1. Detect unit row ─────────────────────────────────────────────────
    unit_row: int | None = None
    detected_units: dict[str, str] = {}

    if len(raw) > 0:
        first_data_row_candidate = raw.iloc[0]
        # A unit row has a high fraction of "unit-like" values
        unit_like_count = sum(
            1 for v in first_data_row_candidate.values
            if isinstance(v, str) and (v.strip() == "" or _UNIT_LIKE.match(v.strip()))
        )
        fraction = unit_like_count / max(len(columns), 1)
        if fraction >= 0.5:
            unit_row = 1  # 1-based row after header, i.e. pandas row index 0
            for col, val in zip(columns, first_data_row_candidate.values):
                normed = normalize_unit(str(val).strip()) if val else ""
                # Keep original if normalize_unit returns empty (unknown unit)
                detected_units[col] = normed or str(val).strip()

    first_data_row = (unit_row or 0) + 1  # 1-based row index in the file

    # ── 2. Detect depth column ─────────────────────────────────────────────
    depth_col = columns[0]  # default: first column
    depth_col_idx = 0
    confidence = "low"

    for i, col in enumerate(columns):
        if col.strip().upper() in [m.upper() for m in _DEPTH_MNEMONICS]:
            depth_col = col
            depth_col_idx = i
            confidence = "high"
            break

    return CSVLayout(
        depth_column=depth_col,
        unit_row=unit_row,
        first_data_row=first_data_row,
        detected_units={k: v for k, v in detected_units.items() if v},
        depth_column_index=depth_col_idx,
        all_columns=columns,
        detection_confidence=confidence,
    )



class CSVConverter(DLISConverter):
    """
    Converts delimited text files (CSV, TSV) to DLIS.

    Parameters
    ----------
    depth_column : str, optional
        Name of the depth / time index column.  If ``None`` (default), the
        converter searches for a recognised depth mnemonic (DEPTH, DEPT, MD,
        TVD, …) and falls back to the first column.
    column_units : dict, optional
        Explicit mapping of column name → physical unit string.
        Unspecified columns get their unit from the unit row (if detected)
        or an empty string.
    array_columns : dict, optional
        Mapping of DLIS channel name → list of CSV column names to group
        into a multi-element (image) channel.
        Example: ``{"IMAGE": ["IMG_001", "IMG_002", ..., "IMG_120"]}``
    null_value : float
        Sentinel value representing missing data.  Replaced with NaN.
    image_resolution : str
        ``"low"`` (float32) or ``"high"`` (float64) for array channels.
    unit_row : int, optional
        0-based **row index** (after the header) of a unit row, e.g. ``0``
        means the second line of the file (first line after the header) is a
        unit row.  ``None`` (default) triggers auto-detection.
        Pass ``-1`` to explicitly disable unit-row detection.
    delimiter : str, optional
        Column separator.  ``None`` auto-detects (comma, tab, etc.).
    skip_rows : int
        Number of comment lines to skip before the header.
    encoding : str
        File encoding.

    Notes
    -----
    **Auto-detection** runs when both ``depth_column`` and ``unit_row`` are
    ``None``.  Use :func:`detect_csv_layout` to preview what will be detected
    before converting.

    **Memory**: after extracting numpy arrays the pandas DataFrame is
    immediately released.  For very large files (>1 GB), ensure sufficient
    RAM or split files before converting.
    """

    def __init__(
        self,
        depth_column: str | None = None,
        column_units: dict[str, str] | None = None,
        array_columns: dict[str, list[str]] | None = None,
        null_value: float = -999.25,
        image_resolution: str = "low",
        unit_row: int | None = None,
        delimiter: str | None = None,
        skip_rows: int = 0,
        encoding: str = "utf-8",
        columns: list[str] | None = None,
    ) -> None:
        if image_resolution not in ("low", "high"):
            raise ValueError(
                f"image_resolution must be 'low' or 'high', got {image_resolution!r}"
            )
        self._depth_column = depth_column
        self._column_units = {
            k: normalize_unit(v) or v
            for k, v in (column_units or {}).items()
        }
        self._array_columns = array_columns or {}
        self._null_value = null_value
        self._resolution = Resolution(image_resolution)
        self._unit_row = unit_row      # None = auto-detect; -1 = disabled
        self._delimiter = delimiter
        self._skip_rows = skip_rows
        self._encoding = encoding
        # Optional explicit column whitelist (always includes depth column)
        self._columns: list[str] | None = columns

    # ------------------------------------------------------------------ #
    #  DLISConverter interface                                             #
    # ------------------------------------------------------------------ #

    def read(self, source: str | Path) -> WellDataset:
        """
        Parse a CSV file and return a :class:`~dlis_builder.models.WellDataset`.

        Parameters
        ----------
        source :
            Path to the input CSV / TSV file.
        """
        try:
            import pandas as pd  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "CSVConverter requires 'pandas'.  Install with:  pip install pandas"
            ) from exc

        csv_path = str(Path(source).resolve())
        if not Path(csv_path).exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        logger.info("Reading CSV: %s", csv_path)

        # ── Auto-detect layout if needed ──────────────────────────────────
        effective_unit_row: int | None = self._unit_row
        effective_depth_col: str | None = self._depth_column
        effective_column_units: dict[str, str] = dict(self._column_units)

        layout: CSVLayout | None = None
        needs_detection = (
            (effective_depth_col is None)
            or (effective_unit_row is None and effective_unit_row != -1)
        )
        if needs_detection:
            try:
                layout = detect_csv_layout(
                    csv_path,
                    delimiter=self._delimiter,
                    encoding=self._encoding,
                )
                if effective_depth_col is None:
                    effective_depth_col = layout.depth_column
                    logger.info(
                        "  Auto-detected depth column: '%s' (confidence=%s)",
                        layout.depth_column, layout.detection_confidence,
                    )
                if effective_unit_row is None and layout.unit_row is not None:
                    effective_unit_row = layout.unit_row
                    logger.info("  Auto-detected unit row at CSV row %d", layout.unit_row)
                # Merge detected units (explicit column_units take priority)
                for col, unit in layout.detected_units.items():
                    if col not in effective_column_units and unit:
                        effective_column_units[col] = unit
            except Exception as exc:
                logger.debug("  Layout detection failed (%s); using defaults.", exc)

        # ── Build skiprows list for pandas ────────────────────────────────
        # skip_rows (comment lines) + the unit row (if any)
        base_skiprows = self._skip_rows
        rows_to_skip: list[int] = []
        if base_skiprows:
            rows_to_skip.extend(range(base_skiprows))
        if effective_unit_row is not None and effective_unit_row >= 0:
            # effective_unit_row is 1-based from the header (same convention as
            # detect_csv_layout).  In pandas skiprows, row indices are absolute
            # 0-based file line numbers where line 0 is the header.  Therefore
            # "unit_row=1" means absolute file line 1 → skiprows=[1].
            rows_to_skip.append(effective_unit_row)

        skiprows_arg = rows_to_skip if rows_to_skip else None

        df = pd.read_csv(
            csv_path,
            sep=self._delimiter,
            skiprows=skiprows_arg,
            encoding=self._encoding,
            engine="python" if self._delimiter is None else "c",
        )

        if df.empty:
            raise ValueError(f"No data found in {csv_path}")

        logger.info("  Shape: %d rows × %d columns", *df.shape)
        # ── Apply column whitelist if provided ────────────────────────────────────
        if self._columns:
            depth_col_name = effective_depth_col or df.columns[0]
            keep = [depth_col_name] + [
                c for c in self._columns if c != depth_col_name and c in df.columns
            ]
            missing = [c for c in self._columns if c not in df.columns]
            if missing:
                logger.warning("  Columns not found in CSV (ignored): %s", missing)
            df = df[keep]
            logger.info("  Column filter applied: %d columns kept", len(keep))
        # ── Determine index column ─────────────────────────────────────────
        depth_col = (
            effective_depth_col
            or (layout.depth_column if layout else None)
            or df.columns[0]
        )

        # Validate columns
        validate_csv_config(list(df.columns), depth_col, self._array_columns)

        # ── Replace null sentinel with NaN ─────────────────────────────────
        df = df.replace(self._null_value, np.nan)

        # ── Build channels ─────────────────────────────────────────────────
        raw_names = list(df.columns)
        sanitized = [sanitize_channel_name(n) for n in raw_names]
        unique = deduplicate_names(sanitized)
        name_map = dict(zip(raw_names, unique))   # original CSV col → safe name

        # Track which columns are consumed by array groups
        used_cols: set[str] = set()
        channels: list[ChannelDef] = []

        # Depth index channel — always first
        depth_data = df[depth_col].to_numpy(dtype=np.float64)
        channels.append(ChannelDef(
            name=name_map.get(depth_col, sanitize_channel_name(depth_col)),
            data=depth_data,
            unit=effective_column_units.get(depth_col, "") or "",
            long_name=depth_col,
        ))
        used_cols.add(depth_col)

        # Array / image groups
        for dlis_name, members in self._array_columns.items():
            valid = [m for m in members if m in df.columns]
            if len(valid) < 2:
                logger.warning(
                    "Array group '%s': only %d columns found in CSV, skipping.",
                    dlis_name, len(valid),
                )
                continue
            arr = df[valid].to_numpy(dtype=np.float64)   # (N, M)
            ref_unit = effective_column_units.get(valid[0], "")
            channels.append(ChannelDef(
                name=sanitize_channel_name(dlis_name),
                data=arr,
                unit=ref_unit,
                long_name=dlis_name,
                dimension=[len(valid)],
                resolution=self._resolution,
            ))
            used_cols.update(valid)
            logger.info("  Array: %s[%d]", dlis_name, len(valid))

        # Remaining scalars (in original column order)
        for original_name in raw_names:
            if original_name in used_cols:
                continue
            col_data = df[original_name].to_numpy(dtype=np.float64)
            safe_name = name_map.get(original_name, sanitize_channel_name(original_name))
            unit = effective_column_units.get(original_name, "") or ""
            channels.append(ChannelDef(
                name=safe_name,
                data=col_data,
                unit=unit,
                long_name=original_name,
            ))

        # ── Free DataFrame memory now that arrays are extracted ────────────
        del df
        gc.collect()

        # Insert scalars before arrays (writer reorders, but be explicit)
        scalars = [c for c in channels if not c.is_array]
        arrays  = [c for c in channels if c.is_array]
        ordered = scalars + arrays

        logger.info(
            "  Channels: %d scalar + %d array", len(scalars), len(arrays)
        )

        meta = WellMetadata(null_value=self._null_value)
        origin = OriginConfig()

        return WellDataset(
            metadata=meta,
            origin=origin,
            channels=ordered,
            parameters=[],
        )

    def convert(
        self,
        source: str | Path,
        output_path: str,
        *,
        metadata: WellMetadata | None = None,
        origin: OriginConfig | None = None,
        flatten_arrays: bool = False,
    ) -> str:
        """
        Convert a CSV file to DLIS.

        Parameters
        ----------
        source:
            Path to the input CSV file.
        output_path:
            Destination ``.dlis`` file path.
        metadata:
            Well metadata (name, company, field, etc.).
            Since CSV files contain no metadata, this should always be provided.
        origin:
            DLIS origin / producer configuration.
        flatten_arrays:
            Write image elements as individual scalar channels.

        Returns
        -------
        str
            Absolute path to the created DLIS file.
        """
        dataset = self.read(source)
        if metadata:
            dataset.metadata = metadata
        if origin:
            dataset.origin = origin
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        return self._write(dataset, output_path, flatten_arrays=flatten_arrays)

    def convert_batch(
        self,
        input_dir: str,
        output_dir: str | None = None,
        *,
        recursive: bool = False,
        metadata: WellMetadata | None = None,
        flatten_arrays: bool = False,
        max_workers: int = 4,
    ) -> list[dict]:
        """
        Convert all CSV files in *input_dir* to DLIS in parallel.

        Files are converted concurrently using a thread pool (I/O-bound
        work benefits from threads without the overhead of multiprocessing).

        Parameters
        ----------
        input_dir :
            Directory to scan for ``.csv`` files.
        output_dir :
            Output directory.  Defaults to the same directory as each input.
        recursive :
            If ``True``, walk subdirectories.
        metadata :
            Well metadata applied to **every** file converted.
            Since CSV files carry no metadata, this should always be provided
            for production use.
        flatten_arrays :
            Explode array channels into individual scalar channels.
        max_workers :
            Maximum number of files to convert concurrently.
            Defaults to 4.  Increase for fast SSDs; lower for spinning disks
            or memory-constrained environments.

        Returns
        -------
        list[dict]
            One dict per input file with keys:
            ``csv_path``, ``dlis_path``, ``success``, ``error``.
        """
        inp = Path(input_dir)
        if not inp.is_dir():
            raise NotADirectoryError(f"Not a directory: {input_dir}")
        pattern = "**/*.csv" if recursive else "*.csv"
        csv_files = sorted(inp.glob(pattern))
        if not csv_files:
            logger.warning("No .csv files found in %s", input_dir)
            return []

        logger.info(
            "Batch CSV: %d files in %s (max_workers=%d)",
            len(csv_files), input_dir, max_workers,
        )

        def _convert_one(cf: Path) -> dict:
            out_dir = Path(output_dir) if output_dir else cf.parent
            dp = str(out_dir / cf.with_suffix(".dlis").name)
            try:
                rp = self.convert(cf, dp, metadata=metadata, flatten_arrays=flatten_arrays)
                return dict(csv_path=str(cf), dlis_path=rp, success=True, error=None)
            except Exception as exc:
                logger.error("Failed %s: %s", cf, exc)
                return dict(csv_path=str(cf), dlis_path=dp, success=False, error=str(exc))

        results: list[dict] = [None] * len(csv_files)  # type: ignore[list-item]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_idx = {
                executor.submit(_convert_one, cf): i
                for i, cf in enumerate(csv_files)
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                results[idx] = future.result()

        ok = sum(1 for r in results if r["success"])
        logger.info("Batch complete: %d/%d succeeded", ok, len(results))
        return results
