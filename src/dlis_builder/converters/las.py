"""
dlis_builder.converters.las
-----------------------------
:class:`LASConverter` — converts LAS files (v1.2, 2.0, 3.0) to DLIS.

This converter wraps the full array-detection pipeline (bracket notation,
indexed element notation, numbered-suffix grouping) that was developed for
real-world Baker Hughes / Halliburton / Schlumberger image log files.

Usage
-----
::

    from dlis_builder.converters import LASConverter
    from dlis_builder.models import WellMetadata, OriginConfig

    # Minimal — metadata from the LAS file itself
    conv = LASConverter()
    path = conv.convert("borehole_image.las", "output.dlis")

    # Override metadata from an external system
    path = conv.convert(
        "borehole_image.las",
        "output.dlis",
        metadata=WellMetadata(well_name="WELL_A", company="Acme"),
        array_map={"FMI": [f"PAD_{i:03d}" for i in range(1, 129)]},
        image_resolution="high",
    )

    # Batch
    results = LASConverter().convert_batch(
        "/data/las/", output_dir="/data/dlis/", recursive=True,
    )
"""
from __future__ import annotations

import gc
import logging
import re
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np

from dlis_builder.contracts.converter import DLISConverter
from dlis_builder.models.channel import ChannelDef, Resolution
from dlis_builder.models.frame import WellDataset
from dlis_builder.models.metadata import OriginConfig, ParameterDef, WellMetadata
from dlis_builder.utils.naming import deduplicate_names, sanitize_channel_name
from dlis_builder.utils.units import normalize_unit

logger = logging.getLogger(__name__)

# Null/absent value sentinels that are universally treated as missing in LAS files,
# regardless of the value declared in the NULL. header line.
# -285.43 is the Baker Hughes / Halliburton legacy null for some tool families.
_COMMON_LAS_NULLS: tuple[float, ...] = (-999.25, -999.0, -9999.0, -285.43)

# ── Regex patterns ────────────────────────────────────────────────────────────
_BRACKET_RE = re.compile(r'^(.+)\[(\d+)\]$')
_BRACKET_LINE_RE = re.compile(
    r'^(\s*)([\w-]+)\[(\d+)\](\s*\.\s*\S*)(.*?:\s*)(.*?)\s*$',
    re.MULTILINE,
)
_NUMBERED_RE = re.compile(r'^(.+?)[_.]?(\d{2,})$')
_MIN_ARRAY_GROUP = 3


def _las_header_str(section, key: str, default: str = "") -> str:
    """
    Safely extract a string value from a lasio header section.

    lasio's HeaderItem.__bool__ always returns False regardless of content,
    so truthiness tests are unreliable.  Use ``is not None`` instead.
    """
    item = section.get(key)
    if item is None:
        return default
    val = str(item.value).strip() if item.value is not None else ""
    return val if val else default


def _mask_common_nulls(data: np.ndarray, declared_null: float) -> None:
    """
    Replace all common null sentinels (and the file-declared null) with NaN.

    Operates in-place.  Uses an absolute tolerance of 0.005 — tight enough
    to avoid masking real measurements while still handling floating-point
    representation drift in legacy acquisition systems.
    """
    nulls: set[float] = set(_COMMON_LAS_NULLS)
    nulls.add(declared_null)
    atol = 0.005
    for null in nulls:
        if not np.isnan(null):
            np.putmask(data, np.abs(data - null) <= atol, np.nan)


def _preprocess_bracket_arrays(text: str) -> tuple[str, dict[str, list[str]]]:
    """Pre-process LAS 3.0 bracket array notation before lasio parsing."""
    bracket_map: dict[str, list[str]] = {}
    base_counts: dict[str, int] = {}
    for m in _BRACKET_LINE_RE.finditer(text):
        base = m.group(2)
        base_counts[base] = base_counts.get(base, 0) + 1
    indexed_bases = {b for b, c in base_counts.items() if c >= 2}

    def _expand(m):  # noqa: ANN001
        indent, base, size_s, unit_part, mid, desc = m.groups()
        if base in indexed_bases:
            return m.group(0)
        size = int(size_s)
        if size < 1:
            return m.group(0)
        cols, lines = [], []
        for k in range(1, size + 1):
            cn = f"{base}_{k}"
            cols.append(cn)
            cd = f"{desc} ({k} of {size})" if desc.strip() else f"{base} element {k}"
            lines.append(f"{indent}{cn:<8s}{unit_part}{mid}{cd}")
        bracket_map[base] = cols
        return "\n".join(lines)

    modified = _BRACKET_LINE_RE.sub(_expand, text)
    return modified, bracket_map


def _detect_array_channels(
    raw_curves: list[dict],
    explicit_map: dict[str, list[str]] | None = None,
) -> list[ChannelDef]:
    """Group flat curve list into scalar and array ChannelDef objects."""
    explicit_map = explicit_map or {}

    # Build lookup map: original name → curve dict
    by_name: dict[str, dict] = {c["name"]: c for c in raw_curves}
    by_orig: dict[str, dict] = {c["original_name"]: c for c in raw_curves}
    used: set[str] = set()
    channels: list[ChannelDef] = []

    def _merge(members: list[str], dlis_name: str, ref_curve: dict) -> None:
        cols = []
        for m in members:
            c = by_name.get(m) or by_orig.get(m)
            if c:
                cols.append(c["data"])
                used.add(c["name"])
        if not cols:
            return
        arr = np.stack(cols, axis=1)  # shape (N, M)
        channels.append(ChannelDef(
            name=sanitize_channel_name(dlis_name),
            data=arr,
            unit=ref_curve["unit"],
            long_name=ref_curve["description"],
            dimension=[len(cols)],
        ))

    # 1. Explicit map takes highest priority
    for dlis_name, members in explicit_map.items():
        valid = [m for m in members if m in by_name or m in by_orig]
        if len(valid) >= 2:
            ref = by_name.get(valid[0]) or by_orig.get(valid[0]) or raw_curves[0]
            _merge(valid, dlis_name, ref)
            logger.info("  Array (explicit): %s[%d]", dlis_name, len(valid))

    # 2. Indexed element notation: IMAGE-DYNAMIC[0] … IMAGE-DYNAMIC[119]
    indexed_groups: dict[str, list[tuple[int, dict]]] = {}
    for c in raw_curves:
        if c["name"] in used:
            continue
        m = _BRACKET_RE.match(c["original_name"])
        if m:
            base, idx = m.group(1), int(m.group(2))
            indexed_groups.setdefault(base, []).append((idx, c))

    for base, elements in indexed_groups.items():
        if len(elements) < 2:
            continue
        elements.sort(key=lambda x: x[0])
        ref = elements[0][1]
        cols = [e[1]["data"] for e in elements]
        for _, c in elements:
            used.add(c["name"])
        arr = np.stack(cols, axis=1)
        name = sanitize_channel_name(base)
        channels.append(ChannelDef(
            name=name, data=arr,
            unit=ref["unit"], long_name=ref["description"],
            dimension=[len(cols)],
        ))
        logger.info("  Array (indexed): %s[%d]", base, len(cols))

    # 3. Numbered suffix: AMP01 … AMP48
    prefix_groups: dict[str, list[tuple[int, dict]]] = {}
    for c in raw_curves:
        if c["name"] in used:
            continue
        m = _NUMBERED_RE.match(c["name"])
        if m:
            prefix_groups.setdefault(m.group(1), []).append((int(m.group(2)), c))

    for prefix, group in prefix_groups.items():
        if len(group) < _MIN_ARRAY_GROUP:
            continue
        # All must share the same unit
        units = {item[1]["unit"] for item in group}
        if len(units) > 1:
            continue
        group.sort(key=lambda x: x[0])
        ref_c = group[0][1]
        cols = [item[1]["data"] for item in group]
        for item in group:
            used.add(item[1]["name"])
        arr = np.stack(cols, axis=1)
        channels.append(ChannelDef(
            name=sanitize_channel_name(prefix),
            data=arr, unit=ref_c["unit"], long_name=ref_c["description"],
            dimension=[len(cols)],
        ))
        logger.info("  Array (suffix): %s[%d]", prefix, len(cols))

    # 4. Remaining scalars (in original order)
    scalar_positions = {c["name"]: i for i, c in enumerate(raw_curves)}
    remaining = sorted(
        [c for c in raw_curves if c["name"] not in used],
        key=lambda c: scalar_positions.get(c["name"], 9999),
    )

    # Separate already-built channels into scalars and arrays so we can
    # prepend the remaining scalars without O(n²) list.insert() shifting.
    existing_scalars = [ch for ch in channels if not ch.is_array]
    existing_arrays  = [ch for ch in channels if ch.is_array]
    remaining_scalars = [
        ChannelDef(
            name=c["name"], data=c["data"],
            unit=c["unit"], long_name=c["description"],
        )
        for c in remaining
    ]
    return existing_scalars + remaining_scalars + existing_arrays


class LASConverter(DLISConverter):
    """
    Converts LAS files (v1.2, 2.0, 3.0) to DLIS.

    Parameters
    ----------
    image_resolution:
        Numeric precision for array / image channels.
        ``"low"`` — float32, 4 B/element (Baker Hughes / Techlog compatible).
        ``"high"`` — float64, 8 B/element (full precision).
        ``"auto"`` (default) — infer from array width: dimension ≥ 32 → float32,
        narrower arrays → float64.
    array_map:
        Instance-level explicit channel-grouping map applied to every
        :meth:`read` / :meth:`convert` call.  Per-call ``array_map`` kwargs
        are merged on top (per-call takes priority).
        ``{"DLIS_NAME": ["LAS_COL1", "LAS_COL2", ...]}`
    """

    def __init__(
        self,
        image_resolution: str = "auto",
        *,
        array_map: dict[str, list[str]] | None = None,
    ) -> None:
        if image_resolution not in ("low", "high", "auto"):
            raise ValueError(
                f"image_resolution must be 'low', 'high', or 'auto', got {image_resolution!r}"
            )
        self._resolution_setting: str = image_resolution
        self._array_map: dict[str, list[str]] = array_map or {}

    # ------------------------------------------------------------------ #
    #  DLISConverter interface                                             #
    # ------------------------------------------------------------------ #

    def read(
        self,
        source: str,
        *,
        array_map: dict[str, list[str]] | None = None,
    ) -> WellDataset:
        """
        Parse a LAS file and return a :class:`~dlis_builder.models.WellDataset`.

        Parameters
        ----------
        source:
            Path to the input ``.las`` file.
        array_map:
            Explicit array grouping override:
            ``{"DLIS_NAME": ["LAS_COL1", "LAS_COL2", ...]}``.
        """
        try:
            import lasio  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "LASConverter requires 'lasio'.  Install with:  pip install lasio"
            ) from exc

        las_path = str(Path(source).resolve())
        if not Path(las_path).exists():
            raise FileNotFoundError(f"LAS file not found: {las_path}")

        logger.info("Reading LAS: %s", las_path)

        # Raw text pre-processing (bracket arrays must be expanded before lasio)
        raw_text = Path(las_path).read_text(encoding="utf-8", errors="replace")
        modified_text, bracket_map = _preprocess_bracket_arrays(raw_text)

        # Suppress lasio warnings about non-standard units / duplicates
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            las = lasio.read(modified_text, engine="normal")

        null_item = las.well.get("NULL")
        null_val = float(null_item.value) if null_item is not None else -999.25

        # ── Well metadata ─────────────────────────────────────────────────
        def _well(key: str, default: str = "") -> str:
            return _las_header_str(las.well, key, default)

        meta = WellMetadata(
            well_name=_well("WELL", "UNKNOWN"),
            well_id=_well("UWI") or _well("API"),
            company=_well("COMP", "UNKNOWN"),
            field_name=_well("FLD", "WILDCAT"),
            country=_well("CTRY"),
            state=_well("STAT"),
            county=_well("CNTY"),
            location=_well("LOC"),
            service_company=_well("SRVC"),
            date=_well("DATE"),
            null_value=null_val,
        )

        # ── Origin config — look for embedded DLIS metadata in ~Parameter ──
        params_raw: list[ParameterDef] = []
        param_lookup: dict[str, str] = {}
        try:
            for item in las.header.get("Parameter", {}).values():
                p_name = item.mnemonic.strip()
                p_val  = str(item.value).strip() if item.value else ""
                if not p_val:
                    continue
                try:
                    if float(p_val) == null_val:
                        continue
                except (ValueError, TypeError):
                    pass
                params_raw.append(ParameterDef(
                    name=p_name,
                    value=p_val,
                    unit=item.unit.strip() if item.unit else "",
                    description=item.descr.strip() if item.descr else "",
                ))
                param_lookup[p_name.upper().replace("-", "_")] = p_val
        except Exception as exc:
            logger.debug("Parameter section parsing failed: %s", exc)

        def _param(key: str, default: str = "") -> str:
            return param_lookup.get(key.upper().replace("-", "_"), default)

        origin = OriginConfig(
            producer_name=_param("PRODUCER_NAME", "AIQ"),
            product_name=_param("PRODUCT", "dlis-builder"),
            file_type=_param("FILE_TYPE", "LAS CONVERSION"),
            frame_name=_param("DLIS_FRAME_NAME", "MAIN-FRAME"),
            file_set_name=_param("FILE_SET_NAME") or None,
        )
        try:
            origin.file_set_number = (
                int(float(_param("FILE_SET_NUMBER")))
                if _param("FILE_SET_NUMBER")
                else None
            )
        except (ValueError, TypeError):
            pass
        try:
            origin.file_number = (
                int(float(_param("FILE_NUMBER")))
                if _param("FILE_NUMBER")
                else None
            )
        except (ValueError, TypeError):
            pass

        # ── Curves ────────────────────────────────────────────────────────
        if not las.curves or las.data is None or len(las.data) == 0:
            raise ValueError(f"No curve data found in {las_path}")

        raw_names = [c.mnemonic for c in las.curves]
        sanitized = [sanitize_channel_name(n) for n in raw_names]
        unique = deduplicate_names(sanitized)

        raw_curves: list[dict] = []
        for i, curve in enumerate(las.curves):
            # np.array() with dtype performs a single allocation; avoids the
            # redundant double-copy of .astype(float64).copy().
            raw_data = np.array(las[curve.mnemonic], dtype=np.float64)
            _mask_common_nulls(raw_data, null_val)
            unit = normalize_unit(curve.unit) or ""
            desc = re.sub(r"^\d+\s+", "", (curve.descr or "").strip()) or unique[i]
            raw_curves.append(dict(
                name=unique[i], original_name=curve.mnemonic,
                unit=unit, description=desc, data=raw_data,
            ))

        # Instance-level map provides defaults; per-call map takes priority.
        combined_map = {**self._array_map, **bracket_map, **(array_map or {})}
        channels = _detect_array_channels(raw_curves, explicit_map=combined_map)

        # Apply resolution to all array channels.
        # "auto": wide arrays (≥32 elements) → float32 (saves ~50 % storage);
        # narrow arrays → float64 (full precision for waveforms/spectral).
        for ch in channels:
            if ch.is_array:
                if self._resolution_setting == "auto":
                    ch.resolution = (
                        Resolution.LOW if ch.array_size >= 32 else Resolution.HIGH
                    )
                else:
                    ch.resolution = Resolution(self._resolution_setting)

        logger.info(
            "  Channels: %d (%d scalar + %d array)",
            len(channels),
            sum(1 for c in channels if not c.is_array),
            sum(1 for c in channels if c.is_array),
        )

        return WellDataset(
            metadata=meta,
            origin=origin,
            channels=channels,
            parameters=params_raw,
        )

    def convert(
        self,
        source: str,
        output_path: str,
        *,
        metadata: WellMetadata | None = None,
        origin: OriginConfig | None = None,
        array_map: dict[str, list[str]] | None = None,
        flatten_arrays: bool = False,
    ) -> str:
        """
        Convert a LAS file to DLIS in one call.

        Parameters
        ----------
        source:
            Path to the input ``.las`` file.
        output_path:
            Destination ``.dlis`` file path.
        metadata:
            Override well metadata extracted from the LAS file.
        origin:
            Override DLIS origin / producer configuration.
        array_map:
            Explicit array grouping: ``{"DLIS_NAME": ["COL1", "COL2", ...]}``.
        flatten_arrays:
            Write array elements as individual scalar channels.

        Returns
        -------
        str
            Absolute path to the created DLIS file.
        """
        dataset = self.read(source, array_map=array_map)
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
        array_map: dict[str, list[str]] | None = None,
        flatten_arrays: bool = False,
        max_workers: int = 4,
    ) -> list[dict]:
        """
        Convert all ``.las`` files in *input_dir* to DLIS in parallel.

        LAS conversion is I/O-bound (reading curves, writing DLIS), so a
        thread pool gives meaningful throughput improvements without the
        overhead of ``multiprocessing``.

        Parameters
        ----------
        input_dir :
            Directory to scan.
        output_dir :
            Output directory.  Defaults to the same directory as each input.
        recursive :
            If ``True``, walk subdirectories.
        array_map :
            Applied to every file in the batch.
        flatten_arrays :
            Applied to every file in the batch.
        max_workers :
            Maximum concurrent conversions.  Defaults to 4.

        Returns
        -------
        list[dict]
            One dict per input file with keys:
            ``las_path``, ``dlis_path``, ``success``, ``error``.
        """
        inp = Path(input_dir)
        if not inp.is_dir():
            raise NotADirectoryError(f"Not a directory: {input_dir}")
        pattern = "**/*.las" if recursive else "*.las"
        las_files = sorted(inp.glob(pattern))
        if not las_files:
            logger.warning("No .las files in %s", input_dir)
            return []

        logger.info(
            "Batch LAS: %d files in %s (max_workers=%d)",
            len(las_files), input_dir, max_workers,
        )

        def _convert_one(lf: Path) -> dict:
            out_dir = Path(output_dir) if output_dir else lf.parent
            dp = str(out_dir / lf.with_suffix(".dlis").name)
            try:
                rp = self.convert(
                    str(lf), dp,
                    array_map=array_map,
                    flatten_arrays=flatten_arrays,
                )
                return dict(las_path=str(lf), dlis_path=rp, success=True, error=None)
            except Exception as exc:
                logger.error("Failed %s: %s", lf, exc)
                return dict(las_path=str(lf), dlis_path=dp, success=False, error=str(exc))
            finally:
                gc.collect()

        results: list[dict] = [None] * len(las_files)  # type: ignore[list-item]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_idx = {
                executor.submit(_convert_one, lf): i
                for i, lf in enumerate(las_files)
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                results[idx] = future.result()

        ok = sum(1 for r in results if r["success"])
        logger.info("Batch complete: %d/%d succeeded", ok, len(results))
        return results
