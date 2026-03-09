"""
dlis_builder._writer
----------------------
Core DLIS writing engine.  Consumes a :class:`~dlis_builder.models.WellDataset`
and produces a binary DLIS file via the ``dliswriter`` library.

This module is **internal** (leading underscore).  Callers should use
:class:`~dlis_builder.builder.DLISBuilder` or the converter classes.  The
function :func:`write_dlis` is the single entry point used by all converters
and the builder.

Channel ordering
----------------
DLIS viewers (Techlog, Petrel) require that array / multi-sample channels
appear **after** all scalar channels in a frame row.  Placing a scalar after
an array causes viewers to mis-calculate byte offsets and silently corrupt
every channel.  The writer enforces:

    ``[index] → [scalars in original order] → [arrays in original order]``

Null / absent value handling
-----------------------------
The LAS / source null value (e.g. ``-999.25`` or ``-9999``) is preserved as
the DLIS *absent value* sentinel.  Converting to a generic ``-999.25`` would
create apparent data spikes in software already configured for the original
null value.
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import numpy as np
from dliswriter import DLISFile

from dlis_builder.models.channel import Resolution
from dlis_builder.models.frame import FrameDef, WellDataset
from dlis_builder.utils.naming import to_ascii
from dlis_builder.utils.units import get_index_type
from dlis_builder.utils.validation import validate_dataset

logger = logging.getLogger(__name__)

# 64 MB chunk size prevents OOM on large files while remaining efficient.
# dliswriter default is 4 GB which is unsafe on constrained containers.
_SAFE_CHUNK: int = 64 * 1024 * 1024


def _encode_string_channel(name: str, arr: np.ndarray) -> tuple[np.ndarray, str]:
    """Encode a string / categorical channel as ``int32`` category codes.

    DLIS frame data is strictly numeric; string values must be pre-converted.
    Each unique label is assigned an integer code (starting from 1).  Null
    entries (``None``, ``NaN``, or empty string) are assigned code ``0``.

    Parameters
    ----------
    name:
        Channel mnemonic, used only for logging.
    arr:
        1-D numpy array with ``dtype=object``, ``'U'``, or ``'S'``.

    Returns
    -------
    codes : np.ndarray[int32]
        Category codes aligned to the input array.
    labels_str : str
        Lookup table in ``"0=<null>,1=Label1,2=Label2,..."`` format suitable
        for storage as a DLIS Parameter value.
    """

    def _is_null(v: object) -> bool:
        if v is None:
            return True
        if isinstance(v, float) and np.isnan(v):
            return True
        return False

    # First pass: collect unique non-null labels in first-seen order
    seen: dict[str, int] = {}
    next_code = 1
    for v in arr:
        if _is_null(v):
            continue
        s = str(v).strip()
        if s and s not in seen:
            seen[s] = next_code
            next_code += 1

    # Second pass: build the code array
    codes = np.empty(len(arr), dtype=np.int32)
    for i, v in enumerate(arr):
        if _is_null(v):
            codes[i] = 0
        else:
            codes[i] = seen.get(str(v).strip(), 0)

    parts = ["0=<null>"] + [
        f"{c}={lbl}"
        for lbl, c in sorted(seen.items(), key=lambda x: x[1])
    ]
    logger.info(
        "  String channel '%s': %d unique label(s) encoded as int32",
        name, len(seen),
    )
    return codes, ",".join(parts)


def write_dlis(
    dataset: WellDataset,
    output_path: str,
    *,
    flatten_arrays: bool = False,
) -> str:
    """
    Write a :class:`~dlis_builder.models.WellDataset` to a DLIS file.

    Parameters
    ----------
    dataset:
        Fully populated dataset (validated before writing starts).
    output_path:
        Destination file path.  Parent directories are created if absent.
    flatten_arrays:
        If ``True``, explode each array channel into individual scalar channels
        named ``<CHANNEL>_001 … <CHANNEL>_N``.  Use for legacy viewers that do
        not support ``dimension=[N]`` multi-sample channels.

    Returns
    -------
    str
        Absolute path to the created DLIS file.

    Raises
    ------
    ValueError
        If the dataset fails validation (no channels, mismatched lengths, etc.).
    """
    dataset.validate()
    validate_dataset(dataset)

    output_path = str(Path(output_path).resolve())
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    meta   = dataset.metadata
    origin = dataset.origin
    null   = float(meta.null_value) if meta.null_value is not None else -999.25

    absent_f64 = np.float64(null)
    absent_f32 = np.float32(null)

    logger.info("Writing DLIS: %s", output_path)
    logger.info(
        "  Well: %s  /  %s  /  %s",
        meta.well_name, meta.company, meta.field_name,
    )

    # ── 1. DLIS file + logical file ──────────────────────────────────────
    dlis_file = DLISFile()
    lf = dlis_file.add_logical_file(
        fh_id=f"dlis-builder-{origin.version}",
    )

    # ── 2. Origin ────────────────────────────────────────────────────────
    lf.add_origin(
        name="ORIGIN",
        well_name=to_ascii(meta.well_name),
        well_id=to_ascii(meta.well_id) if meta.well_id else None,
        company=to_ascii(meta.company),
        field_name=to_ascii(meta.field_name) if meta.field_name else "WILDCAT",
        producer_name=to_ascii(origin.producer_name),
        product=to_ascii(origin.product_name),
        version=origin.version,
        creation_time=origin.creation_time or datetime.now(),
        file_type=to_ascii(origin.file_type),
        file_set_name=to_ascii(origin.file_set_name) if origin.file_set_name else None,
        file_set_number=origin.file_set_number,
        file_number=origin.file_number,
        name_space_name=to_ascii(origin.name_space_name) if origin.name_space_name else None,
        name_space_version=origin.name_space_version,
        programs=["dlis-builder"],
    )

    # ── 3. Parameters ────────────────────────────────────────────────────
    ok = fail = 0
    for p in dataset.parameters:
        p_name = to_ascii(p.name)
        long_name = to_ascii(f"{p.description} [{p.unit}]" if p.unit else p.description or p.name)
        try:
            # Fast path: value is already a numeric type (ParameterDef.numeric())
            if isinstance(p.value, (int, float)):
                lf.add_parameter(name=p_name, long_name=long_name, values=[float(p.value)])
            else:
                # String value: try numeric conversion, fall back to text
                try:
                    lf.add_parameter(name=p_name, long_name=long_name, values=[float(str(p.value))])
                except (ValueError, TypeError):
                    lf.add_parameter(
                        name=p_name, long_name=long_name,
                        values=[to_ascii(str(p.value))],
                    )
            ok += 1
        except Exception as exc:
            fail += 1
            logger.debug("  Parameter '%s' skipped: %s", p.name, exc)
    if dataset.parameters:
        logger.info("  Parameters: %d written%s", ok, f" ({fail} failed)" if fail else "")

    # ── 4–6. Frames: reorder channels, build DLIS Channel objects, add frame ──
    # When dataset.frames is non-empty (multi-frame path) each FrameDef becomes
    # one DLIS Frame.  When it is empty the single legacy frame is used.
    frame_list: list[FrameDef] = (
        dataset.frames
        if dataset.frames
        else [FrameDef(name=origin.frame_name, channels=dataset.channels)]
    )

    # Track channel names across all frames to catch/rename duplicates.
    used_ch_names: set[str] = set()

    for f_idx, frame_def in enumerate(frame_list):
        index_chs = frame_def.channels[:1]
        if flatten_arrays:
            ordered = frame_def.channels
        else:
            scalars = [c for c in frame_def.channels[1:] if not c.is_array]
            arrays  = [c for c in frame_def.channels[1:] if c.is_array]
            ordered = index_chs + scalars + arrays

        dlis_ch_objects: list = []

        for ch in ordered:
            # Resolve cross-frame name conflict (e.g. two frames both named DEPT)
            ch_name = ch.name
            if ch_name in used_ch_names:
                ch_name = f"{ch.name}_F{f_idx + 1}"
                logger.warning(
                    "  Channel '%s' already used in a previous frame; "
                    "renamed to '%s' in frame '%s'.",
                    ch.name, ch_name, frame_def.name,
                )
            used_ch_names.add(ch_name)

            if ch.is_string:
                # ── String / categorical channel ──────────────────────────
                # dliswriter only supports numeric dtypes in frame data.  Encode
                # string values as int32 category codes and store the lookup
                # table as a companion DLIS Parameter (LABELS_<channel_name>).
                codes, labels_str = _encode_string_channel(ch.name, ch.data)
                param_name = to_ascii(f"LABELS_{ch_name}")[:249]
                try:
                    lf.add_parameter(
                        name=param_name,
                        long_name=to_ascii(f"Label lookup for {ch_name}"),
                        values=[labels_str],
                    )
                except Exception as exc:  # pragma: no cover
                    logger.debug(
                        "  Could not write label parameter for '%s': %s",
                        ch_name, exc,
                    )
                dlis_ch = lf.add_channel(
                    ch_name,
                    data=codes,
                    cast_dtype=np.int32,
                    long_name=to_ascii(ch.long_name),
                    units=ch.unit or "",
                )
                dlis_ch_objects.append(dlis_ch)

            elif ch.is_array and flatten_arrays:
                # Explode array into individual scalar channels
                width = len(str(ch.array_size))
                for k in range(ch.array_size):
                    elem_name = f"{ch_name}_{k+1:0{width}d}"
                    col = ch.data[:, k].astype(np.float64, copy=False)
                    if col is ch.data[:, k]:
                        col = col.copy()
                    nan_mask = np.isnan(col)
                    if nan_mask.any():
                        col[nan_mask] = absent_f64
                    elem_ch = lf.add_channel(
                        elem_name,
                        data=col,
                        cast_dtype=np.float64,
                        long_name=to_ascii(f"{ch.long_name} element {k+1}"),
                        units=ch.unit or "",
                    )
                    dlis_ch_objects.append(elem_ch)
            else:
                arr_dtype = (
                    (np.float32 if ch.resolution == Resolution.LOW else np.float64)
                    if ch.is_array
                    else np.float64
                )
                absent = (
                    (absent_f32 if ch.resolution == Resolution.LOW else absent_f64)
                    if ch.is_array
                    else absent_f64
                )
                # Avoid a redundant copy when the source array already has the
                # target dtype.  astype(copy=False) returns the same object when
                # dtypes match; we then take an explicit copy so the original is
                # never mutated.
                out_data = ch.data.astype(arr_dtype, copy=False)
                if out_data is ch.data:
                    out_data = out_data.copy()
                nan_mask = np.isnan(out_data)
                if nan_mask.any():
                    out_data[nan_mask] = absent

                kwargs: dict = dict(
                    cast_dtype=arr_dtype,
                    long_name=to_ascii(ch.long_name),
                    units=ch.unit or "",
                )
                if ch.is_array:
                    kwargs["dimension"] = ch.dimension
                    logger.info(
                        "  Array channel: %s  dim=%s  dtype=%s",
                        ch.name, ch.dimension,
                        "float32" if arr_dtype is np.float32 else "float64",
                    )

                dlis_ch = lf.add_channel(ch_name, data=out_data, **kwargs)
                dlis_ch_objects.append(dlis_ch)

        index_channel = ordered[0]
        index_type = get_index_type(index_channel.unit)
        lf.add_frame(
            to_ascii(frame_def.name),
            channels=dlis_ch_objects,
            index_type=index_type,
            spacing=None,
        )

    # ── 7. Write ─────────────────────────────────────────────────────────
    try:
        dlis_file.write(output_path, output_chunk_size=_SAFE_CHUNK)
    except TypeError:
        # Older dliswriter versions may not accept output_chunk_size
        dlis_file.write(output_path)

    size_kb = Path(output_path).stat().st_size / 1024
    logger.info("  Written: %s (%.1f KB)", output_path, size_kb)
    return output_path
