"""
dlis_builder.utils.validation
------------------------------
Input validation helpers used by converters and the builder.

Design philosophy
-----------------
Two levels of feedback are used, in line with Python conventions:

* **Warnings** (:class:`DLISMetadataWarning`) — for values that are
  technically allowed but likely wrong, e.g. well name left as "UNKNOWN".
  These never stop execution; callers can suppress them with ``warnings.filterwarnings``.

* **Errors** (``ValueError``, ``TypeError``) — for conditions that
  would produce a corrupt or unreadable DLIS file.  These always raise.

Public API
----------
:func:`validate_metadata`
    Checks a :class:`~dlis_builder.models.WellMetadata` for suspicious
    defaults and returns a list of human-readable warning strings.

:func:`validate_dataset`
    Full pre-write validation of a :class:`~dlis_builder.models.WellDataset`.
    Raises ``ValueError`` on any critical problem.

:func:`validate_channel_names`
    Checks that channel names are DLIS-safe (max 256 chars, printable ASCII).

:func:`validate_csv_config`
    Validates CSV converter config before reading the file.

Examples
--------
::

    import warnings
    from dlis_builder.utils.validation import validate_metadata, DLISMetadataWarning

    # Get warnings as exceptions (useful in CI / strict mode)
    warnings.filterwarnings("error", category=DLISMetadataWarning)

    from dlis_builder.models import WellMetadata
    validate_metadata(WellMetadata())   # raises DLISMetadataWarning: well_name is UNKNOWN
"""
from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dlis_builder.models.channel import ChannelDef
    from dlis_builder.models.frame import WellDataset
    from dlis_builder.models.metadata import WellMetadata


# ── Custom warning category ───────────────────────────────────────────────────

class DLISMetadataWarning(UserWarning):
    """
    Raised when metadata fields have suspicious defaults.

    Treat as a warning (not an error) because files with default metadata
    are still valid DLIS — they are just harder to identify in data management
    systems.

    To turn these warnings into errors in tests or CI pipelines::

        import warnings
        warnings.filterwarnings("error", category=DLISMetadataWarning)
    """


# ── Metadata validation ───────────────────────────────────────────────────────

_DEFAULT_WELL_NAME = "UNKNOWN"
_DEFAULT_COMPANY   = "UNKNOWN"

# Fields that should not be left at their default value for production data
_SUSPICIOUS_DEFAULTS: dict[str, str] = {
    "well_name": _DEFAULT_WELL_NAME,
    "company":   _DEFAULT_COMPANY,
}


def validate_metadata(meta: WellMetadata) -> list[str]:
    """
    Check a :class:`~dlis_builder.models.WellMetadata` for suspicious defaults.

    Issues warnings (not errors) so callers can decide how strict to be.

    Parameters
    ----------
    meta:
        Well metadata to check.

    Returns
    -------
    list[str]
        Human-readable warning messages.  Empty list means no issues found.

    Examples
    --------
    ::

        msgs = validate_metadata(WellMetadata())
        # ['well_name is "UNKNOWN" — provide a real well identifier',
        #  'company is "UNKNOWN" — provide the operator name']

        # Alternatively, turn them into hard errors for CI / strict mode:
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("error", DLISMetadataWarning)
            validate_metadata(WellMetadata(company="Acme"))
    """
    msgs: list[str] = []

    for field_name, default_val in _SUSPICIOUS_DEFAULTS.items():
        actual = getattr(meta, field_name, default_val)
        if str(actual).strip().upper() == default_val.upper():
            msgs.append(
                f"{field_name!r} is {default_val!r} — "
                f"provide a real value so the file can be identified in data management systems."
            )

    if meta.null_value is not None:
        if float(meta.null_value) > 0:
            msgs.append(
                f"null_value={meta.null_value!r} is positive.  "
                "DLIS sentinel values are conventionally negative (e.g. -999.25, -9999)."
            )

    for msg in msgs:
        warnings.warn(msg, DLISMetadataWarning, stacklevel=3)

    return msgs


# ── Dataset validation ────────────────────────────────────────────────────────

def validate_dataset(ds: WellDataset) -> None:
    """
    Full pre-write validation of a :class:`~dlis_builder.models.WellDataset`.

    Raises ``ValueError`` on conditions that would produce an unreadable file.
    Issues :class:`DLISMetadataWarning` for suspicious but non-fatal settings.

    When ``ds.frames`` is non-empty each frame is validated independently,
    enabling channels with different depth samplings to coexist in one file.

    Parameters
    ----------
    ds:
        Dataset to validate.

    Raises
    ------
    ValueError
        * No channels defined.
        * Channel length mismatch within a frame.
        * Array channel ``dimension`` inconsistent with data shape.
        * Index channel contains NaN values.
        * Duplicate channel names within a frame.
    """
    import numpy as np

    # Determine which channel lists to validate
    if ds.frames:
        frame_channel_lists = [f.channels for f in ds.frames if f.channels]
        if not frame_channel_lists:
            raise ValueError("Dataset has no channels.  Add at least a depth channel.")
    else:
        if not ds.channels:
            raise ValueError("Dataset has no channels.  Add at least a depth channel.")
        frame_channel_lists = [ds.channels]

    for channels in frame_channel_lists:
        # Index channel must be 1-D and NaN-free
        idx = channels[0]
        if idx.data.ndim != 1:
            raise ValueError(
                f"Index channel '{idx.name}' must be 1-D; got shape {idx.data.shape}."
            )
        # np.isnan only works on float/complex dtypes; skip for object/integer arrays.
        if (
            hasattr(np, "any")
            and idx.data.dtype.kind in ("f", "c")
            and bool(np.any(np.isnan(idx.data)))
        ):
            raise ValueError(
                f"Index channel '{idx.name}' contains NaN values.  "
                "The depth / time axis must be fully populated."
            )

        # All channels in this frame must have the same number of samples
        n = len(idx.data)
        for ch in channels[1:]:
            if len(ch.data) != n:
                raise ValueError(
                    f"Channel '{ch.name}' has {len(ch.data)} samples but "
                    f"index channel '{idx.name}' has {n}.  "
                    "All channels must share the same depth axis."
                )

        # Array channels: dimension must match data.shape[1]
        for ch in channels:
            if ch.is_array:
                if ch.data.ndim != 2:
                    raise ValueError(
                        f"Array channel '{ch.name}' must have 2-D data "
                        f"(samples \u00d7 elements); got shape {ch.data.shape}."
                    )
                declared = ch.dimension[0] if ch.dimension else None
                actual   = ch.data.shape[1]
                if declared is not None and declared != actual:
                    raise ValueError(
                        f"Array channel '{ch.name}' declares dimension={declared} "
                        f"but data has {actual} columns.  "
                        "Ensure dimension=[N] matches data.shape[1]."
                    )

        # Duplicate channel names corrupt frame byte offsets in many viewers
        names = [c.name for c in channels]
        seen: set[str] = set()
        duplicates: list[str] = []
        for name in names:
            if name in seen:
                duplicates.append(name)
            seen.add(name)
        if duplicates:
            raise ValueError(
                f"Duplicate channel name(s): {duplicates}.  "
                "DLIS requires unique channel mnemonics within a frame."
            )

    # Metadata warnings (non-fatal)
    validate_metadata(ds.metadata)


# ── Channel-name validation ───────────────────────────────────────────────────

_MAX_MNEMONIC_LEN = 256  # RP 66 V1 allows up to 256 chars


def validate_channel_names(channels: list[ChannelDef]) -> None:
    """
    Check that channel names satisfy DLIS mnemonic constraints.

    Raises
    ------
    ValueError
        If any name exceeds 256 characters or contains non-printable bytes.
    """
    for ch in channels:
        if len(ch.name) > _MAX_MNEMONIC_LEN:
            raise ValueError(
                f"Channel name '{ch.name[:30]}…' is {len(ch.name)} characters long.  "
                f"RP 66 V1 limits mnemonic identifiers to {_MAX_MNEMONIC_LEN} characters."
            )
        if not ch.name.isprintable():
            raise ValueError(
                f"Channel name {ch.name!r} contains non-printable characters.  "
                "DLIS mnemonic identifiers must be printable ASCII."
            )


# ── CSV-specific validation ───────────────────────────────────────────────────

def validate_csv_config(
    columns: list[str],
    depth_column: str,
    array_columns: dict[str, list[str]],
) -> None:
    """
    Validate CSV converter configuration against the actual file columns.

    Parameters
    ----------
    columns:
        List of column names read from the CSV header.
    depth_column:
        Name of the column to use as the depth index.
    array_columns:
        Explicit array-grouping map from `CSVConverter`.

    Raises
    ------
    ValueError
        If the depth column is missing.
    UserWarning
        If any array group column is not found in the CSV.
    """
    if depth_column not in columns:
        raise ValueError(
            f"Depth column {depth_column!r} not found in CSV.\n"
            f"  Available columns: {columns}\n"
            f"  Tip: pass depth_column='...' to CSVConverter matching an actual column name,\n"
            f"       or leave it as None to use the first column automatically."
        )

    for group_name, members in array_columns.items():
        missing = [m for m in members if m not in columns]
        if missing:
            warnings.warn(
                f"Array group '{group_name}': {len(missing)} column(s) not found in CSV "
                f"and will be ignored: {missing[:5]}{'…' if len(missing) > 5 else ''}",
                stacklevel=3,
            )
