"""
dlis_builder.utils.naming
---------------------------
Channel name sanitisation and deduplication for RP 66 V1 compliance.

DLIS (RP 66 V1) requires that object names contain only printable ASCII
characters.  While the standard is more permissive in theory, real-world
DLIS viewers (Techlog, Petrel, dlisio) reject names with spaces, brackets,
hyphens in certain positions, or non-ASCII codepoints.
"""
from __future__ import annotations

import re
import unicodedata

# Only alphanumerics and underscores are universally safe in DLIS mnemonics.
_INVALID_RE = re.compile(r"[^A-Za-z0-9_]")


def to_ascii(text: str) -> str:
    """
    Coerce a Unicode string to pure ASCII for RP 66 V1 compatibility.

    Uses NFKD decomposition first (``Ä → A``, ``é → e``) then replaces
    any remaining non-ASCII bytes with ``?``.

    Parameters
    ----------
    text:
        Input string (may contain Unicode).

    Returns
    -------
    str
        Pure ASCII string, same length or shorter.

    Examples
    --------
    >>> to_ascii("Bühler")
    'Buhler'
    >>> to_ascii("temp °C")
    'temp ?C'
    """
    normalized = unicodedata.normalize("NFKD", text)
    return normalized.encode("ascii", errors="replace").decode("ascii")


def sanitize_channel_name(name: str) -> str:
    """
    Replace any character not in ``[A-Za-z0-9_]`` with an underscore.

    This converts LAS mnemonics like ``IMAGE-DYNAMIC[0]`` or ``SP (mV)``
    into DLIS-safe strings such as ``IMAGE_DYNAMIC_0_`` and ``SP__mV_``.

    Parameters
    ----------
    name:
        Raw channel mnemonic from source data.

    Returns
    -------
    str
        Sanitised mnemonic safe for DLIS.

    Examples
    --------
    >>> sanitize_channel_name("IMAGE-DYNAMIC[0]")
    'IMAGE_DYNAMIC_0_'
    >>> sanitize_channel_name("GR")
    'GR'
    """
    return _INVALID_RE.sub("_", name.strip())


def deduplicate_names(names: list[str]) -> list[str]:
    """
    Ensure all names in a list are unique.

    Duplicate names receive a numeric suffix: ``_2``, ``_3``, ... The first
    occurrence keeps its original name.

    Parameters
    ----------
    names:
        Input list (may contain duplicates).

    Returns
    -------
    list[str]
        Same length as input, all elements unique.

    Examples
    --------
    >>> deduplicate_names(["GR", "GR", "RHOB"])
    ['GR', 'GR_2', 'RHOB']
    """
    seen: dict[str, int] = {}
    result: list[str] = []
    for n in names:
        if n in seen:
            seen[n] += 1
            result.append(f"{n}_{seen[n]}")
        else:
            seen[n] = 1
            result.append(n)
    return result
