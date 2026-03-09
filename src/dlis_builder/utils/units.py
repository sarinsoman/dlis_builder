"""
dlis_builder.utils.units
--------------------------
Unit string normalisation from LAS / CSV free-form values to RP 66 V1
canonical vocabulary used by DLIS.

Usage
-----
::

    from dlis_builder.utils.units import normalize_unit

    normalize_unit("G/CC")    # → "g/cm3"
    normalize_unit("OHMM")    # → "ohm.m"
    normalize_unit("GAPI")    # → "gAPI"
    normalize_unit("US/FT")   # → "us/ft"
    normalize_unit("unknown") # → "unknown"   (pass-through)
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Lookup table: upper-cased, space-stripped LAS string → DLIS canonical unit
# ---------------------------------------------------------------------------
UNIT_MAP: dict[str, str] = {
    # ── Depth / Length ──────────────────────────────────────────────────────
    "M": "m", "FT": "ft", "F": "ft", "FEET": "ft",
    "IN": "in", "INCH": "in", "INCHES": "in",
    "CM": "cm", "MM": "mm", "KM": "km",
    # ── Porosity ────────────────────────────────────────────────────────────
    "V/V": "V/V", "PU": "pu", "%": "pu", "FRAC": "V/V", "DEC": "V/V",
    # ── Density ─────────────────────────────────────────────────────────────
    "G/CM3": "g/cm3", "G/CC":  "g/cm3", "GM/CC": "g/cm3", "G/C3": "g/cm3",
    "KG/M3": "kg/m3", "K/M3":  "kg/m3", "PPG": "ppg",
    # ── Resistivity ─────────────────────────────────────────────────────────
    "OHMM": "ohm.m", "OHM.M": "ohm.m", "OHM-M": "ohm.m", "OHMM2/M": "ohm.m",
    # ── Sonic ───────────────────────────────────────────────────────────────
    "US/F": "us/ft", "US/FT": "us/ft", "USEC/FT": "us/ft",
    "US/M": "us/m",  "USEC/M": "us/m",
    # ── Gamma Ray ───────────────────────────────────────────────────────────
    "GAPI": "gAPI", "API": "gAPI",
    # ── Potential ───────────────────────────────────────────────────────────
    "MV": "mV", "MILLIVOLT": "mV", "V": "V",
    # ── Temperature ─────────────────────────────────────────────────────────
    "DEGF": "degF", "DEGC": "degC", "DEG F": "degF", "DEG C": "degC",
    # ── Pressure ────────────────────────────────────────────────────────────
    "PSI": "psi", "PSIG": "psi", "KPA": "kPa", "MPA": "MPa",
    "BAR": "bar", "ATM": "atm",
    # ── Time ────────────────────────────────────────────────────────────────
    "S": "s", "MS": "ms", "US": "us", "MIN": "min", "H": "h", "HR": "h",
    # ── Angle ───────────────────────────────────────────────────────────────
    "DEG": "deg", "RAD": "rad",
    # ── Permeability ────────────────────────────────────────────────────────
    "MD": "mD", "DARCY": "D",
    # ── Viscosity ───────────────────────────────────────────────────────────
    "CP": "cP",
    # ── Concentration ───────────────────────────────────────────────────────
    "PPM": "ppm",
    # ── Flow / Rate ─────────────────────────────────────────────────────────
    "GPM": "gpm", "FPH": "fph", "FPM": "fpm", "RPM": "rpm",
    # ── Mass ────────────────────────────────────────────────────────────────
    "LBM": "lbm", "KG": "kg", "G": "g", "KLB": "klb",
    # ── Misc ────────────────────────────────────────────────────────────────
    "B/E": "b", "B": "b",
}

# Index type mapping: normalised DLIS unit → RP 66 V1 index type string
INDEX_TYPE_MAP: dict[str, str] = {
    "m":   "BOREHOLE-DEPTH",
    "ft":  "BOREHOLE-DEPTH",
    "s":   "NON-STANDARD",
    "ms":  "NON-STANDARD",
    "min": "NON-STANDARD",
    "h":   "NON-STANDARD",
}


def normalize_unit(raw: str | None) -> str | None:
    """
    Normalise a free-form LAS / CSV unit string to an RP 66 V1 unit.

    Lookup is case-insensitive and strips surrounding whitespace.
    Unrecognised strings are returned unchanged (pass-through).

    Parameters
    ----------
    raw:
        Raw unit string from the source file (e.g. ``"OHMM"``, ``"G/CC"``).

    Returns
    -------
    str | None
        Normalised unit, or ``None`` / empty string if *raw* was falsy.

    Examples
    --------
    >>> normalize_unit("G/CC")
    'g/cm3'
    >>> normalize_unit("OHMM")
    'ohm.m'
    >>> normalize_unit("my_custom_unit")
    'my_custom_unit'
    """
    if not raw or not raw.strip():
        return None
    key = raw.strip().upper().replace(" ", "")
    return UNIT_MAP.get(key, raw.strip())


def get_index_type(unit: str | None) -> str:
    """
    Map a normalised unit string to a DLIS index type.

    Returns ``"BOREHOLE-DEPTH"`` for depth units and ``"NON-STANDARD"``
    for time units.  Defaults to ``"BOREHOLE-DEPTH"`` for unknown units.
    """
    if not unit:
        return "BOREHOLE-DEPTH"
    return INDEX_TYPE_MAP.get(unit.lower(), "BOREHOLE-DEPTH")
