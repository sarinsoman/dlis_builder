"""
dlis_builder.models.metadata
-----------------------------
Pure data models for well / file-level metadata.  No I/O logic lives here.

Two classes cover different levels of identity in a DLIS file:

* :class:`WellMetadata` -- what is in the file: well name, company,
  field, location, null value.  Written to the DLIS Origin well fields.
* :class:`DLISFileConfig` (alias :class:`OriginConfig`) -- how the file
  was produced: software name, version, file set number, frame name.
  Written to the DLIS Origin producer fields.

Most users only need :class:`WellMetadata`.  Use :class:`DLISFileConfig`
when you need to customise DLIS file-level identity (multi-file sets,
custom frame naming, producer attribution).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class WellMetadata:
    """
    Well-level identification and provenance.

    Written into the DLIS Origin object (primary identification record).

    Parameters
    ----------
    well_name : str
        Official well name.
    well_id : str
        UWI / API / national well identifier.
    company : str
        Operating company / owner.
    field_name : str
        Field or block name.
    country : str
        ISO country code (e.g. "UAE", "GBR").
    null_value : float
        Absent-data sentinel.  Conventionally negative (-999.25, -9999.0).

    Examples
    --------
    Direct construction::

        meta = WellMetadata(
            well_name="WELL_A",
            company="Acme Energy",
            field_name="NORTH_BLOCK",
            country="UAE",
            null_value=-9999.0,
        )

    From a database row or API response dict::

        meta = WellMetadata.from_dict(row._asdict())
    """

    well_name: str = "UNKNOWN"
    well_id: str = ""
    company: str = "UNKNOWN"
    field_name: str = "WILDCAT"
    country: str = ""
    state: str = ""
    county: str = ""
    location: str = ""
    service_company: str = ""
    date: str = ""
    null_value: float = -999.25

    @classmethod
    def from_dict(cls, data: dict) -> WellMetadata:
        """
        Create from a plain dict; unknown keys are silently ignored.

        Examples
        --------
        ::

            meta = WellMetadata.from_dict(
                {"well_name": "F-15", "company": "Shell", "null_value": -9999.0}
            )
        """
        valid = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in data.items() if k in valid})

    def __repr__(self) -> str:
        parts = [f"well_name={self.well_name!r}", f"company={self.company!r}"]
        if self.field_name and self.field_name != "WILDCAT":
            parts.append(f"field_name={self.field_name!r}")
        if self.country:
            parts.append(f"country={self.country!r}")
        return f"WellMetadata({', '.join(parts)})"


@dataclass
class OriginConfig:
    """
    DLIS file-level configuration (producer / software identity).

    Also exported as :class:`DLISFileConfig` (preferred alias).

    Parameters
    ----------
    producer_name : str
        Organisation that produced the file (DLIS PRODUCER-NAME).
    product_name : str
        Software that created the file (DLIS PRODUCT).
    version : str
        Software version.
    file_type : str
        RP 66 V1 file type: "LAS CONVERSION", "CALIBRATED", "PROCESSED", etc.
    frame_name : str
        Primary DLIS Frame name.  Use descriptive names for multi-frame files
        ("DEPTH-FRAME", "TIME-FRAME").
    file_set_name : str, optional
        DLIS FILE-SET-NAME for multi-file sets.
    file_set_number : int, optional
        Integer FILE-SET-NUMBER.
    file_number : int, optional
        Integer FILE-NUMBER within the set.
    creation_time : datetime
        File creation timestamp.  Defaults to now().

    Examples
    --------
    ::

        from dlis_builder.models import DLISFileConfig

        cfg = DLISFileConfig(
            producer_name="Acme Logging",
            product_name="MyApp",
            version="2.1.0",
            file_type="LAS CONVERSION",
            frame_name="DEPTH-FRAME",
        )
    """

    producer_name: str = ""
    product_name: str = "dlis-builder"
    version: str = "1.0.0"
    file_type: str = "LAS CONVERSION"
    frame_name: str = "MAIN-FRAME"
    file_set_name: str | None = None
    file_set_number: int | None = None
    file_number: int | None = None
    name_space_name: str | None = None
    name_space_version: int | None = None
    creation_time: datetime = field(default_factory=datetime.now)

    @classmethod
    def from_dict(cls, data: dict) -> OriginConfig:
        """Create from a plain dict; unknown keys are silently ignored."""
        valid = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in data.items() if k in valid})

    def __repr__(self) -> str:
        return (
            f"DLISFileConfig(producer_name={self.producer_name!r}, "
            f"file_type={self.file_type!r}, "
            f"frame_name={self.frame_name!r})"
        )


# DLISFileConfig is the user-facing name; OriginConfig kept for back-compat.
DLISFileConfig = OriginConfig


@dataclass
class ParameterDef:
    """
    A single acquisition or environmental parameter to embed in the DLIS file.

    Examples
    --------
    ::

        p = ParameterDef(name="BHT", unit="degC", value="85.0",
                         description="Bottom Hole Temperature")
    """

    name: str
    value: str | float | int
    unit: str = ""
    description: str = ""

    @classmethod
    def numeric(cls, name: str, value: float, unit: str = "",
                description: str = "") -> ParameterDef:
        """Convenience constructor for numeric values. Stores the value as float."""
        return cls(name=name, value=float(value), unit=unit, description=description)

    @classmethod
    def text(cls, name: str, value: str,
             description: str = "") -> ParameterDef:
        """Convenience constructor for string values."""
        return cls(name=name, value=value, unit="", description=description)
