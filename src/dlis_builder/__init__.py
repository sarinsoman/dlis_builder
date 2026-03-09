"""
dlis_builder
============

Python library for creating DLIS (Digital Log Interchange Standard) files
from any data source: LAS files, CSV files, databases, REST APIs, or
any in-memory dataset.

Quick-start examples
--------------------

**Programmatic builder (database / API data):**

::

    import numpy as np
    from dlis_builder import DLISBuilder
    from dlis_builder.models import WellMetadata, ChannelDef, ParameterDef, Resolution

    depth = np.linspace(300.0, 600.0, 2001)
    gr    = np.random.uniform(20, 120, 2001)
    image = np.random.uniform(0.1, 10.0, (2001, 120))

    path = (
        DLISBuilder()
        .set_origin(WellMetadata(well_name="WELL_A", company="Acme"))
        .add_parameter(ParameterDef.numeric("BHT", 85.0, "degC", "Bottom Hole Temp"))
        .add_channel(ChannelDef(name="DEPT",  unit="m",     data=depth))
        .add_channel(ChannelDef(name="GR",    unit="gAPI",  data=gr))
        .add_channel(ChannelDef(name="IMAGE", unit="ohm.m", data=image,
                                dimension=[120], resolution=Resolution.LOW))
        .build("output.dlis")
    )

**DataSource protocol (any database / API object):**

::

    from dlis_builder import DLISBuilder
    from dlis_builder.contracts import DataSource

    class WellRepository:
        def get_metadata(self)   -> WellMetadata:     ...
        def get_channels(self)   -> list[ChannelDef]: ...
        def get_parameters(self) -> list[ParameterDef]: ...

    DLISBuilder.from_source(WellRepository(db, well_id=42)).build("out.dlis")

**LAS → DLIS:**

::

    from dlis_builder.converters import LASConverter

    LASConverter().convert("well.las", "well.dlis")

    # With explicit image grouping
    LASConverter(image_resolution="high").convert(
        "borehole_image.las", "image.dlis",
        array_map={"FMI": [f"PAD_{i:03d}" for i in range(1, 129)]},
    )

**CSV → DLIS:**

::

    from dlis_builder.converters import CSVConverter
    from dlis_builder.models import WellMetadata

    CSVConverter(
        depth_column="DEPTH",
        column_units={"DEPTH": "m", "GR": "gAPI", "RHOB": "g/cm3"},
        null_value=-9999.0,
    ).convert(
        "data.csv", "output.dlis",
        metadata=WellMetadata(well_name="WELL_A", company="Acme"),
    )
"""
from dlis_builder._version import __version__
from dlis_builder.builder import DLISBuilder
from dlis_builder.converters.csv import CSVLayout, detect_csv_layout
from dlis_builder.models import (
    ChannelDef,
    DLISFileConfig,
    FrameDef,
    OriginConfig,
    ParameterDef,
    Resolution,
    WellDataset,
    WellMetadata,
)

__all__ = [
    # Version
    "__version__",
    # Primary entry point
    "DLISBuilder",
    # Models
    "WellMetadata",
    "OriginConfig",
    "DLISFileConfig",
    "ParameterDef",
    "ChannelDef",
    "Resolution",
    "FrameDef",
    "WellDataset",
    # CSV layout detection
    "detect_csv_layout",
    "CSVLayout",
]
