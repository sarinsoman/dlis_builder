"""
dlis_builder.contracts.source
-------------------------------
Structural-typing contracts (``typing.Protocol``) for data sources.

Any object that satisfies a Protocol can be used with the library without
explicitly inheriting from it.  This enables zero-coupling integration with
databases, REST APIs, message queues, in-memory datasets, etc.

Usage
-----
::

    from dlis_builder.contracts import DataSource
    from dlis_builder import DLISBuilder

    class MyDatabaseSource:
        \"\"\"Fetches well data from PostgreSQL — no explicit inheritance needed.\"\"\"

        def get_metadata(self) -> WellMetadata:
            row = db.execute("SELECT * FROM wells WHERE id=?", well_id).fetchone()
            return WellMetadata(well_name=row.name, company=row.company, ...)

        def get_channels(self) -> list[ChannelDef]:
            rows = db.execute("SELECT * FROM channels WHERE well_id=?", well_id)
            return [
                ChannelDef(name=r.mnemonic, unit=r.unit, data=np.array(r.samples))
                for r in rows
            ]

        def get_parameters(self) -> list[ParameterDef]:
            return []   # optional — empty list is fine

    source = MyDatabaseSource(well_id=42)
    DLISBuilder.from_source(source).build("output.dlis")
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from dlis_builder.models.channel import ChannelDef
from dlis_builder.models.metadata import ParameterDef, WellMetadata


@runtime_checkable
class DataSource(Protocol):
    """
    Read-only contract for any object that can supply well log data.

    Implement all three methods on your data-access object and pass it to
    :meth:`dlis_builder.DLISBuilder.from_source`.  No inheritance required.

    ``get_parameters`` is allowed to return an empty list — it is optional
    from the DLIS perspective.

    Method contract
    ---------------
    ``get_metadata() -> WellMetadata``
        Return well identification and file provenance.

    ``get_channels() -> list[ChannelDef]``
        Return an ordered list of channels.  The first element **must** be
        the depth (or time) index channel.

    ``get_parameters() -> list[ParameterDef]``
        Return acquisition / environmental parameters.  May be empty.
    """

    def get_metadata(self) -> WellMetadata:
        """Return well identification metadata."""
        ...

    def get_channels(self) -> list[ChannelDef]:
        """Return ordered channel definitions.  First = depth index."""
        ...

    def get_parameters(self) -> list[ParameterDef]:
        """Return acquisition parameters (empty list is valid)."""
        ...
