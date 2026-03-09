"""
dlis_builder.builder
----------------------
:class:`DLISBuilder` — fluent programmatic API for constructing DLIS files
from any data source: database queries, REST API responses, computed arrays,
or any in-memory dataset.

This is the primary entry point for custom integrations.

Quick start
-----------
::

    import numpy as np
    from dlis_builder import DLISBuilder
    from dlis_builder.models import WellMetadata, ChannelDef, ParameterDef, Resolution

    depth = np.linspace(300.0, 600.0, 2001)
    gr    = np.random.uniform(20, 120, 2001)
    image = np.random.uniform(0.1, 10.0, (2001, 120))   # 120 azimuths

    path = (
        DLISBuilder()
        .set_origin(WellMetadata(well_name="WELL_A", company="Acme", field_name="NORTH"))
        .add_parameter(ParameterDef.numeric("BHT", 85.0, unit="degC",
                                            description="Bottom Hole Temperature"))
        .add_channel(ChannelDef(name="DEPT", unit="m",    data=depth))
        .add_channel(ChannelDef(name="GR",   unit="gAPI", data=gr))
        .add_channel(ChannelDef(name="IMAGE", unit="ohm.m", data=image,
                                dimension=[120], resolution=Resolution.LOW))
        .build("output.dlis")
    )

Database / API integration
--------------------------
Implement the :class:`~dlis_builder.contracts.DataSource` protocol on your
data-access object and use :meth:`DLISBuilder.from_source`::

    from dlis_builder.contracts import DataSource

    class WellRepository:
        def get_metadata(self) -> WellMetadata: ...
        def get_channels(self) -> list[ChannelDef]: ...
        def get_parameters(self) -> list[ParameterDef]: ...

    repo = WellRepository(connection, well_id=42)
    path = DLISBuilder.from_source(repo).build("output.dlis")
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from dlis_builder._writer import write_dlis
from dlis_builder.contracts.source import DataSource
from dlis_builder.models.channel import ChannelDef, Resolution
from dlis_builder.models.frame import FrameDef, WellDataset
from dlis_builder.models.metadata import OriginConfig, ParameterDef, WellMetadata

if TYPE_CHECKING:  # pragma: no cover
    pass

logger = logging.getLogger(__name__)

# ── Depth-index channel detection ──────────────────────────────────────────
# Used by _detect_frames() to find the index channel in each depth group.
_DEPTH_MNEMONICS: frozenset[str] = frozenset({
    "DEPT", "DEPTH", "MD", "TDEP", "TVD", "TVDSS", "TVDMD",
    "BOREHOLE-DEPTH", "BOREHOLE_DEPTH",
})
_DEPTH_UNITS: frozenset[str] = frozenset({
    "m", "ft", "in", "cm", "mm", "km",
})


def _find_index_channel(channels: list[ChannelDef]) -> ChannelDef | None:
    """Return the first identifiable depth/index channel in *channels*, or None."""
    for ch in channels:
        if ch.name.upper() in _DEPTH_MNEMONICS:
            return ch
    for ch in channels:
        if ch.unit and ch.unit.lower() in _DEPTH_UNITS:
            return ch
    return None


class DLISBuilder:
    """
    Fluent builder for constructing and writing a DLIS file.

    All ``add_*`` and ``set_*`` methods return ``self`` so calls can be
    chained.  Call :meth:`build` to write the file and get the output path.

    Thread safety
    -------------
    Instances are **not** thread-safe.  Create one builder per output file.
    """

    def __init__(self) -> None:
        self._metadata: WellMetadata = WellMetadata()
        self._origin: OriginConfig = OriginConfig()
        self._channels: list[ChannelDef] = []
        self._parameters: list[ParameterDef] = []
        self._flatten: bool = False

    def __repr__(self) -> str:  # pragma: no cover
        well = self._metadata.well_name or "<unnamed>"
        return (
            f"DLISBuilder(well={well!r}, "
            f"channels={len(self._channels)}, "
            f"parameters={len(self._parameters)})"
        )

    # ------------------------------------------------------------------ #
    #  Class-level factory                                                 #
    # ------------------------------------------------------------------ #

    @classmethod
    def from_source(cls, source: DataSource) -> DLISBuilder:
        """
        Populate a builder from any object that satisfies the
        :class:`~dlis_builder.contracts.DataSource` protocol.

        No inheritance is required — any object with ``get_metadata()``,
        ``get_channels()``, and ``get_parameters()`` methods qualifies.

        Parameters
        ----------
        source:
            Data-access object (database repository, API client, …).

        Returns
        -------
        DLISBuilder
            A builder pre-populated from the source, ready for :meth:`build`.

        Raises
        ------
        TypeError
            If *source* does not implement the ``DataSource`` protocol.

        Examples
        --------
        ::

            class APIClient:
                def get_metadata(self)   -> WellMetadata:   ...
                def get_channels(self)   -> list[ChannelDef]: ...
                def get_parameters(self) -> list[ParameterDef]: ...

            DLISBuilder.from_source(APIClient(base_url, well_id)).build("out.dlis")
        """
        if not isinstance(source, DataSource):
            raise TypeError(
                f"{type(source).__name__!r} does not implement the DataSource "
                f"protocol.  It must have get_metadata(), get_channels(), and "
                f"get_parameters() methods."
            )
        builder = cls()
        builder.set_origin(source.get_metadata())
        for ch in source.get_channels():
            builder.add_channel(ch)
        for p in source.get_parameters():
            builder.add_parameter(p)
        return builder

    @classmethod
    def from_las(
        cls,
        path: str,
        *,
        array_map: dict[str, list[str]] | None = None,
        flatten_arrays: bool = False,
        image_resolution: str = "auto",
    ) -> DLISBuilder:
        """
        Convenience factory: read a LAS file and return a pre-populated builder.

        This is equivalent to::

            from dlis_builder.converters import LASConverter
            ds = LASConverter(image_resolution=image_resolution).read(path, array_map=array_map)
            builder = DLISBuilder()
            builder.set_origin(ds.metadata)
            for ch in ds.channels:
                builder.add_channel(ch)

        Parameters
        ----------
        path :
            Path to the ``.las`` file.
        array_map :
            Explicit channel-grouping map (passed to :class:`LASConverter`).
        flatten_arrays :
            Expand array channels into scalar columns.
        image_resolution :
            ``"high"``, ``"low"``, or ``"auto"`` (default – infer from array width).

        Returns
        -------
        DLISBuilder
            Pre-populated with channels and metadata from the LAS file.
        """
        from dlis_builder.converters.las import LASConverter  # noqa: PLC0415

        conv = LASConverter(image_resolution=image_resolution)
        ds = conv.read(path, array_map=array_map)
        builder = cls()
        builder.set_origin(ds.metadata)
        for ch in ds.channels:
            builder.add_channel(ch)
        for p in ds.parameters:
            builder.add_parameter(p)
        builder.set_flatten_arrays(flatten_arrays)
        return builder

    @classmethod
    def from_csv(
        cls,
        path: str,
        *,
        depth_column: str | None = None,
        columns: list[str] | None = None,
        column_units: dict[str, str] | None = None,
        array_columns: dict[str, list[str]] | None = None,
        flatten_arrays: bool = False,
        metadata: WellMetadata | None = None,
        **converter_kwargs: Any,
    ) -> DLISBuilder:
        """
        Convenience factory: read a CSV file and return a pre-populated builder.

        Unit rows and depth columns are auto-detected when not provided.  Pass
        the relevant arguments to override auto-detection.

        Parameters
        ----------
        path :
            Path to the ``.csv`` file.
        depth_column :
            Column name to use as the depth index.
            ``None`` (default) → auto-detected.
        columns :
            Ordered list of column names to include.
            ``None`` → all detected columns.
        column_units :
            ``{column_name: unit_string}`` overrides.  Auto-detected units are
            used for columns not listed here.
        array_columns :
            ``{array_name: [col1, col2, ...]}`` grouping for multi-element channels.
        flatten_arrays :
            Expand array channels into scalar columns.
        metadata :
            Well metadata (CSV files carry none; always provide this for
            production use).
        **converter_kwargs :
            Forwarded to :class:`~dlis_builder.converters.CSVConverter`.

        Returns
        -------
        DLISBuilder
            Pre-populated with channels from the CSV file.
        """
        from dlis_builder.converters.csv import CSVConverter  # noqa: PLC0415

        conv = CSVConverter(
            depth_column=depth_column,
            columns=columns,
            column_units=column_units or {},
            array_columns=array_columns or {},
            **converter_kwargs,
        )
        ds = conv.read(path)
        builder = cls()
        if metadata is not None:
            builder.set_origin(metadata)
        for ch in ds.channels:
            builder.add_channel(ch)
        for p in ds.parameters:
            builder.add_parameter(p)
        builder.set_flatten_arrays(flatten_arrays)
        return builder

    # ------------------------------------------------------------------ #
    #  Configuration methods (return self for chaining)                   #
    # ------------------------------------------------------------------ #

    def set_origin(self, metadata: WellMetadata) -> DLISBuilder:
        """
        Set well / file identification metadata.

        Parameters
        ----------
        metadata:
            Well name, company, field, null value, etc.
        """
        self._metadata = metadata
        return self

    def set_origin_config(self, config: OriginConfig) -> DLISBuilder:
        """
        Set DLIS file-level origin configuration (producer, version, etc.).

        Parameters
        ----------
        config:
            Producer name, product name, file type, frame name, etc.
        """
        self._origin = config
        return self

    def set_file_config(self, config: OriginConfig) -> DLISBuilder:
        """
        User-friendly alias for :meth:`set_origin_config`.

        Accepts a :class:`~dlis_builder.models.DLISFileConfig` (which is the
        same class as :class:`~dlis_builder.models.OriginConfig`) so that
        the most common pattern reads naturally::

            from dlis_builder.models import DLISFileConfig

            cfg = DLISFileConfig.from_dict({
                "producer_name": "Acme",
                "file_type": "WELL-LOG",
            })
            builder.set_file_config(cfg)
        """
        return self.set_origin_config(config)

    def add_channel(self, channel: ChannelDef) -> DLISBuilder:
        """
        Append a channel to the dataset.

        The *first* channel added must be the depth (or time) index channel.
        Subsequent channels may be scalar or array.

        Parameters
        ----------
        channel:
            Channel definition including its data array.
        """
        self._channels.append(channel)
        return self

    def add_channels(self, channels: list[ChannelDef]) -> DLISBuilder:
        """
        Append multiple channels at once.

        Equivalent to calling :meth:`add_channel` for each element.
        """
        for ch in channels:
            self.add_channel(ch)
        return self

    def add_parameter(self, parameter: ParameterDef) -> DLISBuilder:
        """
        Add an acquisition or environmental parameter.

        Parameters are written as DLIS Parameter objects alongside the
        channel data.  They are optional but recommended for traceability.

        Parameters
        ----------
        parameter:
            Name, value, unit, and description of the parameter.
        """
        self._parameters.append(parameter)
        return self

    def set_flatten_arrays(self, flatten: bool = True) -> DLISBuilder:
        """
        Enable or disable array-flattening mode.

        When ``flatten=True``, each element of an array channel becomes its own
        scalar channel (e.g. ``IMAGE_001 … IMAGE_120``).  This improves
        compatibility with legacy DLIS viewers that do not support
        ``dimension=[N]`` multi-sample channels.

        Parameters
        ----------
        flatten:
            ``True`` to enable flatten mode.  Default ``True`` (explicit opt-in).
        """
        self._flatten = flatten
        return self

    # ------------------------------------------------------------------ #
    #  Terminal operation                                                  #
    # ------------------------------------------------------------------ #

    def _detect_frames(self) -> list[FrameDef]:
        """
        Group ``self._channels`` into :class:`FrameDef` objects by row count.

        Returns an empty list when all channels share the same row count
        (single-frame path, fully backward compatible).

        If more than one row count is found, each group is mapped to a
        ``FrameDef``.  The first channel in each group whose name matches a
        well-known depth mnemonic, or whose unit is a length unit, becomes
        that frame’s index channel.

        If ANY group has no identifiable index channel, the method falls back
        to returning ``[]`` so that the existing single-frame validation
        (:meth:`WellDataset.validate`) raises the standard
        “mismatched lengths” ``ValueError``.  This preserves the pre-existing
        error behaviour for datasets that are simply missing a channel.
        """
        from collections import defaultdict  # noqa: PLC0415

        groups: dict[int, list[ChannelDef]] = defaultdict(list)
        for ch in self._channels:
            groups[len(ch.data)].append(ch)

        if len(groups) <= 1:
            return []   # single depth sampling — use legacy path

        base_name = self._origin.frame_name or "FRAME"
        frames: list[FrameDef] = []
        for i, (_, group_chans) in enumerate(groups.items()):
            idx_ch = _find_index_channel(group_chans)
            if idx_ch is None:
                # Fall back: let validate() raise the standard length mismatch
                return []
            ordered = [idx_ch] + [c for c in group_chans if c is not idx_ch]
            frames.append(FrameDef(name=f"{base_name}-{i + 1}", channels=ordered))

        logger.info(
            "  Auto-grouped %d channels into %d frames by depth sampling.",
            len(self._channels), len(frames),
        )
        return frames

    def build(self, output_path: str, *, group_by_depth: bool = True) -> str:
        """
        Validate the dataset and write a DLIS file.

        Parameters
        ----------
        output_path:
            Destination ``.dlis`` file.  Parent directories are created
            automatically if they do not exist.
        group_by_depth:
            When ``True`` (default), channels with different row counts are
            automatically placed in separate DLIS frames within the same
            output file.  Each group must contain an identifiable depth /
            index channel (matched by name or length unit); if one cannot be
            found the method falls back to single-frame validation.

            Set to ``False`` to disable auto-grouping and restore the
            pre-existing behaviour where mismatched lengths always raise
            ``ValueError``.

        Returns
        -------
        str
            Absolute path to the created DLIS file.

        Raises
        ------
        ValueError
            If no channels have been added, or channel lengths are inconsistent
            (when ``group_by_depth=False`` or auto-grouping falls back).
        """
        frames = self._detect_frames() if group_by_depth else []
        dataset = WellDataset(
            metadata=self._metadata,
            origin=self._origin,
            channels=self._channels,
            parameters=self._parameters,
            frames=frames,
        )
        return write_dlis(dataset, output_path, flatten_arrays=self._flatten)

    def to_dataset(self) -> WellDataset:
        """
        Return the assembled :class:`~dlis_builder.models.WellDataset`
        without writing to disk.

        Useful for testing, inspection, or passing to alternative writers.
        """
        return WellDataset(
            metadata=self._metadata,
            origin=self._origin,
            channels=self._channels.copy(),
            parameters=self._parameters.copy(),
        )

    # ------------------------------------------------------------------ #
    #  Convenience shortcuts  (static/class helpers)                      #
    # ------------------------------------------------------------------ #

    @staticmethod
    def scalar_channel(
        name: str,
        data,
        unit: str = "",
        long_name: str = "",
    ) -> ChannelDef:
        """Shortcut to create a scalar :class:`~dlis_builder.models.ChannelDef`."""
        import numpy as np  # noqa: PLC0415
        return ChannelDef(
            name=name,
            data=np.asarray(data, dtype=np.float64),
            unit=unit,
            long_name=long_name or name,
        )

    @staticmethod
    def image_channel(
        name: str,
        data,
        unit: str = "",
        long_name: str = "",
        resolution: Resolution = Resolution.LOW,
    ) -> ChannelDef:
        """
        Shortcut to create an image / array :class:`~dlis_builder.models.ChannelDef`.

        *data* must be 2-D with shape ``(depth_samples, elements_per_sample)``.
        """
        import numpy as np  # noqa: PLC0415
        arr = np.asarray(data, dtype=np.float64)
        if arr.ndim != 2:
            raise ValueError(
                f"image_channel expects 2-D data (samples, elements), "
                f"got shape {arr.shape}."
            )
        return ChannelDef(
            name=name,
            data=arr,
            unit=unit,
            long_name=long_name or name,
            dimension=[arr.shape[1]],
            resolution=resolution,
        )
