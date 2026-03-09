"""
dlis_builder.models.frame
---------------------------
``WellDataset`` is the canonical intermediate representation of all data
that will be written into a DLIS file.  Every converter and the programmatic
builder produces a ``WellDataset``; the DLIS writer engine consumes one.

This clean boundary means:
  converter / builder  →  WellDataset  →  DLIS writer
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .channel import ChannelDef
from .metadata import OriginConfig, ParameterDef, WellMetadata


@dataclass
class FrameDef:
    """
    A group of channels sharing the same depth axis — one DLIS Frame.

    When a :class:`WellDataset` has its ``frames`` field populated, the writer
    uses the ``FrameDef`` list instead of the flat ``channels`` list.  Each
    ``FrameDef`` becomes a separate DLIS Frame in the same logical file,
    enabling channels with different depth samplings to coexist in one file.

    Attributes
    ----------
    name:
        Frame mnemonic written to the DLIS file (e.g. ``"FRAME-1"``).
    channels:
        Channels in this frame.  The *first* element must be the index
        (depth / time) channel.
    """

    name: str = "FRAME"
    channels: list[ChannelDef] = field(default_factory=list)


@dataclass
class WellDataset:
    """
    The complete, source-agnostic representation of a well log dataset.

    A ``WellDataset`` holds everything required to produce a DLIS file:
    well metadata, file-level origin/config, per-channel data, and
    acquisition parameters.  It is the single data-transfer object (DTO)
    passed between the input layer (converters / builder) and the output
    layer (DLIS writer).

    Attributes
    ----------
    metadata:
        Well identification (name, company, field, location, null value).
    origin:
        DLIS file-level origin and producer settings.
    channels:
        Ordered list of channel definitions including their data arrays.
        The *first* element must be the index channel (depth or time).
    parameters:
        Optional list of acquisition / environmental parameters to embed
        in the DLIS file (temperature, bit size, mud weight, etc.).

    Notes
    -----
    Channel ordering matters in DLIS.  The underlying writer engine
    reorders channels before writing as:

        ``[index] → [scalar channels] → [array / image channels]``

    This matches the expectation of Techlog, Petrel, and other viewers.

    Examples
    --------
    ::

        import numpy as np
        from dlis_builder.models import WellDataset, WellMetadata, ChannelDef

        depth = np.linspace(300.0, 600.0, 2001)
        ds = WellDataset(
            metadata=WellMetadata(well_name="WELL_A", company="Acme"),
            channels=[
                ChannelDef(name="DEPT", unit="m",    data=depth),
                ChannelDef(name="GR",   unit="gAPI", data=gr_data),
                ChannelDef(name="RHOB", unit="g/cm3", data=rhob_data),
            ],
        )
    """

    metadata: WellMetadata = field(default_factory=WellMetadata)
    origin: OriginConfig = field(default_factory=OriginConfig)
    channels: list[ChannelDef] = field(default_factory=list)
    parameters: list[ParameterDef] = field(default_factory=list)
    frames: list[FrameDef] = field(default_factory=list)

    # ------------------------------------------------------------------ #
    #  Convenience accessors                                               #
    # ------------------------------------------------------------------ #

    @property
    def index_channel(self) -> ChannelDef | None:
        """The first channel (depth / time index), or None if empty."""
        return self.channels[0] if self.channels else None

    @property
    def scalar_channels(self) -> list[ChannelDef]:
        """All non-array channels excluding the index channel."""
        return [c for c in self.channels[1:] if not c.is_array]

    @property
    def array_channels(self) -> list[ChannelDef]:
        """All multi-element (image / array) channels."""
        return [c for c in self.channels if c.is_array]

    def get_channel(self, name: str) -> ChannelDef | None:
        """Return the first channel whose name matches (case-insensitive)."""
        name_upper = name.upper()
        return next(
            (c for c in self.channels if c.name.upper() == name_upper),
            None,
        )

    def validate(self) -> None:
        """
        Raise ``ValueError`` if the dataset is not in a writable state.

        When ``frames`` is non-empty, each frame is validated independently.
        When ``frames`` is empty, the legacy single-frame path validates
        ``channels`` as before (fully backward compatible).

        Checks (per frame):
        * At least one channel defined.
        * All channels have the same number of depth samples.
        * Array channels have data shape (N, M) consistent with dimension.
        """
        if self.frames:
            for frame in self.frames:
                if not frame.channels:
                    raise ValueError(f"Frame '{frame.name}' has no channels.")
                n_samples = len(frame.channels[0].data)
                for ch in frame.channels:
                    if len(ch.data) != n_samples:
                        raise ValueError(
                            f"Frame '{frame.name}': channel '{ch.name}' has "
                            f"{len(ch.data)} samples but index channel has "
                            f"{n_samples}. All channels in a frame must share "
                            f"the same number of depth samples."
                        )
                    if ch.is_array:
                        if ch.data.ndim != 2:
                            raise ValueError(
                                f"Array channel '{ch.name}' in frame '{frame.name}' "
                                f"must have 2-D data of shape (samples, {ch.array_size}), "
                                f"got shape {ch.data.shape}."
                            )
                        if ch.data.shape[1] != ch.array_size:
                            raise ValueError(
                                f"Array channel '{ch.name}' in frame '{frame.name}' "
                                f"has dimension={ch.dimension} but "
                                f"data.shape[1]={ch.data.shape[1]}."
                            )
            return

        # ── Legacy single-frame path ──────────────────────────────────────
        if not self.channels:
            raise ValueError("WellDataset has no channels; cannot write DLIS.")

        n_samples = len(self.channels[0].data)
        for ch in self.channels:
            if len(ch.data) != n_samples:
                raise ValueError(
                    f"Channel '{ch.name}' has {len(ch.data)} samples but "
                    f"index channel has {n_samples}. All channels must share "
                    f"the same number of depth samples."
                )
            if ch.is_array:
                if ch.data.ndim != 2:
                    raise ValueError(
                        f"Array channel '{ch.name}' must have 2-D data of shape "
                        f"(samples, {ch.array_size}), got shape {ch.data.shape}."
                    )
                if ch.data.shape[1] != ch.array_size:
                    raise ValueError(
                        f"Array channel '{ch.name}' has dimension={ch.dimension} "
                        f"but data.shape[1]={ch.data.shape[1]}."
                    )
