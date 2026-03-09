"""
dlis_builder.models.channel
-----------------------------
Data models for channel definitions and grouped image / array channels.
No I/O or conversion logic belongs here.
"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field

import numpy as np


class Resolution(str, enum.Enum):
    """
    Numeric precision for **array / image** channels.

    Scalar channels (depth, azimuth, GR, etc.) are always stored as
    float64 regardless of this setting.

    Attributes
    ----------
    LOW:
        ``float32`` / FSINGL — 4 bytes per element.
        Smaller files; matches Baker Hughes / Techlog native FMI/XRMI encoding.
        Adequate precision for borehole resistivity imagery.
    HIGH:
        ``float64`` / FDOUBL — 8 bytes per element.
        Full IEEE-754 double precision.  Recommended for spectral or
        acoustic data that requires wider dynamic range.
    """

    LOW = "low"
    HIGH = "high"

    @property
    def numpy_dtype(self) -> type:
        return np.float32 if self == Resolution.LOW else np.float64

    @property
    def bytes_per_element(self) -> int:
        return 4 if self == Resolution.LOW else 8


@dataclass
class ChannelDef:
    """
    Definition of a single channel (column) to be written into a DLIS frame.

    A channel can be:

    * **Scalar** — one numeric value per depth sample (GR, RHOB, ILD, DEPT, ...)
    * **Array / Image** — *N* values per depth sample (borehole image, waveform,
      spectrum, ...).  Set ``dimension=[N]`` and supply ``data`` with shape
      ``(samples, N)``.

    Parameters
    ----------
    name:
        Channel mnemonic used in the DLIS file.  Must contain only
        ``[A-Za-z0-9_]`` characters; the library will sanitise if needed.
    data:
        Numeric data array.
        * Scalar: shape ``(N,)``
        * Array/image: shape ``(N, M)``  where M = number of elements per sample.
    unit:
        Physical unit string.  The library normalises common LAS variants
        (e.g. ``"OHMM"`` → ``"ohm.m"``).  Pass ``None`` or ``""`` for
        dimensionless channels.
    long_name:
        Human-readable description (written as ``LONG-NAME`` in the DLIS
        channel object).  Defaults to ``name``.
    dimension:
        List with one element specifying the per-sample array length.
        * ``[1]`` or ``None`` → scalar channel.
        * ``[120]`` → 120 values per depth sample (image channel).
        If ``None``, inferred automatically from ``data.ndim``.
    resolution:
        Precision for **array** channels only.  Ignored for scalars.
        Defaults to :attr:`Resolution.LOW` (float32).
    properties:
        Optional RP 66 V1 property codes (e.g. ``["DEPTH"]``, ``["AZIMUTH"]``).

    Examples
    --------
    Scalar channel::

        ChannelDef(name="GR", unit="gAPI", data=gr_array)

    Image channel (120 azimuths)::

        ChannelDef(
            name="IMAGE_DYNAMIC",
            unit="ohm.m",
            data=image_array,          # shape (N, 120)
            dimension=[120],
            resolution=Resolution.LOW,
        )
    """

    name: str
    data: np.ndarray
    unit: str = ""
    long_name: str = ""
    dimension: list[int] | None = None
    resolution: Resolution = Resolution.LOW
    properties: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.long_name:
            self.long_name = self.name
        # Infer dimension from data shape if not provided
        if self.dimension is None:
            if self.data.ndim == 2:
                self.dimension = [self.data.shape[1]]
            else:
                self.dimension = [1]

    @property
    def is_array(self) -> bool:
        """True if this channel carries more than one value per depth sample."""
        return self.dimension is not None and self.dimension[0] > 1

    @property
    def is_string(self) -> bool:
        """True if this channel carries string / categorical data.

        Detected when ``data.dtype.kind`` is one of:

        * ``'O'`` — numpy object array (Python strings, ``None``, mixed)
        * ``'U'`` — numpy fixed-width unicode string
        * ``'S'`` — numpy fixed-width byte string

        The DLIS writer encodes string channels as ``int32`` category codes and
        stores the lookup table (``0=<null>,1=Label,...``) as a companion DLIS
        Parameter named ``LABELS_<channel_name>``.
        """
        return self.data.dtype.kind in ("U", "O", "S")

    @property
    def array_size(self) -> int:
        """Number of elements per depth sample (1 for scalars)."""
        return self.dimension[0] if self.dimension else 1
