"""
dlis_builder.contracts.converter
---------------------------------
Abstract base class (ABC) for all file-format converters.

Converters read an external source (LAS, CSV, JSON, …) and produce a
:class:`~dlis_builder.models.WellDataset`.  They also provide a one-shot
``convert()`` method that writes the DLIS file directly.

Design rationale
----------------
* ``Protocol`` (structural typing) is used for **data sources** because
  callers supply them — they cannot be forced to inherit.
* ``ABC`` is used for **converters** because they are *provided* by this
  library; explicit inheritance makes the contract visible in IDEs and docs.

Subclassing
-----------
::

    from dlis_builder.contracts import DLISConverter
    from dlis_builder.models import WellDataset, WellMetadata, OriginConfig

    class MyFormatConverter(DLISConverter):

        def read(self, source: str) -> WellDataset:
            # parse source, build WellDataset, return it
            ...

        def convert(
            self,
            source: str,
            output_path: str,
            *,
            metadata: WellMetadata | None = None,
            origin: OriginConfig | None = None,
            flatten_arrays: bool = False,
        ) -> str:
            ds = self.read(source)
            if metadata:
                ds.metadata = metadata
            if origin:
                ds.origin = origin
            return self._write(ds, output_path, flatten_arrays=flatten_arrays)
"""
from __future__ import annotations

import abc
from typing import Any

from dlis_builder.models.frame import WellDataset
from dlis_builder.models.metadata import OriginConfig, WellMetadata


class DLISConverter(abc.ABC):
    """
    Abstract base class for all file-format → DLIS converters.

    Subclasses must implement:

    * :meth:`read` — parse the source and return a :class:`WellDataset`.
    * :meth:`convert` — end-to-end: read source, write DLIS, return output path.

    The protected :meth:`_write` helper delegates to the library's DLIS
    writer engine so subclasses do not need to re-implement writing.
    """

    # ------------------------------------------------------------------ #
    #  Abstract interface                                                  #
    # ------------------------------------------------------------------ #

    @abc.abstractmethod
    def read(self, source: Any) -> WellDataset:
        """
        Parse *source* and return a :class:`WellDataset`.

        Parameters
        ----------
        source:
            Path (str / Path), file-like object, or any source-specific
            descriptor understood by the converter.

        Returns
        -------
        WellDataset
            The fully populated dataset ready for DLIS writing.
        """

    @abc.abstractmethod
    def convert(
        self,
        source: Any,
        output_path: str,
        *,
        metadata: WellMetadata | None = None,
        origin: OriginConfig | None = None,
        flatten_arrays: bool = False,
    ) -> str:
        """
        Convert *source* to a DLIS file at *output_path*.

        Parameters
        ----------
        source:
            Input data (path, file-object, or source-specific descriptor).
        output_path:
            Destination ``.dlis`` file path.
        metadata:
            Override the well metadata extracted from the source.
        origin:
            Override the DLIS origin / producer configuration.
        flatten_arrays:
            If ``True``, explode array channels into individual scalar channels
            so all per-sample elements get their own mnemonic
            (``IMAGE_DYNAMIC_001 … IMAGE_DYNAMIC_120``).

        Returns
        -------
        str
            Absolute path to the created DLIS file.
        """

    # ------------------------------------------------------------------ #
    #  Protected helper — delegates to the writer engine                  #
    # ------------------------------------------------------------------ #

    def _write(
        self,
        dataset: WellDataset,
        output_path: str,
        *,
        flatten_arrays: bool = False,
    ) -> str:
        """Write *dataset* to *output_path* and return the absolute path."""
        # Import here to keep the contract module dependency-free
        from dlis_builder._writer import write_dlis  # noqa: PLC0415
        return write_dlis(dataset, output_path, flatten_arrays=flatten_arrays)
