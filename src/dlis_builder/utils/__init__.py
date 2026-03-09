"""dlis_builder.utils — internal utility exports."""
from .naming import deduplicate_names, sanitize_channel_name, to_ascii
from .units import UNIT_MAP, get_index_type, normalize_unit
from .validation import (
    DLISMetadataWarning,
    validate_channel_names,
    validate_csv_config,
    validate_dataset,
    validate_metadata,
)

__all__ = [
    "normalize_unit",
    "get_index_type",
    "UNIT_MAP",
    "to_ascii",
    "sanitize_channel_name",
    "deduplicate_names",
    # Validation
    "DLISMetadataWarning",
    "validate_metadata",
    "validate_dataset",
    "validate_channel_names",
    "validate_csv_config",
]
