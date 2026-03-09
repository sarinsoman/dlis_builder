"""dlis_builder.models — re-export public model types."""
from .channel import ChannelDef, Resolution
from .frame import FrameDef, WellDataset
from .metadata import DLISFileConfig, OriginConfig, ParameterDef, WellMetadata

__all__ = [
    "ChannelDef",
    "Resolution",
    "FrameDef",
    "WellDataset",
    "WellMetadata",
    "OriginConfig",
    "DLISFileConfig",   # user-friendly alias for OriginConfig
    "ParameterDef",
]
