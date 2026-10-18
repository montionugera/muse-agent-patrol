"""Host handoff public boundary; no host SDK or model execution."""
from .components.bridge import HostHandoff
from .components.consumer import MuseConsumer

__all__ = ["HostHandoff", "MuseConsumer"]
