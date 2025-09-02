import json
from typing import Any, TypeVar, Optional, Type

T = TypeVar("T")


def dumps(msg: Any, cls: Optional[Type[T]] = None) -> str:
    """
    Serialize a message to JSON string.
    
    Args:
        msg: The message to serialize
        cls: Optional custom serializer class
        
    Returns:
        JSON string representation of the message
    """
    serializer = cls if cls and hasattr(cls, "dumps") else json
    return serializer.dumps(msg)


def loads(data: str, cls: Optional[Type[T]] = None) -> Any:
    """
    Deserialize a JSON string to Python object.
    
    Args:
        data: JSON string to deserialize
        cls: Optional custom deserializer class
        
    Returns:
        Python object from JSON string
    """
    deserializer = cls if cls and hasattr(cls, "loads") else json
    return deserializer.loads(data)