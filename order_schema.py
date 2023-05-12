import dataclasses
from datetime import datetime
from typing import Optional

from dataclasses_avroschema import AvroModel


@dataclasses.dataclass
class OrderType(AvroModel):
    """Order"""
    order_id: int
    user_id: int
    status: str
    gender: str
    num_of_item: int
    created_at: datetime
    returned_at: Optional[datetime] = None
    shipped_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
