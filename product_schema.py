import dataclasses

from dataclasses_avroschema import AvroModel


@dataclasses.dataclass
class ProductType(AvroModel):
    """Product"""
    id: int
    cost: float
    category: str
    name: str
    brand: str
    retail_price: float
    department: str
    sku: str
    distribution_center_id: int
