from product_schema import ProductType

from faker_datasets import Provider, add_dataset, with_datasets


@add_dataset("products", "products.json")
class ProductMicroserviceProvider(Provider):
    @with_datasets("products")
    def product(self, products):
        product = self.__pick__(products)
        avro_product = ProductType(
            id=int(product["id"]),
            cost=float(product["cost"]),
            category=product["category"],
            name=product["name"],
            brand=product["brand"],
            retail_price=float(product["retail_price"]),
            department=product["department"],
            sku=product["sku"],
            distribution_center_id=int(product["distribution_center_id"]),
        )
        return avro_product.to_dict()
