import json
import sys
import time
import signal

from kafka_schema_registry import prepare_producer
from faker import Faker

from product_schema import ProductType
from order_schema import OrderType
from product_microservice import ProductMicroserviceProvider
from order_microservice import OrderMicroserviceProvider

KAFKA_ADDR = "kafka-data-pipeline.aivencloud.com:14209"
KAFKA_SCHEMA_REG = "https://avnadmin:AVNS_FvRqs_d98d5ulr_E-1j@kafka-data-pipeline.aivencloud.com:14212"

KAFKA_SSL_CA = "ca.pem"
KAFKA_SSL_CERT = "service.cert"
KAFKA_SSL_KEY = "service.key"

PRODUCTS_TOPIC = "products"
PRODUCT_SCHEMA = ProductType.avro_schema_to_python()
ORDERS_TOPIC = "orders"
ORDER_SCHEMA = OrderType.avro_schema_to_python()

fake_product = Faker()
Faker.seed(0)
fake_product.add_provider(ProductMicroserviceProvider)
fake_order = Faker()
Faker.seed(0)
fake_order.add_provider(OrderMicroserviceProvider)


def produce_msgs():
    print("Creating producer with value schema:")
    print(json.dumps(PRODUCT_SCHEMA, indent=4))
    product_producer = prepare_producer(
        [KAFKA_ADDR],
        KAFKA_SCHEMA_REG,
        PRODUCTS_TOPIC,
        20,
        3,
        value_schema=PRODUCT_SCHEMA,
        security_protocol='SSL',
        ssl_cafile=KAFKA_SSL_CA,
        ssl_certfile=KAFKA_SSL_CERT,
        ssl_keyfile='service.key'
    )

    print("Creating producer with value schema:")
    print(json.dumps(ORDER_SCHEMA, indent=4))
    order_producer = prepare_producer(
        [KAFKA_ADDR],
        KAFKA_SCHEMA_REG,
        ORDERS_TOPIC,
        20,
        3,
        value_schema=ORDER_SCHEMA,
        security_protocol='SSL',
        ssl_cafile=KAFKA_SSL_CA,
        ssl_certfile=KAFKA_SSL_CERT,
        ssl_keyfile=KAFKA_SSL_KEY
    )

    print("Connected")

    counter = 0
    while True:
        # Send in batches and then flush and pause
        for _ in range(20):
            counter = counter + 1
            product = fake_product.product()
            # No avro key for this implementation
            product_producer.send(PRODUCTS_TOPIC, value=product)

        product_producer.flush()

        for _ in range(20):
            counter = counter + 1
            order, key = fake_order.produce_msg()
            # No avro key for this implementation
            order_producer.send(ORDERS_TOPIC, value=order)

        order_producer.flush()

        print(f"Sent {counter} messages so far")
        time.sleep(5)


def main():
    signal.signal(signal.SIGINT, lambda x, y: sys.exit(0))
    produce_msgs()


if __name__ == '__main__':
    main()
