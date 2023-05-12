import random
from typing import Any
from datetime import datetime, timedelta

from order_schema import OrderType
from faker.providers import BaseProvider

MINUTES_IN_HOUR = 60
MINUTES_IN_DAY = 1440


# https://github.com/GoogleCloudPlatform/public-datasets-pipelines/blob/main/datasets/thelook_ecommerce/pipelines/_images/run_thelook_kub/fake.py
def random_date(start_date: datetime) -> datetime:
    end_date = datetime.now()
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    if days_between_dates <= 1:
        days_between_dates = 2
    random_number_of_days = random.randrange(1, days_between_dates)
    created_at = (
            start_date
            + timedelta(days=random_number_of_days)
            + timedelta(minutes=random.randrange(MINUTES_IN_HOUR * 19))
    )
    return created_at


def generate_user_gender_list():
    user_nb = 10000
    users = range(1, user_nb + 1)
    genders = []
    for _ in range(user_nb):
        genders.append(random.choice(["M", "F"]))
    return list(zip(users, genders))


class OrderMicroserviceProvider(BaseProvider):
    def __init__(self, generator: Any):
        super().__init__(generator)
        self.user_gender = generate_user_gender_list()

    @staticmethod
    def status():
        return random.choices(
            population=["Complete", "Cancelled", "Returned", "Processing", "Shipped"],
            weights=[0.25, 0.15, 0.1, 0.2, 0.3]
        )[0]

    @staticmethod
    def num_of_item():
        return random.choices(
            population=[1, 2, 3, 4],
            weights=[0.7, 0.2, 0.05, 0.05]
        )[0]

    def produce_msg(self):
        order_id = random.randint(1, 10000)
        user_gender = random.choice(self.user_gender)
        status = self.status()
        created_at = random_date(datetime.now() - timedelta(days=7))
        if status == "Returned":
            shipped_at = created_at + timedelta(minutes=random.randrange(MINUTES_IN_DAY * 3))
            delivered_at = shipped_at + timedelta(minutes=random.randrange(MINUTES_IN_DAY * 5))
            returned_at = delivered_at + timedelta(minutes=random.randrange(MINUTES_IN_DAY * 3))
        elif status == "Complete":
            shipped_at = created_at + timedelta(minutes=random.randrange(MINUTES_IN_DAY * 3))
            delivered_at = shipped_at + timedelta(minutes=random.randrange(MINUTES_IN_DAY * 5))
            returned_at = None
        elif status == "Shipped":
            shipped_at = created_at + timedelta(minutes=random.randrange(MINUTES_IN_DAY * 3))
            delivered_at = None
            returned_at = None
        else:
            shipped_at = None
            delivered_at = None
            returned_at = None

        order = OrderType(
            order_id=order_id,
            user_id=user_gender[0],
            status=status,
            gender=user_gender[1],
            created_at=created_at,
            returned_at=returned_at,
            shipped_at=shipped_at,
            delivered_at=delivered_at,
            num_of_item=self.num_of_item(),
        )
        message = order.to_dict()
        # We do not use the key anyway => should use avro instead
        key = order_id
        return message, key
