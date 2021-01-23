import datetime
import dateutil
import utils

from decimal import Decimal
from peewee import *
from playhouse.sqlite_ext import JSONField



DATABASE = 'data.db'

# Create a database instance that will manage the connection and
# execute queries
db = SqliteDatabase(DATABASE)

def create_tables():
    with db:
        db.create_tables([Order])


# Create a base-class all our models will inherit, which defines
# the database we'll be using.
class BaseModel(Model):
    class Meta:
        database = db


class Order(BaseModel):
    """
        Orders placed and managed by btfd_bot
    """
    STATUS__OPEN = 'open'
    STATUS__PENDING = 'pending'
    STATUS__DONE = 'done'
    STATUS__CANCELLED = 'cancelled'
    STATUS__REJECTED = 'rejected'

    order_id = CharField(unique=True)
    percent_diff = DecimalField(null=True)
    target_price = DecimalField()
    size = DecimalField()
    market_name = CharField()
    side = CharField()
    status = CharField()    # open, pending, active, done
    done_reason = CharField(null=True)
    created = DateTimeField()
    updated = DateTimeField(null=True)
    raw_data = JSONField()


    def update_from_json(self, raw_json, percent_diff=None):
        """
            {
                'id': '33295403-e044-470c-93e3-49a554841142',
                'price': '6453.42000000',
                'size': '0.00172157',
                'product_id': 'BTC-USD',
                'profile_id': '4cc0a9c0-9277-476f-951b-630899740a88',
                'side': 'buy',
                'type': 'limit',
                'time_in_force': 'GTC',
                'post_only': True,
                'created_at': '2020-03-31T17:19:03.162429Z',
                'done_at': '2020-03-31T17:21:43.922Z',
                'done_reason': 'filled',
                'fill_fees': '0.0555500713470000',
                'filled_size': '0.00172157',
                'executed_value': '11.1100142694000000',
                'status': 'done',
                'settled': True
            }
        """
        try:
            order = self
            order.order_id = raw_json.get("id")
            order.percent_diff = percent_diff
            order.target_price = Decimal(raw_json.get("price"))
            order.size = Decimal(raw_json.get("size"))
            order.market_name = raw_json.get("product_id")
            order.side = raw_json.get("side")
            order.status = raw_json.get("status")
            order.created = utils.convert_datetime_str(raw_json.get("created_at"))
            order.raw_data = raw_json
            if raw_json.get("done_at"):
                order.updated = utils.convert_datetime_str(raw_json.get("done_at"))
                order.done_reason = raw_json.get("done_reason")
            order.save()
        except Exception as e:
            print(raw_json)
            raise e


    @classmethod
    def create_order_from_json(cls, raw_json, percent_diff=None):
        order = Order()
        order.update_from_json(raw_json, percent_diff)
        return order



