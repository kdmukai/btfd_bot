#!/usr/bin/env python

import argparse
import boto3
import configparser
import datetime
import dateutil
import decimal
import json
import math
import pytz
import sys
import time

import cbpro

from decimal import Decimal
from models import Order
from utils import convert_datetime_str, convert_epoch_to_utc



def get_timestamp():
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


"""
    This is meant to be run as a crontab to make regular updates to the trailing buy/sell orders.
"""
parser = argparse.ArgumentParser(
    description="""
        This is a basic Coinbase Pro BUY THE FUCKING DIP bot (or can also sell the pump).

        ex:
            BTC-USD BUY 14 USD -10.0         (buy $14 worth of BTC @ -10% dip from recent high)
            BTC-USD BUY 0.00125 BTC -10.0    (buy 0.00125 BTC @ -10% dip from recent high)
            ETH-BTC SELL 0.00125 BTC 5.5     (sell 0.00125 BTC worth of ETH @ +5.5% pump from recent low)
            ETH-BTC SELL 0.1 ETH 5.5         (sell 0.1 ETH @ +5.5% pump from recent low)

        Includes optional 200MA (15min close) limit mode where it'll only set trailing buys if the
        price has dipped below the 200MA (or above for selling the pump).
    """,
    formatter_class=argparse.RawTextHelpFormatter
)

# Required positional arguments
parser.add_argument('market_name', help="(e.g. BTC-USD, ETH-BTC, etc)")

parser.add_argument('order_side',
                    type=str,
                    choices=["BUY", "SELL"])

parser.add_argument('amount',
                    type=Decimal,
                    help="The quantity to buy or sell in the amount_currency")

parser.add_argument('amount_currency',
                    help="The currency the amount is denominated in")

parser.add_argument('percent_diff',
                    type=Decimal,
                    help="The percentage above or below recent high (e.g. '-10.0' = 10% below")

# Additional options
parser.add_argument('-sandbox',
                    action="store_true",
                    default=False,
                    dest="sandbox_mode",
                    help="Run against sandbox, skips user confirmation prompt")

parser.add_argument('-m', '--ma_limit',
                    default=False,
                    action="store_true",
                    dest="use_ma_limit",
                    help="Enable optional 200MA (15min) limit")

parser.add_argument('-j', '--job',
                    action="store_true",
                    default=False,
                    dest="job_mode",
                    help="Suppresses user confirmation prompt")

parser.add_argument('-c', '--config',
                    default="settings.conf",
                    dest="config_file",
                    help="Override default config file location")



if __name__ == "__main__":
    args = parser.parse_args()
    print("%s: STARTED: %s" % (get_timestamp(), args))

    market_name = args.market_name
    order_side = args.order_side.lower()
    amount = args.amount
    amount_currency = args.amount_currency
    percent_diff = args.percent_diff

    sandbox_mode = args.sandbox_mode
    use_ma_limit = args.use_ma_limit
    job_mode = args.job_mode

    if not sandbox_mode and not job_mode:
        if sys.version_info[0] < 3:
            # python2.x compatibility
            response = raw_input("Production purchase! Confirm [Y]: ")  # noqa: F821
        else:
            response = input("Production purchase! Confirm [Y]: ")
        if response != 'Y':
            print("Exiting without submitting purchase.")
            exit()

    # Read settings
    config = configparser.ConfigParser()
    config.read(args.config_file)

    config_section = 'production'
    if sandbox_mode:
        config_section = 'sandbox'
    key = config.get(config_section, 'API_KEY')
    passphrase = config.get(config_section, 'PASSPHRASE')
    secret = config.get(config_section, 'SECRET_KEY')
    aws_access_key_id = config.get(config_section, 'AWS_ACCESS_KEY_ID')
    aws_secret_access_key = config.get(config_section, 'AWS_SECRET_ACCESS_KEY')
    sns_topic = config.get(config_section, 'SNS_TOPIC')

    # Instantiate public and auth API clients
    if not args.sandbox_mode:
        auth_client = cbpro.AuthenticatedClient(key, secret, passphrase)
    else:
        # Use the sandbox API (requires a different set of API access credentials)
        auth_client = cbpro.AuthenticatedClient(
            key,
            secret,
            passphrase,
            api_url="https://api-public.sandbox.pro.coinbase.com")

    public_client = cbpro.PublicClient()

    # Retrieve dict list of all trading pairs
    products = public_client.get_products()
    base_min_size = None
    base_increment = None
    quote_increment = None
    for product in products:
        if product.get("id") == market_name:
            base_currency = product.get("base_currency")
            quote_currency = product.get("quote_currency")
            base_min_size = Decimal(product.get("base_min_size")).normalize()
            base_increment = Decimal(product.get("base_increment")).normalize()
            quote_increment = Decimal(product.get("quote_increment")).normalize()
            if amount_currency == product.get("quote_currency"):
                amount_currency_is_quote_currency = True
            elif amount_currency == product.get("base_currency"):
                amount_currency_is_quote_currency = False
            else:
                raise Exception("amount_currency %s not in market %s" % (amount_currency,
                                                                         market_name))
            # print(json.dumps(product, indent=2))

    # print("base_min_size: %s" % base_min_size)
    # print("quote_increment: %s" % quote_increment)

    # Prep boto SNS client for email notifications
    sns = boto3.client(
        "sns",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name="us-east-1"     # N. Virginia
    )

    # Get current balances
    """
        {
            'id': '********-****-****-****-************',
            'currency': 'BTC',
            'balance': '0.0323577949134040',
            'hold': '0.0000000000000000',
            'available': '0.032357794913404',
            'profile_id': '********-****-****-****-************',
            'trading_enabled': True
        }
    """
    base_currency_balance = None
    quote_currency_balance = None
    for account in auth_client.get_accounts():
        if account.get("currency") == base_currency:
            base_currency_balance = Decimal(account.get("balance")).quantize(base_increment)
            base_currency_hold = Decimal(account.get("hold")).quantize(base_increment)
        elif account.get("currency") == quote_currency:
            quote_currency_balance = Decimal(account.get("balance")).quantize(quote_increment)
            quote_currency_hold = Decimal(account.get("hold")).quantize(quote_increment)
        if base_currency_balance and quote_currency_balance:
            break
    print(f"--------------------------------------------------------")
    print(f"\t base_currency_balance:  {base_currency_balance} {base_currency}")
    print(f"\t base_currency_hold:     {base_currency_hold} {base_currency}")
    print(f"\t quote_currency_balance: {quote_currency_balance} {quote_currency}")
    print(f"\t quote_currency_hold:    {quote_currency_hold} {quote_currency}")
    print(f"--------------------------------------------------------")

    # Get the current btfd order
    date_last_updated = None
    order = Order.filter(
        market_name=market_name,
        status__in=[Order.STATUS__OPEN, Order.STATUS__PENDING],
        side=order_side
    ).order_by(Order.created.desc()).first()

    if not order:
        print("No open order. Creating a new one")
        order = Order()

    else:
        print(f"Retrieved order {order.id}: {order.order_id}")

        # Update the order's status
        order_json = auth_client.get_order(order.order_id)
        if not order_json:
            raise Exception(f"Could not retrieve order {order.order_id}")

        if order_json.get("message") == "NotFound":
            # Order was probably manually cancelled
            order.status = Order.STATUS__CANCELLED
            order.save()

            order = Order()

        else:
            date_last_updated = order.created
            order.update_from_json(order_json, percent_diff)

            if order.status not in [Order.STATUS__OPEN, Order.STATUS__PENDING]:
                # Order status is no longer pending!
                print(json.dumps(order_json, indent=2))
                if order.status == Order.STATUS__DONE:
                    if percent_diff < 0:
                        subject = "Bought the dip!"
                    else:
                        subject = "Sold the pump!"
                    date_last_updated = order.updated
                else:
                    subject = "ERROR:"

                sns.publish(
                    TopicArn=sns_topic,
                    Subject=f"{subject} {market_name} {order_side} order of {amount} {amount_currency} {order.status} @ {order.target_price} {quote_currency}",
                    Message=json.dumps(order_json, sort_keys=True, indent=4)
                )

                print("%s: DONE: %s %s order of %s %s %s @ %s %s" % (
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    market_name,
                    order_side,
                    amount,
                    amount_currency,
                    order.status,
                    order.target_price,
                    quote_currency))

                # Now we'll need to prep a new order, starting from the previous one's fill date.
                order = Order()

    if not date_last_updated:
        # We're creating a new order, but try to pick up where the last one left off.
        prev_order = Order.filter(
            market_name=market_name,
            status=Order.STATUS__DONE,
            side=order_side
        ).order_by(Order.created.desc()).first()
        if prev_order:
            print(f"Using order {prev_order.id}'s created: {prev_order.created}")
            date_last_updated = prev_order.created

    if date_last_updated:
        print(f"date_last_updated: {date_last_updated} ({int(pytz.utc.localize(date_last_updated).timestamp())})")

    """
        Grab the last 300 15-min candles to calculate the 200MA on the 15-min close
        [ time, low, high, open, close, volume ]
    """
    CANDLE_TIME = 0
    CANDLE_LOW = 1
    CANDLE_HIGH = 2
    CANDLE_OPEN = 3
    CANDLE_CLOSE = 4
    CANDLE_VOLUME = 5
    market_data = public_client.get_product_historic_rates(
        market_name,
        granularity=60*15    # 15-minute candles
    )

    ma_limit = None
    if use_ma_limit:
        # Calculate the 200 MA for the 15min candles, find the most recent dip below it (or spike above)
        for index, candle in enumerate(market_data):
            if len(market_data) - index < 200:
                break
            cur_200ma = Decimal(sum([c[CANDLE_CLOSE] for c in market_data[index:index + 200]]) / 200.0).quantize(quote_increment)
            print(f"{convert_epoch_to_utc(candle[CANDLE_TIME])}: {cur_200ma}")
            if (percent_diff < 0 and candle[CANDLE_HIGH] > cur_200ma) or (percent_diff > 0 and candle[CANDLE_LOW] < cur_200ma):
                # Current candle is above the 200MA; this is our hard limit
                ma_limit = cur_200ma
                break

        if ma_limit:
            print(f"ma_limit set at {ma_limit} from {convert_epoch_to_utc(candle[CANDLE_TIME])} (UTC)")
        else:
            print(f"The 200MA was not breached through {convert_epoch_to_utc(market_data[200][CANDLE_TIME])}")

    """
        We'll retrieve the most recent 300 1-minute candles so this must be run at least
        every 5hrs (more likely we'll run this every 5min).

        [ time, low, high, open, close, volume ]
    """
    market_data = public_client.get_product_historic_rates(
        market_name,
        granularity=60    # minute candles
    )

    # Scan for new recent_extreme from...
    if date_last_updated:
        # ...last check
        #   (dates from the db have no TZ; must force to utc to avoid local TZ shift assumptions)
        recent_extreme_from_date = int(pytz.utc.localize(date_last_updated).timestamp())
    else:
        # ...or across the whole range of market_data
        recent_extreme_from_date = market_data[-1][CANDLE_TIME]

    print(f"recent_extreme_from_date: {convert_epoch_to_utc(recent_extreme_from_date)} (UTC)")

    # Use the 24hr stats for the current price
    """
        {
            "open": "6745.61000000", 
            "high": "7292.11000000", 
            "low": "6650.00000000", 
            "volume": "26185.51325269", 
            "last": "6813.19000000", 
            "volume_30day": "1019451.11188405"
        }
    """
    stats = public_client.get_product_24hr_stats(market_name)
    current_price = Decimal(stats.get("last")).quantize(quote_increment)

    if market_data[0][CANDLE_TIME] < recent_extreme_from_date:
        # The last order was just recently completed within the ~5min lag time of the market_data candles.
        #   Use the current_price as a good-enough stand in for our new recent_extreme.
        recent_extreme = current_price
        print("Last order just closed; have to use current price for recent_extreme")
    else:
        if percent_diff < 0:
            # BUY THE F'N DIP! Identify recent high
            recent_extreme = Decimal(max([d[CANDLE_HIGH] for d in market_data if d[CANDLE_TIME] > recent_extreme_from_date])).quantize(quote_increment)
            if current_price > recent_extreme:
                # Price has moved further in the ~5min lag time
                recent_extreme = current_price
        else:
            recent_extreme = Decimal(min([d[CANDLE_LOW] for d in market_data if d[CANDLE_TIME] > recent_extreme_from_date])).quantize(quote_increment)
            if current_price < recent_extreme:
                # Price has moved further in the ~5min lag time
                recent_extreme = current_price

    if use_ma_limit:
        if (percent_diff < 0 and recent_extreme > ma_limit) or (percent_diff > 0 and recent_extreme < ma_limit):
            # Constrain the recent_extreme by the ma_limit
            print(f"Enforcing MA limit: {recent_extreme} capped at {ma_limit}")
            recent_extreme = ma_limit

    target_price = (recent_extreme*(Decimal('100.0') + percent_diff)/Decimal('100.0')).quantize(quote_increment)
    print(f"--------------------------------------------------------")
    print(f"\t recent_extreme: {recent_extreme} {quote_currency}")
    print(f"\t target_price:   {target_price} {quote_currency} ({percent_diff}%)")
    print(f"\t current_price:  {current_price} {quote_currency} ({(current_price/recent_extreme*Decimal('100')).quantize(Decimal('0.1')) - Decimal('100.')}%)")
    if order.order_id:
        print(f"\t current_order:  {order.target_price.quantize(quote_increment)} {quote_currency}")
    print(f"--------------------------------------------------------")

    # If we're buying the dip, follow the target_price up as needed; if we're selling the
    #   pump, follow the target_price down.
    if order.order_id and ((percent_diff < 0 and target_price <= order.target_price) or (percent_diff > 0 and target_price >= order.target_price)):
        # The current order's btfd order was set from a higher prior peak, thus a higher
        #   target_price than what we found here. Therefore we let the existing order
        #   ride and keep waiting for it.
        # Or vice versa for a sell the pump.
        print("No order changes required")
    else:
        if order.order_id:
            # Cancel the current order and post a new one at the new price
            print(f"Cancelling order {order.order_id}")
            result = auth_client.cancel_order(order.order_id)
            print(result)

        if amount_currency_is_quote_currency:
            # Convert 'amount' of the quote_currency to equivalent in base_currency
            base_currency_amount = (amount / target_price).quantize(base_increment)
        else:
            # Already in base_currency
            base_currency_amount = amount.quantize(base_increment)

        print("base_currency_amount: %s %s" % (base_currency_amount, base_currency))

        """
            {
                "id": "d0c5340b-6d6c-49d9-b567-48c4bfca13d2",
                "price": "0.10000000",
                "size": "0.01000000",
                "product_id": "BTC-USD",
                "side": "buy",
                "stp": "dc",
                "type": "limit",
                "time_in_force": "GTC",
                "post_only": false,
                "created_at": "2016-12-08T20:02:28.53864Z",
                "fill_fees": "0.0000000000000000",
                "filled_size": "0.00000000",
                "executed_value": "0.0000000000000000",
                "status": "pending",
                "settled": false
            }
        """
        print(f"--------------------------------------------------------")
        print(f"Placing limit order:")
        print(f"\t market: {market_name}")
        print(f"\t side:   {order_side}")
        print(f"\t price:  {target_price} {quote_currency}")
        print(f"\t amount: {base_currency_amount} {base_currency}")
        print(f"\t value:  {(base_currency_amount * target_price).quantize(quote_increment)} {quote_currency}")
        print(f"--------------------------------------------------------")
        result = auth_client.place_limit_order(
            product_id=market_name,
            side=order_side,
            price=float(target_price),          # price in quote_currency
            size=float(base_currency_amount)    # quantity of base_currency to buy   
        )

        print(json.dumps(result, sort_keys=True, indent=4))

        if "message" in result and "Post only mode" in result.get("message"):
            # Price moved away from valid order
            print("Post only mode at %f %s" % (offer_price, quote_currency))

        elif "message" in result:
            # Something went wrong if there's a 'message' field in response
            sns.publish(
                TopicArn=sns_topic,
                Subject="Could not place %s %s order for %s %s" % (market_name,
                                                                   order_side,
                                                                   amount,
                                                                   amount_currency),
                Message=json.dumps(result, sort_keys=True, indent=4)
            )
            exit()

        if result and "status" in result and result["status"] == Order.STATUS__REJECTED:
            # Rejected - usually because price was above lowest sell offer. Try
            #   again in the next loop.
            print("%s: %s Order rejected @ %f %s" % (get_timestamp(),
                                                     market_name,
                                                     current_price,
                                                     quote_currency))

        order.update_from_json(result, percent_diff=percent_diff)

