#!/usr/bin/env python

import argparse
import boto3
import configparser
import datetime
import dateutil
import decimal
import json
import math
import sys
import time

import cbpro

from decimal import Decimal
from models import Order, create_order_from_json, update_order_from_json



def get_timestamp():
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


"""
    Basic Coinbase Pro DCA buy/sell bot that pulls the current market price, subtracts a
        small spread to generate a valid price (see note below), then submits the trade as
        a limit order.

    This is meant to be run as a crontab to make regular buys/sells on a set schedule.
"""
parser = argparse.ArgumentParser(
    description="""
        This is a basic Coinbase Pro BUY THE FUCKING DIP bot (or optional sell-the-pump).

        ex:
            BTC-USD BUY 14 USD -10.0         (buy $14 worth of BTC @ -10% dip from recent high)
            BTC-USD BUY 0.00125 BTC -10.0    (buy 0.00125 BTC @ -10% dip from recent high)
            ETH-BTC SELL 0.00125 BTC 5.5     (sell 0.00125 BTC worth of ETH @ +5.5% pump from recent low)
            ETH-BTC SELL 0.1 ETH 5.5         (sell 0.1 ETH @ +5.5% pump from recent low)
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

parser.add_argument('-warn_after',
                    default=3600,
                    action="store",
                    type=int,
                    dest="warn_after",
                    help="secs to wait before sending an alert that an order isn't done")

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
    job_mode = args.job_mode
    warn_after = args.warn_after

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
            print(product)

    print("base_min_size: %s" % base_min_size)
    print("quote_increment: %s" % quote_increment)

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
    print(f" base_currency_balance: {base_currency_balance} {base_currency}")
    print(f"    base_currency_hold: {base_currency_hold} {base_currency}")
    print(f"quote_currency_balance: {quote_currency_balance} {quote_currency}")
    print(f"   quote_currency_hold: {quote_currency_hold} {quote_currency}")

    # Get the current btfd order
    try:
        order = Order.filter(
            market_name=market_name,
            status__in=["open", "pending"],
            side=order_side
        ).order_by(Order.created.desc()).first()

        if not order:
            order = Order()
        else:
            print(f"Retrieved order {order.id}: {order.order_id}")

            # Update the order's status
            order_json = auth_client.get_order(order.order_id)
            if not order_json:
                raise Exception(f"Could not retrieve order {order.order_id}")

            update_order_from_json(order, order_json, percent_diff)

            if order.status not in ["open", "pending"]:
                # Order status is no longer pending!
                sns.publish(
                    TopicArn=sns_topic,
                    Subject="%s %s order of %s %s %s @ %s %s" % (
                        market_name,
                        order_side,
                        amount,
                        amount_currency,
                        order.status,
                        order.target_price,
                        quote_currency
                    ),
                    Message=json.dumps(order, sort_keys=True, indent=4)
                )

                print("%s: DONE: %s %s order of %s %s %s @ %s %s" % (
                    get_timestamp(),
                    market_name,
                    order_side,
                    amount,
                    amount_currency,
                    order.status,
                    order.target_price,
                    quote_currency))

                # Now we'll need to prep a new order, starting from the previous one's fill date.
                order = Order()

    except Exception as e:
        # Shouldn't be possible. Once an order is filled we should have placed a new one.
        print(e)
        raise e

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
    if order.updated:
        # ...last check
        recent_extreme_from_date = int(order.updated.timestamp())
    else:
        # ...or across the whole range of market_data
        recent_extreme_from_date = market_data[-1][0]

    print(f"recent_extreme_from_date: {recent_extreme_from_date}")

    if percent_diff < 0:
        # BUY THE F'N DIP! Identify recent high
        recent_extreme = Decimal(max([d[2] for d in market_data if d[0] > recent_extreme_from_date])).quantize(quote_increment)
    else:
        recent_extreme = Decimal(min([d[1] for d in market_data if d[0] > recent_extreme_from_date])).quantize(quote_increment)

    # Assume the first candle retrieved is close enough to the current price
    current_price = Decimal(market_data[0][4]).quantize(quote_increment)

    target_price = (recent_extreme*(Decimal('100.0') + percent_diff)/Decimal('100.0')).quantize(quote_increment)
    print(f"recent_extreme: {recent_extreme} {quote_currency}")
    print(f"target_price:   {target_price} {quote_currency} ({percent_diff}%)")
    print(f"current_price:  {current_price} {quote_currency} ({(current_price/recent_extreme*Decimal('100')).quantize(Decimal('0.1')) - Decimal('100.')}%)")
    if order.order_id:
        print(f"current_order:  {order.target_price.quantize(quote_increment)} {quote_currency}")

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
        print(f"Placing limit order:")
        print(f"\tproduct_id: {market_name}")
        print(f"\t      side: {order_side}")
        print(f"\t     price: {target_price} {quote_currency}")
        print(f"\t    amount: {base_currency_amount} {base_currency}")
        print(f"\t     value: {(base_currency_amount * target_price).quantize(quote_increment)} {quote_currency}")
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

        if result and "status" in result and result["status"] == "rejected":
            # Rejected - usually because price was above lowest sell offer. Try
            #   again in the next loop.
            print("%s: %s Order rejected @ %f %s" % (get_timestamp(),
                                                     market_name,
                                                     current_price,
                                                     quote_currency))

        update_order_from_json(order, result, percent_diff=percent_diff)

