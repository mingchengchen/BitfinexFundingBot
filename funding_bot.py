"""
Bitfinex USD funding robot.

Author: Mingcheng Chen
Email: linyufly@gmail.com
"""

from collections import deque

import asyncio
import datetime
import hashlib
import hmac
import json
import sys
import websockets


url = 'wss://api.bitfinex.com/ws/2'

max_offer_pending_time = 120.0
min_offer_amount = 50.0

funding_available_usd = 0.0
active_offers = dict()
last_bid_rate = 0.0
last_bid_period = 30

lock = asyncio.Lock()


def is_dict(message):
  return type(message) == dict


def is_list(message):
  return type(message) == list


def get_event(message):
  assert is_dict(message)
  return message['event']


def get_type(message):
  assert is_list(message)
  return message[1]


def is_connected(message):
  return is_dict(message) and                \
         get_event(message) == 'info' and    \
         'platform' in message and           \
         'status' in message['platform'] and \
         message['platform']['status'] == 1


def is_authenticated(message):
  return is_dict(message) and             \
         get_event(message) == 'auth' and \
         message['status'] == 'OK'


def is_error(message):
  return is_dict(message) and \
         get_event(message) == 'error'


def is_subscribed(message):
  return is_dict(message) and \
         get_event(message) == 'subscribed'


def is_heart_beat(message):
  return is_list(message) and \
         get_type(message) == 'hb'


def is_wallet_update(message):
  return is_list(message) and \
         get_type(message) == 'wu'


def is_public_funding_trade_executed(message):
  return is_list(message) and \
         message[0] != 0 and  \
         get_type(message) == 'fte'


def is_funding_offer_snapshot(message):
  return is_list(message) and \
         message[0] == 0 and  \
         message[1] == 'fos'


def is_funding_offer_update(message):
  return is_list(message) and \
         message[0] == 0 and  \
         message[1] in ['fon', 'fou', 'foc']


def is_public_funding_ticker(message):
  return is_list(message) and    \
         message[0] != 0 and     \
         is_list(message[1]) and \
         not is_list(message[1][0])


async def subscribe_funding_trades(ws):
  payload = {
    'event': 'subscribe',
    'channel': 'trades',
    'symbol': 'fUSD'
  }
  await ws.send(json.dumps(payload))


async def subscribe_funding_tickers(ws):
  payload = {
    'event': 'subscribe',
    'channel': 'ticker',
    'symbol': 'fUSD'
  }
  await ws.send(json.dumps(payload))


async def login(ws):
  if len(sys.argv) != 2:
    print("Usage: python3 funding_robot.py <configuration file>")
    exit()
  reader = open(sys.argv[1])
  params = json.loads(reader.read())
  api_key = params['key']
  api_secret = params['secret']
  auth_nonce = int(datetime.datetime.now().timestamp() * 1e6)
  auth_payload = f'AUTH{auth_nonce}'
  signature = hmac.new(
      api_secret.encode('utf8'),
      auth_payload.encode('utf8'),
      hashlib.sha384).hexdigest()
  payload = {
    'apiKey': api_key,
    'authSig': signature,
    'authNonce': auth_nonce,
    'authPayload': auth_payload,
    'event': 'auth',
    'filter': [
      'funding',
      'wallet'
    ]
  }
  await ws.send(json.dumps(payload))


async def submit_offer(ws, amount, rate, period):
  payload = {
    'type': 'LIMIT',
    'symbol': 'fUSD',
    'amount': str(amount),
    'rate': str(rate),
    'period': period
  }
  message = [0, 'fon', None, payload]  
  await ws.send(json.dumps(message))


async def cancel_offer(ws, id):
  message = [0, 'foc', None, {'id': id}]
  await ws.send(json.dumps(message))


def update_offer(message):
  global active_offers
  offer = dict()
  id = message[0]
  offer['time'] = message[3]
  offer['amount'] = message[4]
  offer['rate'] = message[14]
  status = message[10]
  if status != 'ACTIVE':
    if id in active_offers:
      del active_offers[id]
  else:
    active_offers[id] = offer


def print_active_offers():
  global active_offers
  print("Active offers:")
  for id in active_offers.keys():
    print(f"{id}: {active_offers[id]}")


def update_funding_available_usd(message):
  global funding_available_usd
  funding_available_usd = message[2][4]
  print(f"Available funding USD: ${funding_available_usd}")


def update_public_ticker(message):
  global last_bid_rate
  global last_bid_period
  last_bid_rate = message[1][1]
  last_bid_period = message[1][2]
  print(f"Last bid: {last_bid_rate * 100}% for {last_bid_period} days.")


async def make_offer_decision(ws):
  global active_offers
  global max_offer_pending_time
  global funding_available_usd
  global min_offer_amount
  global last_bid_rate
  global last_bid_period
  # Cancel all the too old active offers.
  current_time = datetime.datetime.now().timestamp() * 1e3
  for id in active_offers.keys():
    last_update_time = active_offers[id]['time']
    age = (current_time - last_update_time) * 1e-3
    if age > max_offer_pending_time:
      await cancel_offer(ws, id)
      print(f"Cancelled offer {id}, {age} seconds old.")
  # Submit an offer.
  if funding_available_usd >= min_offer_amount:
    await submit_offer(
        ws,
        amount=min_offer_amount,
        rate=last_bid_rate,
        period=last_bid_period)
    print("Submitted an offer.")


async def dispatch(ws, message):
  if is_error(message):
    print(message)
  elif is_connected(message):
    await login(ws)
    await subscribe_funding_trades(ws)
    await subscribe_funding_tickers(ws)
  elif is_authenticated(message):
    print("Authenticated.")
  elif is_subscribed(message):
    print(f"Subscribed to {message['channel']} {message['symbol']}.")
  elif is_heart_beat(message):
    if (message[0] != 0):  # Skip the public channel heart beat.
      return
    print("Private heart beat.")
    await make_offer_decision(ws)
  elif is_wallet_update(message):
    if message[2][0] == 'funding' and \
       message[2][1] == 'USD':
      update_funding_available_usd(message)
  elif is_funding_offer_update(message):
    update_offer(message[2])
    print_active_offers()
  elif is_funding_offer_snapshot(message):
    for item in message[2]:
      update_offer(item)
    print_active_offers()
  elif is_public_funding_ticker(message):
    update_public_ticker(message)
    await make_offer_decision(ws)


async def consume(ws):
  async for message in ws:
    async with lock:
      await dispatch(ws, json.loads(message))


async def connect():
  while True:
    try:
      async with websockets.connect(url) as ws:
        await consume(ws)
    except Exception as e:
      print(f"Exception: {e}")
      continue


if __name__ == '__main__':
  asyncio.run(connect())
