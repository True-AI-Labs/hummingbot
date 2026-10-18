"""
Live check for the true_perpetual connector against app.truefinance.ai.

    TRUE_KEY=tbk_... TRUE_SECRET=tbs_... python scripts/true_perpetual_live_check.py [BTC-USDC]

Reads: time sync, markets, trading rules, ticker, balances, positions, order book snapshot,
funding info, websocket market data, the account event stream.
Writes (only with TRUE_PLACE=1): one post-only limit buy 20 percent under the mark at the
minimum lot, status, fills, cancel, status again. Nothing else is written.
"""
import asyncio
import os
import sys
import time
from decimal import Decimal

from hummingbot.connector.derivative.true_perpetual.true_perpetual_derivative import TruePerpetualDerivative
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder

PAIR = sys.argv[1] if len(sys.argv) > 1 else "BTC-USDC"
ROWS = []


def row(name, ok, detail=""):
    ROWS.append((name, "OK" if ok else "FAIL", str(detail)[:110]))
    print(f"{'OK  ' if ok else 'FAIL'} {name}: {str(detail)[:110]}")


async def main():
    key = os.environ.get("TRUE_KEY", "")
    secret = os.environ.get("TRUE_SECRET", "")
    c = TruePerpetualDerivative(true_perpetual_api_key=key, true_perpetual_api_secret=secret, trading_pairs=[PAIR])

    await c._update_time_synchronizer()
    row("time sync", True, f"offset {c._time_synchronizer.time_offset_ms:.0f} ms")

    await c._initialize_trading_pair_symbol_map()
    symbol = await c.exchange_symbol_associated_to_pair(PAIR)
    row("symbol map", symbol == PAIR.replace("-", ""), f"{PAIR} -> {symbol}, {len(c.trading_pair_symbol_map_ready() and await c.all_trading_pairs())} pairs")

    await c._update_trading_rules()
    rule = c.trading_rules.get(PAIR)
    row("trading rules", rule is not None, f"lot {rule.min_base_amount_increment} tick {rule.min_price_increment} min notional {rule.min_notional_size}")

    await c._update_trading_fees()
    row("fees", PAIR in c._trading_fees, c._trading_fees.get(PAIR))

    prices = await c.get_last_traded_prices([PAIR])
    row("last traded price", PAIR in prices, prices)

    await c._update_balances()
    row("balances", "USDC" in c.get_all_balances(), f"total {c.get_balance('USDC')} available {c.get_available_balance('USDC')}")

    await c._update_positions()
    row("positions", True, {k: f"{v.position_side.name} {v.amount} @ {v.entry_price} lev {v.leverage}" for k, v in c.account_positions.items()})

    ds = c._create_order_book_data_source()
    snap = await ds._order_book_snapshot(PAIR)
    row("order book snapshot", len(snap.bids) > 0 and len(snap.asks) > 0, f"{len(snap.bids)} bids / {len(snap.asks)} asks, best bid {snap.bids[0].price} best ask {snap.asks[0].price}")

    fi = await ds.get_funding_info(PAIR)
    row("funding info", fi.mark_price > 0, f"mark {fi.mark_price} rate {fi.rate} next {fi.next_funding_utc_timestamp}")

    # websocket market data for a few seconds
    ws = await ds._connected_websocket_assistant()
    await ds._subscribe_channels(ws)
    counts = {}

    async def pump():
        async for resp in ws.iter_messages():
            data = resp.data
            if data is None:
                break
            ch = ds._channel_originating_message(data)
            if ch:
                counts[ch] = counts.get(ch, 0) + 1
                await {
                    ds._snapshot_messages_queue_key: ds._parse_order_book_snapshot_message,
                    ds._diff_messages_queue_key: ds._parse_order_book_diff_message,
                    ds._trade_messages_queue_key: ds._parse_trade_message,
                    ds._funding_info_messages_queue_key: ds._parse_funding_info_message,
                }[ch](data, asyncio.Queue())
            else:
                await ds._process_message_for_unknown_channel(data, ws)
    try:
        await asyncio.wait_for(pump(), timeout=8)
    except asyncio.TimeoutError:
        pass
    await ws.disconnect()
    row("websocket market data", counts.get(ds._snapshot_messages_queue_key, 0) + counts.get(ds._diff_messages_queue_key, 0) > 0 and counts.get(ds._funding_info_messages_queue_key, 0) > 0, counts)

    # account event stream for a few seconds
    us = c._create_user_stream_data_source()
    q = asyncio.Queue()
    task = asyncio.ensure_future(us.listen_for_user_stream(q))
    try:
        ev = await asyncio.wait_for(q.get(), timeout=10)
        row("account stream", ev.get("event") == "account", f"event {ev.get('event')} equity {(ev.get('data') or {}).get('account', {}).get('equityUsd')}")
    except asyncio.TimeoutError:
        row("account stream", False, "no event in 10 s")
    task.cancel()
    await us.stop()

    if os.environ.get("TRUE_PLACE") == "1":
        mark = fi.mark_price
        market = await c.market_info(symbol)
        band_min = Decimal(str((market.get("priceBands") or {}).get("min") or "0"))
        price = max(mark * Decimal("0.8"), band_min * Decimal("1.02"))
        price = c.quantize_order_price(PAIR, price)
        amount = c.quantize_order_amount(PAIR, rule.min_base_amount_increment)
        if amount * price < rule.min_notional_size:
            amount = c.quantize_order_amount(PAIR, (rule.min_notional_size / price) * Decimal("1.2"))
        cid = f"HBOTcheck{int(time.time())}"
        oid, ts = await c._place_order(cid, PAIR, amount, TradeType.BUY, OrderType.LIMIT_MAKER, price, PositionAction.OPEN)
        row("place limit maker", bool(oid), f"id {oid} {amount} @ {price}")
        order = InFlightOrder(client_order_id=cid, exchange_order_id=oid, trading_pair=PAIR, order_type=OrderType.LIMIT_MAKER,
                              trade_type=TradeType.BUY, amount=amount, price=price, creation_timestamp=ts, position=PositionAction.OPEN)
        upd = await c._request_order_status(order)
        row("order status open", upd.new_state.name in ("OPEN", "PARTIALLY_FILLED"), upd.new_state.name)
        fills = await c._all_trade_updates_for_order(order)
        row("fills for order", True, f"{len(fills)} fills")
        ok = await c._place_cancel(cid, order)
        row("cancel", ok, ok)
        await asyncio.sleep(1.5)
        upd = await c._request_order_status(order)
        row("order status cancelled", upd.new_state.name == "CANCELED", upd.new_state.name)
        try:
            await c._place_cancel(cid, order)
            row("cancel twice", False, "second cancel did not raise")
        except IOError as err:
            row("cancel twice", c._is_order_not_found_during_cancelation_error(err), str(err)[:80])

    fails = [r for r in ROWS if r[1] == "FAIL"]
    print(f"\n{len(ROWS) - len(fails)}/{len(ROWS)} passed")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
