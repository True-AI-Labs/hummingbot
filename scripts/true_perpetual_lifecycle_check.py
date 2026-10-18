"""
Runs the true_perpetual connector the way Hummingbot does (clock ticks, order tracker, polling
loops, account stream) and walks one order through it: buy() -> created -> cancel() -> cancelled.

    TRUE_KEY=tbk_... TRUE_SECRET=tbs_... python scripts/true_perpetual_lifecycle_check.py [BTC-USDC]
"""
import asyncio
import logging
import os
import sys
import time
from decimal import Decimal

from hummingbot.connector.derivative.true_perpetual.true_perpetual_derivative import TruePerpetualDerivative
from hummingbot.core.clock import Clock
from hummingbot.core.clock_mode import ClockMode
from hummingbot.core.data_type.common import OrderType, PositionAction
from hummingbot.core.event.event_forwarder import EventForwarder
from hummingbot.core.event.events import MarketEvent

PAIR = sys.argv[1] if len(sys.argv) > 1 else "BTC-USDC"
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
events = []
forwarders = []  # PubSub holds listeners weakly, so keep them alive here


def record(name):
    def _f(ev):
        events.append((name, ev))
        print(f"EVENT {name}: {ev}")
    return _f


async def wait_for(pred, timeout, what):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if pred():
            return True
        await asyncio.sleep(0.5)
    print(f"TIMEOUT waiting for {what}")
    return False


async def main():
    c = TruePerpetualDerivative(
        true_perpetual_api_key=os.environ["TRUE_KEY"],
        true_perpetual_api_secret=os.environ["TRUE_SECRET"],
        trading_pairs=[PAIR],
    )
    for name in ("BuyOrderCreated", "OrderCancelled", "MarketOrderFailure", "OrderFilled", "BuyOrderCompleted",
                 "OrderFailure", "FundingPaymentCompleted"):
        ev = getattr(MarketEvent, name, None)
        if ev is not None:
            fwd = EventForwarder(record(name))
            forwarders.append(fwd)
            c.add_listener(ev, fwd)

    clock = Clock(ClockMode.REALTIME, tick_size=1.0)
    clock.add_iterator(c)
    await c.start_network()
    async def run_clock():
        with clock:
            await clock.run()
    clock_task = asyncio.ensure_future(run_clock())
    try:
        ok = await wait_for(lambda: c.ready, 60, "connector ready")
        print("status_dict:", c.status_dict)
        if not ok:
            return 1
        print("balances:", dict(c.get_all_balances()), "available USDC:", c.get_available_balance("USDC"))
        print("positions:", {k: (v.position_side.name, str(v.amount), str(v.entry_price)) for k, v in c.account_positions.items()})
        ob = c.get_order_book(PAIR)
        print("book:", ob.get_price(True), ob.get_price(False))
        fi = c.get_funding_info(PAIR)
        print("funding:", fi.mark_price, fi.rate)

        mark = Decimal(str(fi.mark_price))
        price = c.quantize_order_price(PAIR, mark * Decimal("0.8"))
        rule = c.trading_rules[PAIR]
        amount = c.quantize_order_amount(PAIR, rule.min_base_amount_increment)
        if amount * price < rule.min_notional_size:
            amount = c.quantize_order_amount(PAIR, rule.min_notional_size / price * Decimal("1.2"))
        print(f"placing LIMIT_MAKER buy {amount} {PAIR} @ {price}")
        cid = c.buy(PAIR, amount, OrderType.LIMIT_MAKER, price, position_action=PositionAction.OPEN)
        created = await wait_for(lambda: any(n == "BuyOrderCreated" for n, _ in events), 20, "BuyOrderCreated")
        if not created:
            return 1
        order = c.in_flight_orders.get(cid)
        print("in flight:", order.client_order_id, order.exchange_order_id, order.current_state)
        await asyncio.sleep(12)  # let status polls run against the open order
        order = c.in_flight_orders.get(cid)
        print("after poll:", order and order.current_state)
        c.cancel(PAIR, cid)
        cancelled = await wait_for(lambda: any(n == "OrderCancelled" for n, _ in events), 20, "OrderCancelled")
        print("in flight after cancel:", list(c.in_flight_orders.keys()))
        return 0 if cancelled else 1
    finally:
        clock_task.cancel()
        await c.stop_network()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
