# Ready strategy configs for TRUE perps

`conf_true_btc_mm.yml`: perpetual market making on `true_perpetual`, BTC-USDC, 5x, 0.5 percent
each side, 0.001 BTC per order, refreshed every 30 s. Position mode is `One-way` (the only mode
the TRUE connector supports).

Use it without the create wizard:

```bash
cp conf_examples/conf_true_btc_mm.yml conf/strategies/
docker run -it --name hbot-true -e CONFIG_FILE_NAME=conf_true_btc_mm.yml \
  -v "$PWD/conf:/home/hummingbot/conf" -v "$PWD/logs:/home/hummingbot/logs" \
  -v "$PWD/data:/home/hummingbot/data" hummingbot-true
```

`connect true_perpetual` has to be done once before (key id `tbk_...`, secret `tbs_...` from the
Bots tab at https://app.truefinance.ai/bots). After that the container asks only for the
Hummingbot password and starts quoting; `status` shows the orders, `stop` cancels them.
