# ArbiGuard

ArbiGuard is an Erlang/rebar3 project for cross-exchange perpetual arbitrage.

This project is intentionally separated from the AI/model-training code. It keeps only the arbitrage path:

- multi-exchange funding/price opportunity scan
- opportunity scoring
- paper account simulation
- position uniqueness by `symbol + long_exchange + short_exchange`
- HTTP management API
- Windows-compatible OTP dependencies only

## Run

```powershell
cd E:\contract\ArbiGuard
rebar3 shell
```

Default HTTP port:

```text
http://127.0.0.1:8771
```

Or:

```powershell
cd E:\contract\ArbiGuard
.\scripts\start.ps1 -Port 8771
```

## APIs

```text
GET  /api/health
GET  /api/config
GET  /api/funding/state
POST /api/funding/scan
POST /api/funding/paper/reset
```

Runtime configuration lives in:

```text
config/sys.config
config/vm.args
```

Important keys:

```erlang
{http_port, 8771}.
{paper_capital_usdt, 10000.0}.
{default_scan, #{...}}.
{exchanges, [#{id => <<"binance">>, ...}]}.
{execution, #{max_order_book_age_ms => 1000, ...}}.
```

Example scan with local snapshots:

```powershell
$body = Get-Content .\examples\scan_payload.json -Raw
Invoke-RestMethod http://127.0.0.1:8771/api/funding/scan -Method POST -ContentType application/json -Body $body
```

## Notes

The first Erlang version focuses on extracting the arbitrage domain into a clean OTP application. Exchange adapters are isolated in `arbiguard_market`, so more REST/WS details can be filled in without touching the scanner or paper account.
