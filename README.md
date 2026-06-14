# ArbiGuard

ArbiGuard is an Erlang/rebar3 project for cross-exchange perpetual futures arbitrage.

This project is intentionally separated from the AI/model-training code. It keeps only the arbitrage path:

- multi-exchange funding/price opportunity scan
- opportunity scoring
- paper account simulation
- position uniqueness by `symbol + long_exchange + short_exchange`
- HTTP management UI and API
- Windows-compatible OTP dependencies

## Runtime Process Tree

```text
arbiguard_sup
  core/
    arbiguard_app
    arbiguard_sup
    arbiguard_ets
    arbiguard_config
    arbiguard_processes
  account/
    arbiguard_account_manager
    arbiguard_exchange_account
    arbiguard_state
  execution/
    arbiguard_executor
    arbiguard_open_executor
    arbiguard_close_executor
  exchange/
    arbiguard_market
    arbiguard_exchange_ticker
    arbiguard_exchange_funding
    arbiguard_private_ws
  strategy/
    arbiguard_calc
    arbiguard_scanner
  http/
    arbiguard_http
  support/
    arbiguard_json
    arbiguard_util
```

Actual supervised workers:

```text
arbiguard_sup
  arbiguard_state
  arbiguard_open_executor
  arbiguard_close_executor
  arbiguard_account_manager
  arbiguard_exchange_ticker per exchange
  arbiguard_exchange_funding per exchange
  arbiguard_symbol_watcher
  arbiguard_scanner
  arbiguard_http

account_manager dynamic workers
  arbiguard_open_executor per live account
  arbiguard_close_executor per live account
  arbiguard_exchange_account per account/exchange
  arbiguard_private_ws per account/exchange
```

`arbiguard_ets` is not a process. It is a plain helper module. `arbiguard_sup`
creates these named ETS tables directly during supervisor initialization:

```text
arbiguard_ticker_ets
arbiguard_funding_ets
arbiguard_opportunity_ets
```

## Flow

```text
exchange_funding -> ETS funding/ticker baseline
exchange_ticker  -> ETS live ticker
scanner          -> reads ETS and finds opportunities
executor         -> routes to account-bound open/close executors
account_manager  -> owns total account registry and executor binding
exchange_account -> owns per account/exchange token, balance, positions, order callbacks
private_ws       -> account-bound private WS callbacks, started by account_manager
state            -> maintains default simulated account and positions
http             -> management UI and JSON APIs
```

Default accounts:

```text
paper-main
  mode = paper
  open_executor  = arbiguard_open_executor
  close_executor = arbiguard_close_executor

live-main
  mode = live
  open_executor  = arbiguard_open_executor_live_main
  close_executor = arbiguard_close_executor_live_main
  exchange_accounts are created when an account config or token specifies exchanges
```

## Run

```powershell
cd E:\contract\ArbiGuard
rebar3 shell
```

Default management UI:

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
GET  /
GET  /api/health
GET  /api/config
GET  /api/processes
GET  /api/accounts
POST /api/accounts
GET  /api/executor/state
GET  /api/funding/state
GET  /api/live/state
POST /api/live/token
POST /api/live/enabled
POST /api/live/test-order
POST /api/funding/scan
POST /api/funding/paper/reset
```

Runtime configuration:

```text
config/sys.config
config/vm.args
```

Logs use lager:

```text
log/arbiguard.log
log/error.log
```

Important config keys:

```erlang
{http_port, 8771}.
{paper_capital_usdt, 10000.0}.
{default_scan, #{...}}.
{exchanges, [#{id => <<"binance">>, ...}]}.
{execution, #{max_order_book_age_ms => 1000, ...}}.
```

## Documentation

- [Architecture and API](docs/architecture.md)
- [Trading rules](docs/trading_rules.md)
