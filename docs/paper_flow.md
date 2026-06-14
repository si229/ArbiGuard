# 模拟盘流程

本文说明当前模拟盘从行情进入系统、生成套利机会、开仓、持仓监控到平仓结算的完整流程。

## 总流程

```text
交易所行情/资金费 -> ETS
扫描进程 -> ETS 机会表
开仓执行进程 -> 自己读取 ETS 机会 -> 等实时 ticker -> 模拟开仓
模拟账户进程 -> 扣手续费/保证金/生成持仓
平仓执行进程 -> 监听持仓 ticker/资金费 -> 满足规则后模拟平仓
模拟账户进程 -> 结算盈亏/手续费/释放保证金
```

## 1. 行情和资金费进入 ETS

交易所公共数据由两个方向进入本地 ETS：

- `arbiguard_exchange_ticker` 维护公共 ticker、盘口、标记价和更新时间，写入 `arbiguard_ticker_ets`。
- `arbiguard_exchange_funding` 维护资金费率、结算时间、结算周期和合约状态，写入 `arbiguard_funding_ets`。

资金费数据不能写入 ticker ETS。ticker ETS 只保存成交参考价格、盘口、标记价等行情数据。

## 2. 扫描进程只负责生成机会

`arbiguard_scanner` 定时从 ETS 读取 ticker + funding，计算套利机会，然后写入 `arbiguard_opportunity_ets`。

当前主流程不再使用：

```text
scanner -> executor -> account_manager -> open_executor
```

而是使用：

```text
scanner -> arbiguard_opportunity_ets
open_executor -> arbiguard_opportunity_ets
```

也就是说，扫描进程是生产者，开仓执行进程是消费者。谁需要机会数据，谁自己读取 ETS，并按自己的账户和策略决定是否加入待开仓单。

## 3. 开仓执行进程自己读取机会

`arbiguard_open_executor` 启动后会定时读取 `arbiguard_ets:opportunity_snapshot()`。

开仓执行进程会检查：

- 最低预期利润 `min_execution_profit_usdt`
- 最大持仓笔数 `max_open_positions`
- 当前持仓 + 待执行开仓单数量
- 是否重复单
- 账户模式和账户 ID

通过检查后，机会才会变成待开仓执行单。

## 4. 开仓前等待实时 ticker

待开仓单会进入 `waiting_ws_ticker`。

进入执行阶段后，开仓执行进程会通知对应交易所 ticker 进程订阅该币种。这里的订阅分两层：

- 交易所 WS 订阅：行情进程向交易所订阅该 symbol。
- 本地订阅：执行进程告诉行情进程，如果收到这个 symbol 的 ticker，也给自己推送一份。

任意一边 ticker 更新时，开仓执行进程会读取两边最新行情，并重新计算：

- 做多腿按 `ask` 成交
- 做空腿按 `bid` 成交
- 标记价 `mark_price` 只用于爆仓风险判断
- 可成交数量按两边盘口可成交名义额取较小值
- 扣除手续费后的预期利润是否仍满足阈值

## 5. 模拟开仓

模拟盘不会真正向交易所下单。

满足执行条件后，开仓执行进程调用：

```erlang
arbiguard_state:apply_open_order(Req, Order, Op)
```

模拟账户会执行：

- 按盘口成交价生成多空两腿持仓
- 扣开仓手续费
- 扣保证金
- 更新交易所维度模拟余额和权益
- 生成持仓记录

开仓完成后，开仓执行进程会把持仓交给平仓执行进程：

```erlang
arbiguard_account_manager:track_position(Req, Position)
```

账户管理进程这里只做账户路由，不参与平仓策略。

## 6. 平仓执行进程独立管理持仓

`arbiguard_close_executor` 收到持仓后开始跟踪该仓位，并保持 ticker 订阅。

平仓执行进程根据实时 ticker 重算：

- 做多腿平仓价使用 `bid`
- 做空腿平仓价使用 `ask`
- 当前价差盈亏
- 已结算资金费盈亏
- 预计平仓手续费
- 扣除平仓手续费后的当前净浮盈
- 标记价触发的爆仓风险

## 7. 平仓规则

满足任一主要条件时，平仓执行进程会启动模拟平仓：

- 当前实际净浮盈达到 `min_execution_profit_usdt`
- 资金费结算后盈利满足规则
- 95 / 90 / 85 / 80 / 50 分段锁利规则触发
- 最近一次资金费为正收益，但下一周期可能转亏，需要锁利
- 下架、停牌、爆仓风险等保护规则触发

模拟平仓调用：

```erlang
arbiguard_state:apply_close_order(Req, Order, Position)
```

模拟账户会执行：

- 按当前盘口计算平仓成交价
- 扣平仓手续费
- 释放保证金
- 结算价差盈亏
- 结算资金费盈亏
- 写入成交和平仓记录
- 更新交易所维度余额、权益、持仓占用和浮盈

## 8. 模拟盘和实盘的区别

模拟盘在执行阶段只更新本地模拟账户，不调用真实下单模块。

实盘会走：

```erlang
arbiguard_live_order:submit_open/2
arbiguard_live_order:submit_close/2
```

并等待私有 WS 的订单、成交、余额、持仓、资金费和爆仓事件回调。

模拟盘使用本地 `arbiguard_state` 作为账户状态源；实盘使用账户绑定的交易所账户进程和私有 WS 作为真实状态源。

