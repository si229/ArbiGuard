-module(arbiguard_state).
-behaviour(gen_server).

-export([start_link/1, snapshot/0, reset_paper/1, submit_scan/2, apply_open_order/3, apply_close_order/3,
         set_exchange_token/2, get_exchange_token/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {paper}).

start_link(Capital) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Capital], []).

snapshot() ->
    gen_server:call(?MODULE, snapshot).

reset_paper(Payload) ->
    gen_server:call(?MODULE, {reset_paper, Payload}).

submit_scan(Req, Result) ->
    gen_server:call(?MODULE, {submit_scan, Req, Result}).

apply_open_order(Req, Order, Opportunity) ->
    gen_server:call(?MODULE, {apply_open_order, Req, Order, Opportunity}).

apply_close_order(Req, Order, Position) ->
    gen_server:call(?MODULE, {apply_close_order, Req, Order, Position}).

set_exchange_token(ExchangeID, TokenConfig) ->
    gen_server:call(?MODULE, {set_exchange_token, ExchangeID, TokenConfig}).

get_exchange_token(ExchangeID) ->
    gen_server:call(?MODULE, {get_exchange_token, ExchangeID}).

init([Capital]) ->
    {ok, #state{paper = new_paper(Capital)}}.

handle_call(snapshot, _From, State = #state{paper = Paper}) ->
    Snapshot = paper_snapshot(Paper),
    {reply, Snapshot, State};
handle_call({reset_paper, Payload}, _From, State) ->
    Capital = arbiguard_util:to_float(maps:get(capital_usdt, Payload, 10000), 10000),
    Paper = new_paper(Capital),
    {reply, paper_snapshot(Paper), State#state{paper = Paper}};
handle_call({submit_scan, Req, Result}, _From, State = #state{paper = Paper0}) ->
    Paper = update_paper(Paper0, Req, Result),
    {reply, paper_snapshot(Paper), State#state{paper = Paper}};
handle_call({apply_open_order, Req0, Order, Opportunity}, _From, State = #state{paper = Paper0}) ->
    Req = arbiguard_calc:normalize_request(Req0),
    Paper1 = ensure_exchange_balances(Paper0, maps:get(exchanges, Req, [])),
    Key = maps:get(id, Order),
    Notional = maps:get(target_notional, Order, maps:get(suggested_notional, Opportunity, 0)),
    Paper2 = open_position(Paper1, Req, Key, Opportunity, Notional),
    Paper = refresh_equity(Paper2#{updated_at => iso_now()}),
    Snapshot = paper_snapshot(Paper),
    {reply, Snapshot#{opened_position => position_by_id(Key, Snapshot)}, State#state{paper = Paper}};
handle_call({apply_close_order, _Req0, Order, Position0}, _From, State = #state{paper = Paper0}) ->
    Position = normalize_close_position(Position0),
    Key = maps:get(id, Position, maps:get(position_id, Order, maps:get(id, Order, <<"">>))),
    Paper1 = close_position(Paper0, Key, Position, maps:get(close_rule, Order, maps:get(close_rule, Position, <<"strategy_close">>))),
    Paper = refresh_equity(Paper1#{updated_at => iso_now()}),
    {reply, paper_snapshot(Paper), State#state{paper = Paper}};
handle_call({set_exchange_token, ExchangeID, TokenConfig}, _From, State) ->
    {reply, arbiguard_live_account:set_exchange_token(ExchangeID, TokenConfig), State};
handle_call({get_exchange_token, ExchangeID}, _From, State) ->
    {reply, arbiguard_live_account:get_exchange_token(ExchangeID), State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

new_paper(Capital0) ->
    Capital = case Capital0 > 0 of true -> Capital0; false -> 10000.0 end,
    #{enabled => true,
      initial_balance => Capital,
      balance => Capital,
      equity => Capital,
      realized_pnl => 0.0,
      unrealized_pnl => 0.0,
      expected_profit => 0.0,
      total_open_fee => 0.0,
      total_close_fee => 0.0,
      total_funding_pnl => 0.0,
      total_price_pnl => 0.0,
      execution_attempts => 0,
      execution_skips => 0,
      exchange_balances => #{},
      positions => #{},
      logs => [],
      trade_count => 0,
      updated_at => iso_now()}.

update_paper(Paper0, Req0, Result) ->
    Req = arbiguard_calc:normalize_request(Req0),
    Exchanges = maps:get(exchanges, Result, maps:get(exchanges, Req, [])),
    Paper1 = ensure_exchange_balances(Paper0, Exchanges),
    Ops = maps:get(opportunities, Result, []),
    Paper2 = refresh_positions(Paper1, Ops),
    Paper3 = open_best(Paper2, Req, Ops),
    refresh_equity(Paper3#{updated_at => iso_now()}).

ensure_exchange_balances(Paper, Exchanges) ->
    Bal0 = maps:get(exchange_balances, Paper, #{}),
    IDs = [maps:get(id, E) || E <- Exchanges],
    Existing = maps:keys(Bal0),
    Missing = [ID || ID <- IDs, not lists:member(ID, Existing)],
    case Missing of
        [] -> Paper;
        _ ->
            Capital = maps:get(initial_balance, Paper, 10000.0),
            All = lists:usort(IDs ++ Existing),
            Share = case length(All) of 0 -> Capital; N -> Capital / N end,
            Bal = maps:from_list([{ID, maps:get(ID, Bal0, Share)} || ID <- All]),
            Paper#{exchange_balances => Bal}
    end.

refresh_positions(Paper, Ops) ->
    OpByKey = maps:from_list([{position_pair_key(Op), Op} || Op <- Ops]),
    Positions0 = maps:get(positions, Paper, #{}),
    Positions = maps:map(fun(_Key, Pos) ->
        case maps:get(position_pair_key(Pos), OpByKey, undefined) of
            undefined -> Pos;
            Op -> refresh_position(Pos, Op)
        end
    end, Positions0),
    Paper#{positions => Positions}.

refresh_position(Pos, Op) ->
    LongPrice = positive(maps:get(long_price, Op, 0), maps:get(long_current_price, Pos, maps:get(long_entry_price, Pos, 0))),
    ShortPrice = positive(maps:get(short_price, Op, 0), maps:get(short_current_price, Pos, maps:get(short_entry_price, Pos, 0))),
    LongQty = maps:get(long_qty, Pos, 0),
    ShortQty = maps:get(short_qty, Pos, 0),
    PricePNL = (LongPrice - maps:get(long_entry_price, Pos, 0)) * LongQty +
               (maps:get(short_entry_price, Pos, 0) - ShortPrice) * ShortQty,
    FundingPNL = maps:get(funding_pnl, Pos, 0),
    CloseFee = maps:get(close_fee, Pos, 0),
    Pos#{
        long_current_price => LongPrice,
        short_current_price => ShortPrice,
        price_pnl => PricePNL,
        unrealized_pnl => PricePNL + FundingPNL - CloseFee,
        expected_net_profit => maps:get(estimated_net_profit, Op, maps:get(expected_net_profit, Pos, 0)),
        expected_net_return => maps:get(expected_net_return, Op, maps:get(expected_net_return, Pos, 0)),
        updated_at => arbiguard_util:now_ms()
    }.

open_best(Paper, Req, Ops0) ->
    MaxOpen = max(0, maps:get(max_open_positions, Req, 5)),
    Ops = lists:sort(fun(A, B) -> maps:get(expected_net_return, A, 0) >= maps:get(expected_net_return, B, 0) end, Ops0),
    lists:foldl(fun(Op, Acc) ->
        case maps:size(maps:get(positions, Acc, #{})) >= MaxOpen of
            true -> Acc;
            false -> maybe_open(Acc, Req, Op)
        end
    end, Paper, Ops).

maybe_open(Paper, Req, Op) ->
    Key = position_key(Req, Op),
    Positions = maps:get(positions, Paper, #{}),
    case maps:is_key(Key, Positions) of
        true -> Paper;
        false ->
            Notional = maps:get(suggested_notional, Op, max_position(Req)),
            MinProfit = maps:get(min_execution_profit_usdt, Req, 5.0),
            ExpectedProfit = maps:get(estimated_net_profit, Op, 0.0),
            case ExpectedProfit >= MinProfit of
                true -> open_position(Paper, Req, Key, Op, Notional);
                false -> Paper
            end
    end.

open_position(Paper, Req, Key, Op, Notional) ->
    Leverage = max(1.0, arbiguard_util:to_float(maps:get(paper_leverage, Req, 10), 10)),
    Account = account_scope(Req, Op),
    LongEx = maps:get(long_exchange, Op),
    ShortEx = maps:get(short_exchange, Op),
    LongPrice = maps:get(long_price, Op, 0.0),
    ShortPrice = maps:get(short_price, Op, 0.0),
    LongMarkPrice = maps:get(long_mark_price, Op, maps:get(long_price, Op, 0.0)),
    ShortMarkPrice = maps:get(short_mark_price, Op, maps:get(short_price, Op, 0.0)),
    LongFeeRate = maps:get(long_fee_rate, Op, 0.0005),
    ShortFeeRate = maps:get(short_fee_rate, Op, 0.0005),
    Margin = Notional / Leverage,
    OpenFee = Notional * (LongFeeRate + ShortFeeRate),
    Bal0 = maps:get(exchange_balances, Paper, #{}),
    LongNeed = Margin + Notional * LongFeeRate,
    ShortNeed = Margin + Notional * ShortFeeRate,
    case maps:get(LongEx, Bal0, 0) >= LongNeed andalso maps:get(ShortEx, Bal0, 0) >= ShortNeed of
        false -> add_reject(Paper, Op, <<"insufficient_exchange_balance">>);
        true ->
            Now = arbiguard_util:now_ms(),
            LongQty = safe_div(Notional, LongPrice),
            ShortQty = safe_div(Notional, ShortPrice),
            Pos = #{id => Key,
                    account_mode => maps:get(mode, Account),
                    account_id => maps:get(id, Account),
                    symbol => maps:get(symbol, Op),
                    long_exchange => LongEx,
                    short_exchange => ShortEx,
                    long_entry_price => LongPrice,
                    short_entry_price => ShortPrice,
                    long_current_price => LongPrice,
                    short_current_price => ShortPrice,
                    long_mark_price => LongMarkPrice,
                    short_mark_price => ShortMarkPrice,
                    long_liquidation_reference_price => LongMarkPrice,
                    short_liquidation_reference_price => ShortMarkPrice,
                    liquidation_price_basis => <<"mark_price">>,
                    long_qty => LongQty,
                    short_qty => ShortQty,
                    long_margin => Margin,
                    short_margin => Margin,
                    notional => Notional,
                    leverage => Leverage,
                    open_fee => OpenFee,
                    close_fee => 0.0,
                    funding_pnl => 0.0,
                    price_pnl => 0.0,
                    unrealized_pnl => 0.0,
                    expected_net_profit => maps:get(estimated_net_profit, Op, 0),
                    expected_net_return => maps:get(expected_net_return, Op, 0),
                    long_funding_rate => maps:get(long_funding_rate, Op, 0),
                    short_funding_rate => maps:get(short_funding_rate, Op, 0),
                    long_funding_interval_hours => maps:get(long_funding_interval_hours, Op, 8),
                    short_funding_interval_hours => maps:get(short_funding_interval_hours, Op, 8),
                    opened_at => Now,
                    updated_at => Now,
                    close_threshold => maps:get(price_gap_close_profit_usdt, Req, 10),
                    last_opportunity_method => maps:get(arbitrage_method, Op, <<"">>)},
            Trade = #{time => Now,
                      action => <<"open">>,
                      account_mode => maps:get(mode, Account),
                      account_id => maps:get(id, Account),
                      symbol => maps:get(symbol, Op),
                      long_exchange => LongEx,
                      short_exchange => ShortEx,
                      long_price => LongPrice,
                      short_price => ShortPrice,
                      long_qty => LongQty,
                      short_qty => ShortQty,
                      notional => Notional,
                      open_fee => OpenFee,
                      close_fee => 0.0,
                      fee => OpenFee,
                      funding_pnl => 0.0,
                      price_pnl => 0.0,
                      net_pnl => -OpenFee,
                      expected_net_profit => maps:get(estimated_net_profit, Op, 0),
                      reason => <<"paper_open_priority">>},
            Bal = Bal0#{LongEx => maps:get(LongEx, Bal0, 0) - LongNeed,
                        ShortEx => maps:get(ShortEx, Bal0, 0) - ShortNeed},
            Positions = maps:get(positions, Paper, #{}),
            Logs = [Trade | maps:get(logs, Paper, [])],
            Paper#{exchange_balances => Bal,
                   positions => Positions#{Key => Pos},
                   logs => lists:sublist(Logs, 1000),
                   trade_count => maps:get(trade_count, Paper, 0) + 1,
                   total_open_fee => maps:get(total_open_fee, Paper, 0) + OpenFee,
                   execution_attempts => maps:get(execution_attempts, Paper, 0) + 1}
    end.

close_position(Paper, Key, Position, Reason) ->
    Positions0 = maps:get(positions, Paper, #{}),
    Stored = maps:get(Key, Positions0, Position),
    Pos = maps:merge(Stored, Position),
    Notional = maps:get(notional, Pos, maps:get(notional_usdt, Pos, 0)),
    LongEx = maps:get(long_exchange, Pos, <<"">>),
    ShortEx = maps:get(short_exchange, Pos, <<"">>),
    LongClose = positive(maps:get(long_close_price, Pos, 0), maps:get(long_current_price, Pos, maps:get(long_entry_price, Pos, 0))),
    ShortClose = positive(maps:get(short_close_price, Pos, 0), maps:get(short_current_price, Pos, maps:get(short_entry_price, Pos, 0))),
    LongQty = maps:get(long_qty, Pos, safe_div(Notional, maps:get(long_entry_price, Pos, 0))),
    ShortQty = maps:get(short_qty, Pos, safe_div(Notional, maps:get(short_entry_price, Pos, 0))),
    PricePNL = (LongClose - maps:get(long_entry_price, Pos, 0)) * LongQty +
               (maps:get(short_entry_price, Pos, 0) - ShortClose) * ShortQty,
    FundingPNL = maps:get(funding_pnl, Pos, 0),
    CloseFee = maps:get(close_fee, Pos, Notional * (maps:get(long_fee_rate, Pos, 0.0005) + maps:get(short_fee_rate, Pos, 0.0005))),
    NetPNL = PricePNL + FundingPNL - CloseFee,
    LongMargin = maps:get(long_margin, Pos, Notional / max(1.0, maps:get(leverage, Pos, 10.0))),
    ShortMargin = maps:get(short_margin, Pos, Notional / max(1.0, maps:get(leverage, Pos, 10.0))),
    Bal0 = maps:get(exchange_balances, Paper, #{}),
    Bal = Bal0#{LongEx => maps:get(LongEx, Bal0, 0) + LongMargin + PricePNL / 2 + FundingPNL / 2 - CloseFee / 2,
                ShortEx => maps:get(ShortEx, Bal0, 0) + ShortMargin + PricePNL / 2 + FundingPNL / 2 - CloseFee / 2},
    Trade = #{time => arbiguard_util:now_ms(),
              action => <<"close">>,
              account_mode => maps:get(account_mode, Pos, <<"paper">>),
              account_id => maps:get(account_id, Pos, <<"paper-main">>),
              symbol => maps:get(symbol, Pos, <<"">>),
              long_exchange => LongEx,
              short_exchange => ShortEx,
              long_price => LongClose,
              short_price => ShortClose,
              long_entry_price => maps:get(long_entry_price, Pos, 0),
              short_entry_price => maps:get(short_entry_price, Pos, 0),
              long_qty => LongQty,
              short_qty => ShortQty,
              notional => Notional,
              open_fee => maps:get(open_fee, Pos, 0),
              close_fee => CloseFee,
              fee => CloseFee,
              funding_pnl => FundingPNL,
              price_pnl => PricePNL,
              net_pnl => NetPNL,
              expected_net_profit => maps:get(expected_net_profit, Pos, 0),
              reason => Reason},
    Logs = [Trade | maps:get(logs, Paper, [])],
    Paper#{exchange_balances => Bal,
           positions => maps:remove(Key, Positions0),
           logs => lists:sublist(Logs, 1000),
           trade_count => maps:get(trade_count, Paper, 0) + 1,
           total_close_fee => maps:get(total_close_fee, Paper, 0) + CloseFee,
           total_funding_pnl => maps:get(total_funding_pnl, Paper, 0) + FundingPNL,
           total_price_pnl => maps:get(total_price_pnl, Paper, 0) + PricePNL,
           realized_pnl => maps:get(realized_pnl, Paper, 0) + NetPNL}.

normalize_close_position(Position) ->
    Position#{long_current_price => positive(maps:get(long_close_price, Position, 0), maps:get(long_current_price, Position, 0)),
              short_current_price => positive(maps:get(short_close_price, Position, 0), maps:get(short_current_price, Position, 0))}.

add_reject(Paper, Op, Reason) ->
    Now = arbiguard_util:now_ms(),
    Trade = #{time => Now,
              action => <<"skip">>,
              symbol => maps:get(symbol, Op, <<"">>),
              long_exchange => maps:get(long_exchange, Op, <<"">>),
              short_exchange => maps:get(short_exchange, Op, <<"">>),
              notional => maps:get(suggested_notional, Op, 0),
              expected_net_profit => maps:get(estimated_net_profit, Op, 0),
              reason => Reason},
    Logs = [Trade | maps:get(logs, Paper, [])],
    Paper#{logs => lists:sublist(Logs, 1000),
           execution_skips => maps:get(execution_skips, Paper, 0) + 1}.

refresh_equity(Paper) ->
    Positions = maps:get(positions, Paper, #{}),
    Unrealized = lists:sum([maps:get(unrealized_pnl, P, 0) || {_K, P} <- maps:to_list(Positions)]),
    Expected = lists:sum([maps:get(expected_net_profit, P, 0) || {_K, P} <- maps:to_list(Positions)]),
    BalTotal = lists:sum([V || {_K, V} <- maps:to_list(maps:get(exchange_balances, Paper, #{}))]),
    Equity = BalTotal + position_margin_total(Positions) + Unrealized,
    Paper#{unrealized_pnl => Unrealized,
           expected_profit => Expected,
           equity => Equity,
           balance => BalTotal}.

paper_snapshot(Paper) ->
    Positions = [P || {_K, P} <- maps:to_list(maps:get(positions, Paper, #{}))],
    Logs = maps:get(logs, Paper, []),
    Paper#{
        positions => Positions,
        logs => Logs,
        pair_stats => pair_stats(Positions, Logs),
        exchange_equity => exchange_equity(Paper)
    }.

position_by_id(ID, Snapshot) ->
    case [P || P <- maps:get(positions, Snapshot, []), maps:get(id, P, undefined) =:= ID] of
        [Position | _] -> Position;
        [] -> undefined
    end.

pair_stats(Positions, Logs) ->
    Keys = lists:usort([pair_key(P) || P <- Positions] ++ [pair_key(L) || L <- Logs, maps:get(action, L, <<"">>) =/= <<"skip">>]),
    [pair_stat(K, Positions, Logs) || K <- Keys].

pair_stat(K, Positions, Logs) ->
    Ps = [P || P <- Positions, pair_key(P) =:= K],
    Ls = [L || L <- Logs, pair_key(L) =:= K],
    #{pair => K,
      net_pnl => lists:sum([maps:get(net_pnl, L, 0) || L <- Ls]) + lists:sum([maps:get(unrealized_pnl, P, 0) || P <- Ps]),
      funding_pnl => lists:sum([maps:get(funding_pnl, L, 0) || L <- Ls]) + lists:sum([maps:get(funding_pnl, P, 0) || P <- Ps]),
      price_pnl => lists:sum([maps:get(price_pnl, L, 0) || L <- Ls]) + lists:sum([maps:get(price_pnl, P, 0) || P <- Ps]),
      open_fee => lists:sum([maps:get(open_fee, L, 0) || L <- Ls]),
      close_fee => lists:sum([maps:get(close_fee, L, 0) || L <- Ls]),
      position_count => length(Ps)}.

exchange_equity(Paper) ->
    Bal = maps:get(exchange_balances, Paper, #{}),
    Positions = maps:get(positions, Paper, #{}),
    maps:map(fun(Ex, Value) ->
        Value + lists:sum([position_exchange_equity_delta(Ex, P) || {_K, P} <- maps:to_list(Positions)])
    end, Bal).

position_exchange_equity_delta(Ex, P) ->
    LongDelta = case maps:get(long_exchange, P, <<"">>) =:= Ex of
        true -> maps:get(long_margin, P, 0) + long_leg_pnl(P);
        false -> 0.0
    end,
    ShortDelta = case maps:get(short_exchange, P, <<"">>) =:= Ex of
        true -> maps:get(short_margin, P, 0) + short_leg_pnl(P);
        false -> 0.0
    end,
    LongDelta + ShortDelta.

long_leg_pnl(P) ->
    LongPrice = maps:get(long_current_price, P, maps:get(long_entry_price, P, 0)),
    LongEntry = maps:get(long_entry_price, P, 0),
    LongQty = maps:get(long_qty, P, 0),
    FundingPNL = maps:get(funding_pnl, P, 0) / 2,
    CloseFee = maps:get(close_fee, P, 0) / 2,
    (LongPrice - LongEntry) * LongQty + FundingPNL - CloseFee.

short_leg_pnl(P) ->
    ShortPrice = maps:get(short_current_price, P, maps:get(short_entry_price, P, 0)),
    ShortEntry = maps:get(short_entry_price, P, 0),
    ShortQty = maps:get(short_qty, P, 0),
    FundingPNL = maps:get(funding_pnl, P, 0) / 2,
    CloseFee = maps:get(close_fee, P, 0) / 2,
    (ShortEntry - ShortPrice) * ShortQty + FundingPNL - CloseFee.

position_margin_total(Positions) ->
    lists:sum([maps:get(long_margin, P, 0) + maps:get(short_margin, P, 0) || {_K, P} <- maps:to_list(Positions)]).

position_key(Req, Op) ->
    Account = account_scope(Req, Op),
    <<(maps:get(id, Account))/binary, "|", (maps:get(mode, Account))/binary, "|",
      (maps:get(symbol, Op))/binary, "|", (maps:get(long_exchange, Op))/binary, "|",
      (maps:get(short_exchange, Op))/binary>>.

position_pair_key(Item) ->
    <<(maps:get(symbol, Item, <<"">>))/binary, "|", (maps:get(long_exchange, Item, <<"">>))/binary, "|",
      (maps:get(short_exchange, Item, <<"">>))/binary>>.

pair_key(Item) ->
    <<(maps:get(long_exchange, Item, <<"">>))/binary, "->", (maps:get(short_exchange, Item, <<"">>))/binary>>.

account_scope(Req, Op) ->
    Mode0 = maps:get(account_mode, Req, maps:get(account_mode, Op, <<"paper">>)),
    Mode = case Mode0 of live -> <<"live">>; paper -> <<"paper">>; _ -> arbiguard_util:to_binary(Mode0) end,
    DefaultID = case Mode of <<"live">> -> <<"live-main">>; _ -> <<"paper-main">> end,
    #{mode => Mode, id => arbiguard_util:to_binary(maps:get(account_id, Req, maps:get(account_id, Op, DefaultID)))}.

max_position(Req) ->
    max(maps:get(execution_notional_usdt, Req, 200), maps:get(capital_usdt, Req, 10000) * maps:get(max_position_pct, Req, 0.1)).

safe_div(_A, B) when B =< 0 -> 0.0;
safe_div(A, B) -> A / B.

positive(V, _Fallback) when V > 0 -> V;
positive(_, Fallback) -> Fallback.

iso_now() ->
    {{Y, M, D}, {H, Min, S}} = calendar:universal_time(),
    list_to_binary(io_lib:format("~4..0B-~2..0B-~2..0BT~2..0B:~2..0B:~2..0BZ", [Y, M, D, H, Min, S])).
