-module(arbiguard_exchange_account).
-behaviour(gen_server).

-export([start_link/3, name/2, snapshot/2, stop/2, sync_now/2, set_token/3, get_token/2,
         report_order_event/3, report_balance/3, report_position/3,
         report_liquidation/3, report_funding_settlement/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {account_id, exchange_id, config = #{}, token = undefined,
                balances = #{}, positions = #{}, orders = #{}, liquidations = [],
                logs = [], last_sync_at = 0, last_sync_status = <<"not_started">>,
                last_sync_error = undefined}).

start_link(AccountID, ExchangeID, Config) ->
    gen_server:start_link({local, name(AccountID, ExchangeID)}, ?MODULE,
                          [AccountID, ExchangeID, Config], []).

name(AccountID, ExchangeID) ->
    list_to_atom("arbiguard_exchange_account_" ++ safe_atom_part(AccountID) ++ "_" ++ safe_atom_part(ExchangeID)).

snapshot(AccountID, ExchangeID) ->
    gen_server:call(name(AccountID, ExchangeID), snapshot).

stop(AccountID, ExchangeID) ->
    case whereis(name(AccountID, ExchangeID)) of
        undefined -> ok;
        Pid -> gen_server:stop(Pid, normal, 5000)
    end.

sync_now(AccountID, ExchangeID) ->
    gen_server:call(name(AccountID, ExchangeID), sync_now, 30000).

set_token(AccountID, ExchangeID, Token) ->
    gen_server:call(name(AccountID, ExchangeID), {set_token, Token}).

get_token(AccountID, ExchangeID) ->
    case whereis(name(AccountID, ExchangeID)) of
        undefined -> undefined;
        _ -> gen_server:call(name(AccountID, ExchangeID), get_token)
    end.

report_order_event(AccountID, ExchangeID, Event) ->
    safe_cast(AccountID, ExchangeID, {report_order_event, Event}).

report_balance(AccountID, ExchangeID, Balance) ->
    safe_cast(AccountID, ExchangeID, {report_balance, Balance}).

report_position(AccountID, ExchangeID, Position) ->
    safe_cast(AccountID, ExchangeID, {report_position, Position}).

report_liquidation(AccountID, ExchangeID, Event) ->
    safe_cast(AccountID, ExchangeID, {report_liquidation, Event}).

report_funding_settlement(AccountID, ExchangeID, Event) ->
    safe_cast(AccountID, ExchangeID, {report_funding_settlement, Event}).

init([AccountID0, ExchangeID0, Config]) ->
    {ok, #state{account_id = norm_account(AccountID0),
                exchange_id = norm_exchange(ExchangeID0),
                config = Config}}.

handle_call(snapshot, _From, State) ->
    {reply, public_state(State), State};
handle_call(sync_now, _From, State) ->
    {Result, State1} = run_account_sync(State),
    {reply, Result, State1};
handle_call({set_token, Token}, _From, State) ->
    lager:info("exchange account token configured account=~s exchange=~s",
               [State#state.account_id, State#state.exchange_id]),
    self() ! sync_account_snapshot,
    {reply, ok, State#state{token = Token}};
handle_call(get_token, _From, State) ->
    {reply, State#state.token, State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({report_order_event, Event0}, State = #state{orders = Orders, logs = Logs}) ->
    Event = Event0#{account_id => State#state.account_id, exchange => State#state.exchange_id},
    OrderID = maps:get(order_id, Event, maps:get(id, Event, <<"">>)),
    Orders1 = case OrderID of <<"">> -> Orders; _ -> Orders#{OrderID => Event} end,
    {noreply, State#state{orders = Orders1, logs = add_log(<<"order_event">>, Event, Logs)}};
handle_cast({report_balance, Balance0}, State = #state{balances = Balances, logs = Logs}) ->
    Balance = Balance0#{account_id => State#state.account_id, exchange => State#state.exchange_id,
                        updated_at => arbiguard_util:now_ms()},
    Asset = maps:get(asset, Balance, <<"USDT">>),
    {noreply, State#state{balances = Balances#{Asset => Balance}, logs = add_log(<<"balance">>, Balance, Logs)}};
handle_cast({report_position, Position0}, State = #state{positions = Positions, logs = Logs}) ->
    Position = Position0#{account_id => State#state.account_id, exchange => State#state.exchange_id,
                          updated_at => arbiguard_util:now_ms()},
    Key = position_key(Position),
    {noreply, State#state{positions = Positions#{Key => Position}, logs = add_log(<<"position">>, Position, Logs)}};
handle_cast({report_liquidation, Event0}, State = #state{liquidations = Liquidations, logs = Logs}) ->
    Event = Event0#{account_id => State#state.account_id, exchange => State#state.exchange_id,
                    time => arbiguard_util:now_ms()},
    {noreply, State#state{liquidations = lists:sublist([Event | Liquidations], 100),
                          logs = add_log(<<"liquidation">>, Event, Logs)}};
handle_cast({report_funding_settlement, Event0}, State = #state{logs = Logs}) ->
    Event = Event0#{account_id => State#state.account_id, exchange => State#state.exchange_id},
    {noreply, State#state{logs = add_log(<<"funding">>, Event, Logs)}};
handle_cast(_Msg, State) -> {noreply, State}.
handle_info(sync_account_snapshot, State) ->
    {_Result, State1} = run_account_sync(State),
    {noreply, State1};
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

safe_cast(AccountID, ExchangeID, Msg) ->
    Name = name(AccountID, ExchangeID),
    case whereis(Name) of
        undefined -> {error, exchange_account_not_found};
        _ -> gen_server:cast(Name, Msg), ok
    end.

public_state(State) ->
    #{account_id => State#state.account_id,
      exchange => State#state.exchange_id,
      token_configured => State#state.token =/= undefined,
      balances => State#state.balances,
      positions => maps:values(State#state.positions),
      orders => maps:values(State#state.orders),
      liquidations => State#state.liquidations,
      logs => State#state.logs,
      last_sync_at => State#state.last_sync_at,
      last_sync_status => State#state.last_sync_status,
      last_sync_error => State#state.last_sync_error}.

run_account_sync(State = #state{token = undefined}) ->
    Reason = <<"live_token_not_configured">>,
    Result = #{ok => false, reason => Reason, exchange => State#state.exchange_id},
    {Result, sync_status(State, <<"failed">>, Reason)};
run_account_sync(State) ->
    Result0 = catch arbiguard_live_adapter:account_snapshot(State#state.exchange_id, State#state.config, State#state.token),
    Result = case Result0 of
        R when is_map(R) -> R;
        {'EXIT', Reason} -> #{ok => false, reason => <<"live_snapshot_exception">>, detail => fmt(Reason)};
        _ -> #{ok => false, reason => <<"live_snapshot_bad_result">>}
    end,
    State1 = apply_snapshot_result(Result, State),
    notify_account_manager(Result, State1),
    {Result, State1}.

apply_snapshot_result(Result, State) ->
    Status = case maps:get(ok, Result, false) of true -> <<"ok">>; _ -> <<"failed">> end,
    Error = case Status of <<"ok">> -> undefined; _ -> maps:get(reason, Result, <<"live_snapshot_failed">>) end,
    Balances = merge_balances(maps:get(balances, Result, #{}), State#state.balances),
    Positions = merge_positions(maps:get(positions, Result, []), State#state.positions, State),
    Orders = merge_orders(maps:get(orders, Result, []), State#state.orders, State),
    Log = add_log(<<"account_snapshot">>, maps:without([raw], Result), State#state.logs),
    State#state{balances = Balances,
                positions = Positions,
                orders = Orders,
                logs = Log,
                last_sync_at = arbiguard_util:now_ms(),
                last_sync_status = Status,
                last_sync_error = Error}.

merge_balances(Balances0, Old) when is_map(Balances0) ->
    maps:fold(fun(_K, Balance0, Acc) ->
        Balance = ensure_map(Balance0),
        Asset = maps:get(asset, Balance, <<"USDT">>),
        Acc#{Asset => Balance}
    end, Old, Balances0);
merge_balances(Balances0, Old) when is_list(Balances0) ->
    lists:foldl(fun(Balance0, Acc) ->
        Balance = ensure_map(Balance0),
        Asset = maps:get(asset, Balance, <<"USDT">>),
        Acc#{Asset => Balance}
    end, Old, Balances0);
merge_balances(_Balances, Old) ->
    Old.

merge_positions(Positions0, Old, State) when is_list(Positions0) ->
    lists:foldl(fun(Position0, Acc) ->
        Position = (ensure_map(Position0))#{account_id => State#state.account_id,
                                            exchange => State#state.exchange_id,
                                            updated_at => arbiguard_util:now_ms()},
        Acc#{position_key(Position) => Position}
    end, Old, Positions0);
merge_positions(_Positions, Old, _State) ->
    Old.

merge_orders(Orders0, Old, State) when is_list(Orders0) ->
    lists:foldl(fun(Order0, Acc) ->
        Order = (ensure_map(Order0))#{account_id => State#state.account_id,
                                      exchange => State#state.exchange_id,
                                      updated_at => arbiguard_util:now_ms()},
        OrderID = maps:get(order_id, Order, maps:get(id, Order, <<"">>)),
        case OrderID of <<"">> -> Acc; _ -> Acc#{OrderID => Order} end
    end, Old, Orders0);
merge_orders(_Orders, Old, _State) ->
    Old.

notify_account_manager(Result, State) ->
    case maps:get(ok, Result, false) of
        true -> arbiguard_account_manager:exchange_snapshot_synced(State#state.account_id, State#state.exchange_id, public_state(State));
        _ -> ok
    end.

sync_status(State, Status, Error) ->
    State#state{last_sync_at = arbiguard_util:now_ms(),
                last_sync_status = Status,
                last_sync_error = Error,
                logs = add_log(<<"account_snapshot">>, #{ok => false, reason => Error}, State#state.logs)}.

ensure_map(Map) when is_map(Map) -> Map;
ensure_map(_) -> #{}.

fmt(Term) ->
    unicode:characters_to_binary(io_lib:format("~p", [Term])).

position_key(Position) ->
    <<(maps:get(symbol, Position, <<"">>))/binary, "|",
      (maps:get(side, Position, <<"">>))/binary>>.

add_log(Action, Data, Logs) ->
    lists:sublist([#{time => arbiguard_util:now_ms(), action => Action,
                     data => maps:without([raw], Data)} | Logs], 300).

norm_account(V) -> arbiguard_util:to_binary(V).
norm_exchange(V) -> string:lowercase(arbiguard_util:to_binary(V)).

safe_atom_part(V) ->
    S = binary_to_list(string:lowercase(arbiguard_util:to_binary(V))),
    [case ((C >= $a andalso C =< $z) orelse (C >= $0 andalso C =< $9)) of true -> C; false -> $_ end || C <- S].
