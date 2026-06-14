-module(arbiguard_live_account).
-behaviour(gen_server).

-export([start_link/0, snapshot/0, set_exchange_token/2, get_exchange_token/1,
         submit_order/2, report_fill/2, report_funding_settlement/2, set_enabled/1,
         debug_order/1, test_order/1, report_balance/2, report_position/2,
         report_order_event/2, report_liquidation/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {enabled = false, tokens = #{}, orders = #{}, logs = [],
                balances = #{}, positions = #{}, liquidations = [],
                debug_balances = #{}, debug_positions = #{}}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

snapshot() ->
    gen_server:call(?MODULE, snapshot).

set_enabled(Enabled) ->
    gen_server:call(?MODULE, {set_enabled, Enabled}).

set_exchange_token(ExchangeID, TokenConfig) ->
    gen_server:call(?MODULE, {set_exchange_token, ExchangeID, TokenConfig}).

get_exchange_token(ExchangeID) ->
    gen_server:call(?MODULE, {get_exchange_token, ExchangeID}).

submit_order(Req, Order) ->
    gen_server:call(?MODULE, {submit_order, Req, Order}).

report_fill(OrderID, Fill) ->
    gen_server:call(?MODULE, {report_fill, OrderID, Fill}).

report_order_event(ExchangeID, Event) ->
    gen_server:call(?MODULE, {report_order_event, ExchangeID, Event}).

report_balance(ExchangeID, Balance) ->
    gen_server:call(?MODULE, {report_balance, ExchangeID, Balance}).

report_position(ExchangeID, Position) ->
    gen_server:call(?MODULE, {report_position, ExchangeID, Position}).

report_liquidation(ExchangeID, Event) ->
    gen_server:call(?MODULE, {report_liquidation, ExchangeID, Event}).

report_funding_settlement(PositionID, Settlement) ->
    gen_server:call(?MODULE, {report_funding_settlement, PositionID, Settlement}).

debug_order(Payload) ->
    gen_server:call(?MODULE, {debug_order, Payload}).

test_order(Payload) ->
    gen_server:call(?MODULE, {test_order, Payload}, 30000).

init([]) ->
    {ok, #state{}}.

handle_call(snapshot, _From, State = #state{enabled = Enabled, tokens = Tokens, orders = Orders, logs = Logs,
                                           balances = LiveBalances, positions = LivePositions, liquidations = Liquidations,
                                           debug_balances = DebugBalances, debug_positions = DebugPositions}) ->
    {reply, #{enabled => Enabled,
              token_exchanges => maps:keys(Tokens),
              orders => maps:values(Orders),
              balances => LiveBalances,
              positions => maps:values(LivePositions),
              liquidations => Liquidations,
              debug_balances => DebugBalances,
              debug_positions => maps:values(DebugPositions),
              logs => Logs}, State};
handle_call({set_enabled, Enabled0}, _From, State) ->
    Enabled = truthy(Enabled0),
    lager:warning("live account enabled=~p", [Enabled]),
    {reply, #{enabled => Enabled}, State#state{enabled = Enabled}};
handle_call({set_exchange_token, ExchangeID0, TokenConfig}, _From, State = #state{tokens = Tokens}) ->
    ExchangeID = norm_exchange(ExchangeID0),
    lager:info("live token configured exchange=~s", [ExchangeID]),
    {reply, ok, State#state{tokens = Tokens#{ExchangeID => TokenConfig}}};
handle_call({get_exchange_token, ExchangeID0}, _From, State = #state{tokens = Tokens}) ->
    ExchangeID = norm_exchange(ExchangeID0),
    {reply, maps:get(ExchangeID, Tokens, undefined), State};
handle_call({submit_order, Req, Order0}, _From, State = #state{enabled = Enabled, orders = Orders, logs = Logs}) ->
    Now = arbiguard_util:now_ms(),
    AccountID = arbiguard_util:to_binary(maps:get(account_id, Order0, maps:get(account_id, Req, <<"live-main">>))),
    Order = Order0#{submitted_at => Now, mode => live, account_mode => <<"live">>, account_id => AccountID,
                    requested_notional => maps:get(target_notional, Order0, maps:get(requested_notional, Order0, 0.0)),
                    filled_notional => maps:get(filled_notional, Order0, 0.0),
                    fill_reports => maps:get(fill_reports, Order0, [])},
    ID = maps:get(id, Order, order_id(Order)),
    Result =
        case Enabled of
            true ->
                %% Real exchange adapters should submit signed orders and call report_fill/2
                %% when exchange execution reports arrive.
                Order#{id => ID, status => <<"awaiting_fill">>,
                       adapter_status => <<"pending_adapter">>,
                       reason => <<"waiting_exchange_fill_report">>};
            false ->
                Order#{id => ID, status => <<"rejected">>, reason => <<"live_account_disabled">>}
        end,
    Log = #{time => Now,
            action => <<"live_order_request">>,
            account_id => AccountID,
            id => ID,
            status => maps:get(status, Result),
            reason => maps:get(reason, Result),
            req => Req},
    lager:warning("live order request id=~s status=~s reason=~s",
                  [ID, maps:get(status, Result), maps:get(reason, Result)]),
    {reply, sanitize_order(Result), State#state{orders = Orders#{ID => Result}, logs = lists:sublist([Log | Logs], 500)}};
handle_call({report_fill, OrderID0, Fill0}, _From, State = #state{orders = Orders, logs = Logs}) ->
    OrderID = arbiguard_util:to_binary(OrderID0),
    Now = arbiguard_util:now_ms(),
    case maps:get(OrderID, Orders, undefined) of
        undefined ->
            {reply, #{ok => false, reason => <<"order_not_found">>}, State};
        Order0 ->
            Fill = normalize_fill(Fill0),
            FilledNotional = maps:get(filled_notional, Order0, 0.0) + maps:get(filled_notional, Fill, 0.0),
            Requested = maps:get(requested_notional, Order0, maps:get(target_notional, Order0, 0.0)),
            Full = Requested > 0 andalso FilledNotional >= Requested * 0.999,
            Status = case Full of true -> <<"filled">>; false -> <<"partial_filled">> end,
            Reports = [Fill#{time => Now} | maps:get(fill_reports, Order0, [])],
            Order = Order0#{status => Status,
                            filled_notional => FilledNotional,
                            remaining_notional => max(0.0, Requested - FilledNotional),
                            fill_reports => Reports,
                            updated_at => Now},
            maybe_notify_owner(Order),
            Log = #{time => Now,
                    action => <<"live_fill_report">>,
                    id => OrderID,
                    status => Status,
                    filled_notional => FilledNotional,
                    remaining_notional => maps:get(remaining_notional, Order, 0.0)},
            {reply, sanitize_order(Order), State#state{orders = Orders#{OrderID => Order},
                                                       logs = lists:sublist([Log | Logs], 500)}}
    end;
handle_call({report_order_event, ExchangeID0, Event0}, _From, State = #state{orders = Orders, logs = Logs}) ->
    ExchangeID = norm_exchange(ExchangeID0),
    Event = normalize_order_event(ExchangeID, Event0),
    OrderID = maps:get(order_id, Event, <<"">>),
    Now = arbiguard_util:now_ms(),
    State1 = case OrderID of
        <<"">> ->
            State;
        _ ->
            case maps:get(OrderID, Orders, undefined) of
                undefined ->
                    State;
                Order0 ->
                    FilledNotional0 = maps:get(filled_notional, Order0, 0.0),
                    DeltaFilled = maps:get(delta_filled_notional, Event, 0.0),
                    FilledNotional = case DeltaFilled > 0 of
                        true -> FilledNotional0 + DeltaFilled;
                        false -> max(FilledNotional0, maps:get(filled_notional, Event, FilledNotional0))
                    end,
                    Requested = maps:get(requested_notional, Order0, maps:get(target_notional, Order0, 0.0)),
                    Status = normalize_order_status(maps:get(status, Event, <<"">>), Requested, FilledNotional),
                    Order = Order0#{status => Status,
                                    exchange_status => maps:get(status, Event, <<"">>),
                                    filled_notional => FilledNotional,
                                    remaining_notional => max(0.0, Requested - FilledNotional),
                                    last_exchange_event => Event,
                                    updated_at => Now},
                    maybe_notify_owner(Order),
                    State#state{orders = Orders#{OrderID => Order}}
            end
    end,
    Log = #{time => Now,
            action => <<"live_order_event">>,
            exchange => ExchangeID,
            id => OrderID,
            status => maps:get(status, Event, <<"">>),
            filled_notional => maps:get(filled_notional, Event, 0.0),
            event => maps:without([raw], Event)},
    {reply, #{ok => true, event => Event}, State1#state{logs = lists:sublist([Log | Logs], 500)}};
handle_call({report_balance, ExchangeID0, Balance0}, _From, State = #state{balances = Balances, logs = Logs}) ->
    ExchangeID = norm_exchange(ExchangeID0),
    Now = arbiguard_util:now_ms(),
    Balance = (normalize_balance(ExchangeID, Balance0))#{updated_at => Now},
    Log = #{time => Now, action => <<"live_balance_update">>, exchange => ExchangeID,
            balance => maps:without([raw], Balance)},
    {reply, #{ok => true, balance => Balance},
     State#state{balances = Balances#{ExchangeID => Balance}, logs = lists:sublist([Log | Logs], 500)}};
handle_call({report_position, ExchangeID0, Position0}, _From, State = #state{positions = Positions, logs = Logs}) ->
    ExchangeID = norm_exchange(ExchangeID0),
    Now = arbiguard_util:now_ms(),
    Position = (normalize_position(ExchangeID, Position0))#{updated_at => Now},
    Key = position_key(Position),
    Log = #{time => Now, action => <<"live_position_update">>, exchange => ExchangeID,
            symbol => maps:get(symbol, Position, <<"">>), side => maps:get(side, Position, <<"">>),
            position => maps:without([raw], Position)},
    {reply, #{ok => true, position => Position},
     State#state{positions = Positions#{Key => Position}, logs = lists:sublist([Log | Logs], 500)}};
handle_call({report_liquidation, ExchangeID0, Event0}, _From, State = #state{liquidations = Liquidations, logs = Logs}) ->
    ExchangeID = norm_exchange(ExchangeID0),
    Now = arbiguard_util:now_ms(),
    Event = (normalize_liquidation(ExchangeID, Event0))#{time => Now},
    notify_liquidation(Event),
    Log = #{time => Now, action => <<"live_liquidation_event">>, exchange => ExchangeID,
            symbol => maps:get(symbol, Event, <<"">>), event => maps:without([raw], Event)},
    {reply, #{ok => true, event => Event},
     State#state{liquidations = lists:sublist([Event | Liquidations], 100),
                 logs = lists:sublist([Log | Logs], 500)}};
handle_call({report_funding_settlement, PositionID0, Settlement0}, _From, State = #state{logs = Logs}) ->
    PositionID = arbiguard_util:to_binary(PositionID0),
    Now = arbiguard_util:now_ms(),
    SettlementBase = normalize_funding_settlement(Settlement0),
    Settlement = SettlementBase#{
        position_id => PositionID,
        time => maps:get(time, SettlementBase, Now)
    },
    arbiguard_close_executor ! {live_funding_settlement, PositionID, Settlement},
    Log = #{time => Now,
            action => <<"live_funding_settlement">>,
            position_id => PositionID,
            symbol => maps:get(symbol, Settlement, <<"">>),
            exchange => maps:get(exchange, Settlement, <<"">>),
            side => maps:get(side, Settlement, <<"">>),
            funding_pnl => maps:get(funding_pnl, Settlement, 0.0),
            funding_rate => maps:get(funding_rate, Settlement, 0.0)},
    lager:info("live funding settlement position=~s exchange=~s symbol=~s side=~s pnl=~p rate=~p",
               [PositionID, maps:get(exchange, Settlement, <<"">>), maps:get(symbol, Settlement, <<"">>),
                maps:get(side, Settlement, <<"">>), maps:get(funding_pnl, Settlement, 0.0),
                maps:get(funding_rate, Settlement, 0.0)]),
    {reply, #{ok => true, settlement => Settlement}, State#state{logs = lists:sublist([Log | Logs], 500)}};
handle_call({debug_order, Payload0}, _From, State0) ->
    Payload = normalize_debug_payload(Payload0),
    {Result, State} = apply_debug_order(Payload, ensure_debug_balance(Payload, State0)),
    {reply, Result, State};
handle_call({test_order, Payload0}, _From, State0) ->
    Payload = normalize_live_test_payload(Payload0),
    {Result, State} = apply_live_test_order(Payload, State0),
    {reply, Result, State};
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

norm_exchange(V) ->
    string:lowercase(arbiguard_util:to_binary(V)).

truthy(true) -> true;
truthy(<<"true">>) -> true;
truthy(1) -> true;
truthy(_) -> false.

normalize_fill(Fill0) ->
    Fill = case is_map(Fill0) of true -> Fill0; false -> #{} end,
    Fill#{filled_notional => arbiguard_util:to_float(maps:get(filled_notional, Fill, maps:get(notional, Fill, 0.0)), 0.0)}.

normalize_order_event(ExchangeID, Event0) ->
    Event = case is_map(Event0) of true -> Event0; false -> #{} end,
    Event#{
        exchange => ExchangeID,
        order_id => arbiguard_util:to_binary(maps:get(order_id, Event, maps:get(id, Event, <<"">>))),
        client_order_id => arbiguard_util:to_binary(maps:get(client_order_id, Event, maps:get(client_oid, Event, <<"">>))),
        symbol => string:uppercase(arbiguard_util:to_binary(maps:get(symbol, Event, <<"">>))),
        side => string:lowercase(arbiguard_util:to_binary(maps:get(side, Event, <<"">>))),
        status => string:lowercase(arbiguard_util:to_binary(maps:get(status, Event, <<"">>))),
        filled_qty => arbiguard_util:to_float(maps:get(filled_qty, Event, maps:get(fill_qty, Event, 0.0)), 0.0),
        filled_price => arbiguard_util:to_float(maps:get(filled_price, Event, maps:get(price, Event, 0.0)), 0.0),
        filled_notional => arbiguard_util:to_float(maps:get(filled_notional, Event, maps:get(notional, Event, 0.0)), 0.0),
        delta_filled_notional => arbiguard_util:to_float(maps:get(delta_filled_notional, Event, 0.0), 0.0),
        fee => arbiguard_util:to_float(maps:get(fee, Event, 0.0), 0.0)
    }.

normalize_order_status(<<"filled">>, _Requested, _Filled) -> <<"filled">>;
normalize_order_status(<<"closed">>, _Requested, _Filled) -> <<"filled">>;
normalize_order_status(<<"canceled">>, _Requested, _Filled) -> <<"cancelled">>;
normalize_order_status(<<"cancelled">>, _Requested, _Filled) -> <<"cancelled">>;
normalize_order_status(<<"partially_filled">>, _Requested, _Filled) -> <<"partial_filled">>;
normalize_order_status(_Status, Requested, Filled) when Requested > 0, Filled >= Requested * 0.999 -> <<"filled">>;
normalize_order_status(_Status, _Requested, Filled) when Filled > 0 -> <<"partial_filled">>;
normalize_order_status(Status, _Requested, _Filled) when Status =/= <<"">> -> Status;
normalize_order_status(_Status, _Requested, _Filled) -> <<"exchange_update">>.

normalize_balance(ExchangeID, Balance0) ->
    Balance = case is_map(Balance0) of true -> Balance0; false -> #{} end,
    #{exchange => ExchangeID,
      asset => string:uppercase(arbiguard_util:to_binary(maps:get(asset, Balance, <<"USDT">>))),
      wallet_balance => arbiguard_util:to_float(maps:get(wallet_balance, Balance, maps:get(balance, Balance, 0.0)), 0.0),
      available_balance => arbiguard_util:to_float(maps:get(available_balance, Balance, maps:get(available, Balance, 0.0)), 0.0),
      margin_balance => arbiguard_util:to_float(maps:get(margin_balance, Balance, maps:get(equity, Balance, 0.0)), 0.0),
      unrealized_pnl => arbiguard_util:to_float(maps:get(unrealized_pnl, Balance, 0.0), 0.0),
      raw => Balance}.

normalize_position(ExchangeID, Position0) ->
    Position = case is_map(Position0) of true -> Position0; false -> #{} end,
    #{exchange => ExchangeID,
      symbol => string:uppercase(arbiguard_util:to_binary(maps:get(symbol, Position, <<"">>))),
      side => string:lowercase(arbiguard_util:to_binary(maps:get(side, Position, <<"">>))),
      qty => arbiguard_util:to_float(maps:get(qty, Position, maps:get(position_amt, Position, 0.0)), 0.0),
      entry_price => arbiguard_util:to_float(maps:get(entry_price, Position, 0.0), 0.0),
      mark_price => arbiguard_util:to_float(maps:get(mark_price, Position, 0.0), 0.0),
      liquidation_price => arbiguard_util:to_float(maps:get(liquidation_price, Position, 0.0), 0.0),
      unrealized_pnl => arbiguard_util:to_float(maps:get(unrealized_pnl, Position, 0.0), 0.0),
      margin => arbiguard_util:to_float(maps:get(margin, Position, 0.0), 0.0),
      raw => Position}.

normalize_liquidation(ExchangeID, Event0) ->
    Event = case is_map(Event0) of true -> Event0; false -> #{} end,
    #{exchange => ExchangeID,
      symbol => string:uppercase(arbiguard_util:to_binary(maps:get(symbol, Event, <<"">>))),
      side => string:lowercase(arbiguard_util:to_binary(maps:get(side, Event, <<"">>))),
      event_type => arbiguard_util:to_binary(maps:get(event_type, Event, <<"liquidation_or_margin_call">>)),
      mark_price => arbiguard_util:to_float(maps:get(mark_price, Event, 0.0), 0.0),
      liquidation_price => arbiguard_util:to_float(maps:get(liquidation_price, Event, 0.0), 0.0),
      raw => Event}.

position_key(Position) ->
    <<(maps:get(exchange, Position, <<"">>))/binary, "|",
      (maps:get(symbol, Position, <<"">>))/binary, "|",
      (maps:get(side, Position, <<"">>))/binary>>.

notify_liquidation(Event) ->
    arbiguard_open_executor ! {live_liquidation_event, Event},
    arbiguard_close_executor ! {live_liquidation_event, Event},
    ok.

normalize_funding_settlement(Settlement0) ->
    Settlement = case is_map(Settlement0) of true -> Settlement0; false -> #{} end,
    Settlement#{
        exchange => norm_exchange(maps:get(exchange, Settlement, <<"">>)),
        symbol => string:uppercase(arbiguard_util:to_binary(maps:get(symbol, Settlement, <<"">>))),
        side => string:lowercase(arbiguard_util:to_binary(maps:get(side, Settlement, <<"">>))),
        funding_pnl => arbiguard_util:to_float(maps:get(funding_pnl, Settlement, maps:get(amount, Settlement, 0.0)), 0.0),
        funding_rate => arbiguard_util:to_float(maps:get(funding_rate, Settlement, 0.0), 0.0),
        funding_time => arbiguard_util:to_int(maps:get(funding_time, Settlement, maps:get(time, Settlement, 0)), 0)
    }.

maybe_notify_owner(Order) ->
    case maps:get(owner_pid, Order, undefined) of
        Pid when is_pid(Pid) -> Pid ! {live_order_update, sanitize_order(Order)};
        _ -> ok
    end.

sanitize_order(Order) ->
    maps:without([owner_pid, req, opportunity, position], Order).

order_id(Order) ->
    Symbol = maps:get(symbol, Order, <<"UNKNOWN">>),
    Long = maps:get(long_exchange, Order, <<"long">>),
    Short = maps:get(short_exchange, Order, <<"short">>),
    <<Symbol/binary, "|", Long/binary, "|", Short/binary, "|", (integer_to_binary(arbiguard_util:now_ms()))/binary>>.

normalize_debug_payload(Payload0) ->
    Payload = case is_map(Payload0) of true -> Payload0; false -> #{} end,
    Payload#{
        action => string:lowercase(arbiguard_util:to_binary(maps:get(action, Payload, <<"open">>))),
        exchange => norm_exchange(maps:get(exchange, Payload, <<"binance">>)),
        symbol => string:uppercase(arbiguard_util:to_binary(maps:get(symbol, Payload, <<"BTCUSDT">>))),
        side => string:lowercase(arbiguard_util:to_binary(maps:get(side, Payload, <<"long">>))),
        notional => arbiguard_util:to_float(maps:get(notional, Payload, 100), 100),
        price => arbiguard_util:to_float(maps:get(price, Payload, 1), 1),
        leverage => max(1.0, arbiguard_util:to_float(maps:get(leverage, Payload, 10), 10)),
        taker_fee_rate => arbiguard_util:to_float(maps:get(taker_fee_rate, Payload, 0.0006), 0.0006),
        order_id => arbiguard_util:to_binary(maps:get(order_id, Payload, <<"">>))
    }.

normalize_live_test_payload(Payload0) ->
    Payload = case is_map(Payload0) of true -> Payload0; false -> #{} end,
    Payload#{
        action => string:lowercase(arbiguard_util:to_binary(maps:get(action, Payload, <<"open">>))),
        exchange => norm_exchange(maps:get(exchange, Payload, <<"binance">>)),
        symbol => string:uppercase(arbiguard_util:to_binary(maps:get(symbol, Payload, <<"BTCUSDT">>))),
        side => string:lowercase(arbiguard_util:to_binary(maps:get(side, Payload, <<"long">>))),
        order_type => string:uppercase(arbiguard_util:to_binary(maps:get(order_type, Payload, <<"LIMIT">>))),
        notional => arbiguard_util:to_float(maps:get(notional, Payload, 0), 0),
        quantity => arbiguard_util:to_float(maps:get(quantity, Payload, 0), 0),
        price => arbiguard_util:to_float(maps:get(price, Payload, 0), 0),
        leverage => max(1.0, arbiguard_util:to_float(maps:get(leverage, Payload, 1), 1)),
        reduce_only => truthy(maps:get(reduce_only, Payload, false)),
        client_order_id => arbiguard_util:to_binary(maps:get(client_order_id, Payload, <<"">>)),
        confirm => arbiguard_util:to_binary(maps:get(confirm, Payload, <<"">>))
    }.

apply_live_test_order(Payload = #{exchange := Exchange}, State = #state{enabled = Enabled, tokens = Tokens, logs = Logs}) ->
    Now = arbiguard_util:now_ms(),
    Token = maps:get(Exchange, Tokens, undefined),
    Result0 =
        case {Enabled, Token, maps:get(confirm, Payload, <<"">>)} of
            {false, _, _} ->
                #{ok => false, status => <<"rejected">>, reason => <<"live_account_disabled">>};
            {_, undefined, _} ->
                #{ok => false, status => <<"rejected">>, reason => <<"live_token_not_configured">>};
            {_, _, <<"LIVE">>} ->
                arbiguard_live_adapter:test_order(Payload, Token);
            {_, _, _} ->
                #{ok => false, status => <<"rejected">>, reason => <<"confirm_live_required">>}
        end,
    Result = Result0#{time => Now, exchange => Exchange, symbol => maps:get(symbol, Payload, <<"">>)},
    Log = #{time => Now,
            action => <<"live_api_test_order">>,
            exchange => Exchange,
            symbol => maps:get(symbol, Payload, <<"">>),
            side => maps:get(side, Payload, <<"">>),
            status => maps:get(status, Result, <<"unknown">>),
            reason => maps:get(reason, Result, <<"">>),
            result => maps:without([api_secret, secret, passphrase], Result)},
    lager:warning("live api test exchange=~s symbol=~s status=~s reason=~s",
                  [Exchange, maps:get(symbol, Payload, <<"">>),
                   maps:get(status, Result, <<"unknown">>),
                   maps:get(reason, Result, <<"">>)]),
    {Result, State#state{logs = lists:sublist([Log | Logs], 500)}}.

ensure_debug_balance(#{exchange := Exchange}, State = #state{debug_balances = Balances}) ->
    case maps:is_key(Exchange, Balances) of
        true -> State;
        false -> State#state{debug_balances = Balances#{Exchange => 2500.0}}
    end.

apply_debug_order(#{action := <<"open">>} = Payload, State) ->
    debug_open(Payload, State);
apply_debug_order(#{action := <<"close">>} = Payload, State) ->
    debug_close(Payload, State);
apply_debug_order(#{action := <<"cancel">>} = Payload, State = #state{logs = Logs}) ->
    Now = arbiguard_util:now_ms(),
    OrderID0 = maps:get(order_id, Payload, <<"">>),
    OrderID = case OrderID0 of <<"">> -> debug_order_id(Payload, Now); _ -> OrderID0 end,
    Log = (debug_log(Payload, Now))#{action => <<"debug_cancel">>, order_id => OrderID, status => <<"cancelled">>},
    {#{ok => true, action => <<"cancel">>, order_id => OrderID, balances => State#state.debug_balances,
       positions => maps:values(State#state.debug_positions)},
     State#state{logs = lists:sublist([Log | Logs], 500)}};
apply_debug_order(Payload, State) ->
    {#{ok => false, reason => <<"unsupported_debug_action">>, payload => Payload}, State}.

debug_open(Payload, State = #state{debug_balances = Balances0, debug_positions = Positions0, logs = Logs}) ->
    Now = arbiguard_util:now_ms(),
    Exchange = maps:get(exchange, Payload),
    Notional = maps:get(notional, Payload),
    Price = maps:get(price, Payload),
    Leverage = maps:get(leverage, Payload),
    Fee = Notional * maps:get(taker_fee_rate, Payload),
    Margin = Notional / Leverage,
    Need = Margin + Fee,
    Balance0 = maps:get(Exchange, Balances0, 0.0),
    case Price > 0 andalso Balance0 >= Need of
        false ->
            Log = (debug_log(Payload, Now))#{action => <<"debug_open_rejected">>, status => <<"rejected">>,
                                             reason => <<"insufficient_balance_or_bad_price">>},
            {#{ok => false, reason => <<"insufficient_balance_or_bad_price">>, balance => Balance0, need => Need},
             State#state{logs = lists:sublist([Log | Logs], 500)}};
        true ->
            ID = debug_order_id(Payload, Now),
            Qty = Notional / Price,
            Position = #{id => ID,
                         exchange => Exchange,
                         symbol => maps:get(symbol, Payload),
                         side => maps:get(side, Payload),
                         notional => Notional,
                         qty => Qty,
                         entry_price => Price,
                         current_price => Price,
                         leverage => Leverage,
                         margin => Margin,
                         open_fee => Fee,
                         unrealized_pnl => 0.0,
                         opened_at => Now,
                         updated_at => Now},
            Balances = Balances0#{Exchange => Balance0 - Need},
            Log = (debug_log(Payload, Now))#{action => <<"debug_open">>, order_id => ID, status => <<"filled">>,
                                             fee => Fee, margin => Margin, qty => Qty},
            {#{ok => true, action => <<"open">>, position => Position, balance_before => Balance0,
               balance_after => maps:get(Exchange, Balances), balances => Balances,
               positions => maps:values(Positions0#{ID => Position})},
             State#state{debug_balances = Balances,
                         debug_positions = Positions0#{ID => Position},
                         logs = lists:sublist([Log | Logs], 500)}}
    end.

debug_close(Payload, State = #state{debug_balances = Balances0, debug_positions = Positions0, logs = Logs}) ->
    Now = arbiguard_util:now_ms(),
    case find_debug_position(Payload, Positions0) of
        undefined ->
            Log = (debug_log(Payload, Now))#{action => <<"debug_close_rejected">>, status => <<"rejected">>,
                                             reason => <<"position_not_found">>},
            {#{ok => false, reason => <<"position_not_found">>, positions => maps:values(Positions0)},
             State#state{logs = lists:sublist([Log | Logs], 500)}};
        {ID, Position} ->
            Exchange = maps:get(exchange, Position),
            Price = maps:get(price, Payload),
            Notional = maps:get(notional, Position, 0.0),
            Qty = maps:get(qty, Position, 0.0),
            Entry = maps:get(entry_price, Position, 0.0),
            Fee = Notional * maps:get(taker_fee_rate, Payload),
            PricePNL = case maps:get(side, Position, <<"long">>) of
                <<"short">> -> (Entry - Price) * Qty;
                _ -> (Price - Entry) * Qty
            end,
            NetPNL = PricePNL - Fee,
            Balance0 = maps:get(Exchange, Balances0, 0.0),
            Balance1 = Balance0 + maps:get(margin, Position, 0.0) + NetPNL,
            Balances = Balances0#{Exchange => Balance1},
            Log = (debug_log(Payload, Now))#{action => <<"debug_close">>, order_id => ID, status => <<"filled">>,
                                             close_fee => Fee, price_pnl => PricePNL, net_pnl => NetPNL},
            {#{ok => true, action => <<"close">>, closed_position => Position#{close_price => Price, net_pnl => NetPNL},
               balance_before => Balance0, balance_after => Balance1, balances => Balances,
               positions => maps:values(maps:remove(ID, Positions0))},
             State#state{debug_balances = Balances,
                         debug_positions = maps:remove(ID, Positions0),
                         logs = lists:sublist([Log | Logs], 500)}}
    end.

find_debug_position(#{order_id := OrderID}, Positions) when OrderID =/= <<"">> ->
    case maps:find(OrderID, Positions) of
        {ok, Position} -> {OrderID, Position};
        error -> undefined
    end;
find_debug_position(Payload, Positions) ->
    Exchange = maps:get(exchange, Payload),
    Symbol = maps:get(symbol, Payload),
    Side = maps:get(side, Payload),
    case [{ID, P} || {ID, P} <- maps:to_list(Positions),
                    maps:get(exchange, P, <<"">>) =:= Exchange,
                    maps:get(symbol, P, <<"">>) =:= Symbol,
                    maps:get(side, P, <<"">>) =:= Side] of
        [Hit | _] -> Hit;
        [] -> undefined
    end.

debug_log(Payload, Now) ->
    #{time => Now,
      action => <<"debug">>,
      exchange => maps:get(exchange, Payload),
      symbol => maps:get(symbol, Payload),
      side => maps:get(side, Payload),
      notional => maps:get(notional, Payload),
      price => maps:get(price, Payload)}.

debug_order_id(Payload, Now) ->
    <<(maps:get(exchange, Payload))/binary, "|", (maps:get(symbol, Payload))/binary, "|",
      (maps:get(side, Payload))/binary, "|", (integer_to_binary(Now))/binary>>.
