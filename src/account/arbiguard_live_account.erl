-module(arbiguard_live_account).
-behaviour(gen_server).

-export([start_link/0, snapshot/0, set_exchange_token/2, get_exchange_token/1,
         submit_order/2, report_fill/2, report_funding_settlement/2, set_enabled/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {enabled = false, tokens = #{}, orders = #{}, logs = []}).

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

report_funding_settlement(PositionID, Settlement) ->
    gen_server:call(?MODULE, {report_funding_settlement, PositionID, Settlement}).

init([]) ->
    {ok, #state{}}.

handle_call(snapshot, _From, State = #state{enabled = Enabled, tokens = Tokens, orders = Orders, logs = Logs}) ->
    {reply, #{enabled => Enabled,
              token_exchanges => maps:keys(Tokens),
              orders => maps:values(Orders),
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
