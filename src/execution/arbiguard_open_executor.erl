-module(arbiguard_open_executor).
-behaviour(gen_server).

-export([start_link/0, notify_opportunities/2, submit_order/2, snapshot/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {orders = #{}, last_opportunities = [], last_notify = 0, ticker_cache = #{}}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

notify_opportunities(Req, Result) ->
    gen_server:cast(?MODULE, {opportunities, Req, Result}).

submit_order(Req, Opportunity) ->
    gen_server:call(?MODULE, {submit_order, Req, Opportunity}).

snapshot() ->
    gen_server:call(?MODULE, snapshot).

init([]) ->
    {ok, #state{}}.

handle_call({submit_order, Req, Opportunity}, _From, State) ->
    {Order, NewState} = create_order(Req, Opportunity, State),
    {reply, Order, NewState};
handle_call(snapshot, _From, State) ->
    {reply, #{orders => [public_order(O) || O <- maps:values(State#state.orders)],
              last_opportunities => State#state.last_opportunities,
              last_notify => State#state.last_notify,
              ticker_cache_size => maps:size(State#state.ticker_cache)}, State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({opportunities, Req, Result}, State) ->
    Opportunities = maps:get(opportunities, Result, []),
    NewState = lists:foldl(fun(Op, Acc) -> maybe_create_execution_order(Req, Op, Acc) end, State, Opportunities),
    {noreply, NewState#state{last_opportunities = Opportunities, last_notify = arbiguard_util:now_ms()}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({ticker_update, Row}, State = #state{ticker_cache = Cache}) ->
    Key = ticker_key(Row),
    State1 = State#state{ticker_cache = Cache#{Key => Row}},
    {noreply, maybe_dispatch_ready_orders(State1)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_create_execution_order(Req, Op, State = #state{orders = Orders}) ->
    MinProfit = maps:get(min_execution_profit_usdt, arbiguard_calc:normalize_request(Req), 5.0),
    Req1 = arbiguard_calc:normalize_request(Req),
    ID = order_key(Req1, Op),
    case maps:get(estimated_net_profit, Op, 0) >= MinProfit andalso not maps:is_key(ID, Orders) of
        true ->
            subscribe_order_symbols(Op),
            Order = (order_plan(Req1, Op))#{status => <<"waiting_ws_ticker">>,
                                           req => Req1,
                                           opportunity => Op},
            maybe_dispatch_ready_orders(State#state{orders = Orders#{ID => Order}});
        false ->
            State
    end.

create_order(Req, Op, State = #state{orders = Orders}) ->
    Req1 = arbiguard_calc:normalize_request(Req),
    subscribe_order_symbols(Op),
    Order = (order_plan(Req1, Op))#{status => <<"waiting_ws_ticker">>,
                                   req => Req1,
                                   opportunity => Op},
    State1 = maybe_dispatch_ready_orders(State#state{orders = Orders#{maps:get(id, Order) => Order}}),
    {public_order(maps:get(maps:get(id, Order), State1#state.orders, Order)), State1}.

order_plan(Req0, Op) ->
    Req = arbiguard_calc:normalize_request(Req0),
    Notional = maps:get(suggested_notional, Op, maps:get(execution_notional_usdt, Req, 200.0)),
    LongPrice = maps:get(long_price, Op, 0.0),
    ShortPrice = maps:get(short_price, Op, 0.0),
    Account = account_scope(Req),
    #{id => order_key(Req, Op),
      status => <<"planned_open">>,
      account_mode => maps:get(mode, Account),
      account_id => maps:get(id, Account),
      symbol => maps:get(symbol, Op),
      long_exchange => maps:get(long_exchange, Op),
      short_exchange => maps:get(short_exchange, Op),
      target_notional => Notional,
      long_target_qty => safe_div(Notional, LongPrice),
      short_target_qty => safe_div(Notional, ShortPrice),
      mode => maps:get(execution_order_mode, Req, <<"fok">>),
      expected_net_profit => maps:get(estimated_net_profit, Op, 0.0),
      created_at => arbiguard_util:now_ms()}.

subscribe_order_symbols(Op) ->
    Symbol = maps:get(symbol, Op),
    LongEx = maps:get(long_exchange, Op),
    ShortEx = maps:get(short_exchange, Op),
    catch arbiguard_exchange_ticker:subscribe(LongEx, Symbol, open_execution_order),
    catch arbiguard_exchange_ticker:subscribe(ShortEx, Symbol, open_execution_order),
    ok.

dispatch_order(Req0, Order, Op) ->
    Req = arbiguard_calc:normalize_request(Req0),
    AccountMode = maps:get(account_mode, Order, maps:get(account_mode, Req, <<"paper">>)),
    case AccountMode of
        <<"live">> ->
            LiveResult = catch arbiguard_live_account:submit_order(Req, Order),
            public_order(Order#{status => <<"submitted_live_open">>, account_submit_result => LiveResult,
                                submitted_at => arbiguard_util:now_ms()});
        live ->
            LiveResult = catch arbiguard_live_account:submit_order(Req, Order),
            public_order(Order#{status => <<"submitted_live_open">>, account_submit_result => LiveResult,
                                submitted_at => arbiguard_util:now_ms()});
        _ ->
            _ = catch arbiguard_state:apply_open_order(Req, Order, Op),
            (public_order(Order))#{status => <<"filled_paper_open">>,
                                   exchange_submit => <<"skipped_paper_account">>,
                                   execution_path => <<"ws_ticker_paper_filled_no_exchange_submit">>,
                                   filled_at => arbiguard_util:now_ms()}
    end.

maybe_dispatch_ready_orders(State = #state{orders = Orders, ticker_cache = Cache}) ->
    NewOrders = maps:map(fun(_ID, Order) -> maybe_dispatch_order(Order, Cache) end, Orders),
    State#state{orders = NewOrders}.

maybe_dispatch_order(Order, Cache) ->
    case maps:get(status, Order, <<"">>) of
        <<"waiting_ws_ticker">> ->
            Op0 = maps:get(opportunity, Order, #{}),
            Req = arbiguard_calc:normalize_request(maps:get(req, Order, #{})),
            case enrich_opportunity_from_cache(Op0, Req, Cache) of
                {ok, Op} ->
                    lager:info("open executor ws-ready symbol=~s long=~s short=~s long_price=~p short_price=~p long_mark=~p short_mark=~p",
                               [maps:get(symbol, Op, <<"">>), maps:get(long_exchange, Op, <<"">>),
                                maps:get(short_exchange, Op, <<"">>), maps:get(long_price, Op, 0),
                                maps:get(short_price, Op, 0), maps:get(long_mark_price, Op, 0),
                                maps:get(short_mark_price, Op, 0)]),
                    dispatch_order(Req, public_order(Order), Op);
                wait ->
                    Order
            end;
        _ ->
            Order
    end.

enrich_opportunity_from_cache(Op, Req, Cache) ->
    Symbol = maps:get(symbol, Op, <<"">>),
    LongEx = maps:get(long_exchange, Op, <<"">>),
    ShortEx = maps:get(short_exchange, Op, <<"">>),
    case {ticker_from_cache_or_ets(LongEx, Symbol, Cache), ticker_from_cache_or_ets(ShortEx, Symbol, Cache)} of
        {LongRow, ShortRow} when is_map(LongRow), is_map(ShortRow) ->
            LongPrice = maps:get(ask, LongRow, 0.0),
            ShortPrice = maps:get(bid, ShortRow, 0.0),
            case LongPrice > 0 andalso ShortPrice > 0 of
                true ->
                    Op1 = Op#{long_price => LongPrice,
                               short_price => ShortPrice,
                               long_current_price => LongPrice,
                               short_current_price => ShortPrice,
                               long_trade_mid_price => maps:get(trade_mid_price, LongRow, LongPrice),
                               short_trade_mid_price => maps:get(trade_mid_price, ShortRow, ShortPrice),
                               long_mark_price => maps:get(mark_price, LongRow, LongPrice),
                               short_mark_price => maps:get(mark_price, ShortRow, ShortPrice),
                               long_liquidation_reference_price => maps:get(liquidation_reference_price, LongRow, LongPrice),
                               short_liquidation_reference_price => maps:get(liquidation_reference_price, ShortRow, ShortPrice),
                               execution_price_basis => <<"ws_bid_ask_or_latest_ets">>,
                               liquidation_price_basis => <<"mark_price">>,
                               execution_price_updated_at => min(maps:get(updated_at, LongRow, 0), maps:get(updated_at, ShortRow, 0))},
                    require_profitable(refresh_expected_profit(Op1, Req), Req);
                false -> wait
            end;
        _ ->
            wait
    end.

ticker_from_cache_or_ets(Exchange, Symbol, Cache) ->
    case maps:get({Exchange, Symbol}, Cache, undefined) of
        undefined ->
            case arbiguard_ets:get_ticker(Exchange, Symbol) of
                {ok, Row} -> Row;
                _ -> undefined
            end;
        Row ->
            Row
    end.

refresh_expected_profit(Op, Req) ->
    LongPrice = maps:get(long_price, Op, 0.0),
    ShortPrice = maps:get(short_price, Op, 0.0),
    Mid = case LongPrice > 0 andalso ShortPrice > 0 of true -> (LongPrice + ShortPrice) / 2; false -> 0.0 end,
    PriceGap = case Mid > 0 of true -> (ShortPrice - LongPrice) / Mid; false -> 0.0 end,
    FundingEdge = maps:get(funding_edge_return, Op, 0.0),
    FeeRate = maps:get(long_fee_rate, Op, 0.0005) + maps:get(short_fee_rate, Op, 0.0005),
    ExpectedNetReturn = FundingEdge + PriceGap - FeeRate * 2,
    Notional = maps:get(suggested_notional, Op, maps:get(execution_notional_usdt, Req, 200.0)),
    Op#{price_gap_return => PriceGap,
        basis_rate => PriceGap,
        expected_gross_return => FundingEdge + PriceGap,
        expected_net_return => ExpectedNetReturn,
        estimated_net_profit => Notional * ExpectedNetReturn,
        suggested_notional => Notional}.

require_profitable(Op, Req) ->
    MinProfit = maps:get(min_execution_profit_usdt, Req, 5.0),
    case maps:get(estimated_net_profit, Op, 0.0) >= MinProfit of
        true -> {ok, Op};
        false ->
            lager:info("open executor wait profit_below_threshold symbol=~s profit=~p min=~p",
                       [maps:get(symbol, Op, <<"">>), maps:get(estimated_net_profit, Op, 0.0), MinProfit]),
            wait
    end.

public_order(Order) ->
    maps:without([req, opportunity], Order).

order_key(Req, Op) ->
    Account = account_scope(Req),
    <<(maps:get(id, Account))/binary, "|", (maps:get(mode, Account))/binary, "|",
      (maps:get(symbol, Op))/binary, "|", (maps:get(long_exchange, Op))/binary, "|",
      (maps:get(short_exchange, Op))/binary>>.

account_scope(Req) ->
    Mode0 = maps:get(account_mode, Req, <<"paper">>),
    Mode = case Mode0 of live -> <<"live">>; paper -> <<"paper">>; _ -> arbiguard_util:to_binary(Mode0) end,
    DefaultID = case Mode of <<"live">> -> <<"live-main">>; _ -> <<"paper-main">> end,
    #{mode => Mode, id => arbiguard_util:to_binary(maps:get(account_id, Req, DefaultID))}.

safe_div(_A, B) when B =< 0 -> 0.0;
safe_div(A, B) -> A / B.

ticker_key(Row) ->
    {maps:get(exchange, Row, <<"">>), maps:get(symbol, Row, <<"">>)}.
