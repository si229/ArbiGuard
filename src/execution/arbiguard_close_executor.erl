-module(arbiguard_close_executor).
-behaviour(gen_server).

-export([start_link/0, submit_close/2, snapshot/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {orders = #{}, last_submit = 0, ticker_cache = #{}}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

submit_close(Req, Position) ->
    gen_server:call(?MODULE, {submit_close, Req, Position}).

snapshot() ->
    gen_server:call(?MODULE, snapshot).

init([]) ->
    {ok, #state{}}.

handle_call({submit_close, Req, Position}, _From, State = #state{orders = Orders}) ->
    Order = close_plan(Req, Position),
    subscribe_position_symbols(Position),
    NewOrder = Order#{status => <<"waiting_ws_ticker">>, req => Req, position => Position},
    State1 = maybe_dispatch_ready_orders(State#state{orders = Orders#{maps:get(id, NewOrder) => NewOrder},
                                                     last_submit = arbiguard_util:now_ms()}),
    {reply, public_order(maps:get(maps:get(id, NewOrder), State1#state.orders, NewOrder)), State1};
handle_call(snapshot, _From, State) ->
    {reply, #{orders => [public_order(O) || O <- maps:values(State#state.orders)],
              last_submit => State#state.last_submit,
              ticker_cache_size => maps:size(State#state.ticker_cache)}, State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({ticker_update, Row}, State = #state{ticker_cache = Cache}) ->
    Symbol = maps:get(symbol, Row, <<"">>),
    State1 = State#state{ticker_cache = update_symbol_cache(Row, Cache)},
    {noreply, maybe_dispatch_symbol_orders(Symbol, State1)};
handle_info({live_order_update, OrderUpdate}, State = #state{orders = Orders}) ->
    ID = maps:get(id, OrderUpdate, <<"">>),
    Status = maps:get(status, OrderUpdate, <<"">>),
    FinalOrder = merge_order(maps:get(ID, Orders, #{}), OrderUpdate),
    case Status of
        <<"filled">> ->
            unsubscribe_position_symbols(FinalOrder),
            {noreply, State#state{orders = Orders#{ID => FinalOrder#{status => <<"filled_live_close">>}}}};
        <<"partial_filled">> ->
            {noreply, State#state{orders = Orders#{ID => FinalOrder#{status => <<"partial_live_close_continue">>}}}};
        _ ->
            {noreply, State#state{orders = Orders#{ID => FinalOrder}}}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

close_plan(Req0, Position) ->
    Req = arbiguard_calc:normalize_request(Req0),
    Symbol = maps:get(symbol, Position, <<"">>),
    LongEx = maps:get(long_exchange, Position, <<"">>),
    ShortEx = maps:get(short_exchange, Position, <<"">>),
    Account = account_scope(Req, Position),
    #{id => <<(maps:get(id, Account))/binary, "|", (maps:get(mode, Account))/binary, "|",
              Symbol/binary, "|", LongEx/binary, "|", ShortEx/binary, "|close">>,
      status => <<"planned_close">>,
      account_mode => maps:get(mode, Account),
      account_id => maps:get(id, Account),
      symbol => Symbol,
      long_exchange => LongEx,
      short_exchange => ShortEx,
      target_notional => maps:get(notional, Position, maps:get(notional_usdt, Position, 0.0)),
      mode => maps:get(close_order_mode, Req, <<"ioc">>),
      created_at => arbiguard_util:now_ms()}.

subscribe_position_symbols(Position) ->
    Symbol = maps:get(symbol, Position, <<"">>),
    LongEx = maps:get(long_exchange, Position, <<"">>),
    ShortEx = maps:get(short_exchange, Position, <<"">>),
    catch arbiguard_exchange_ticker:subscribe(LongEx, Symbol, close_execution_order),
    catch arbiguard_exchange_ticker:subscribe(ShortEx, Symbol, close_execution_order),
    ok.

unsubscribe_position_symbols(Order) ->
    Symbol = maps:get(symbol, Order, <<"">>),
    LongEx = maps:get(long_exchange, Order, <<"">>),
    ShortEx = maps:get(short_exchange, Order, <<"">>),
    case Symbol =/= <<"">> of
        true ->
            catch arbiguard_exchange_ticker:unsubscribe(LongEx, Symbol, close_execution_order),
            catch arbiguard_exchange_ticker:unsubscribe(ShortEx, Symbol, close_execution_order),
            lager:info("close executor local unsubscribe done symbol=~s long=~s short=~s",
                       [Symbol, LongEx, ShortEx]);
        false ->
            ok
    end,
    ok.

dispatch_close(Req0, Order) ->
    Req = arbiguard_calc:normalize_request(Req0),
    AccountMode = maps:get(account_mode, Order, maps:get(account_mode, Req, <<"paper">>)),
    FinalOrder = case AccountMode of
        <<"live">> ->
            LiveResult = catch arbiguard_live_account:submit_order(Req, Order#{owner_pid => self()}),
            (public_order(Order))#{status => live_status(LiveResult, <<"awaiting_live_close_fill">>),
                                   account_submit_result => LiveResult,
                                   submitted_at => arbiguard_util:now_ms()};
        live ->
            LiveResult = catch arbiguard_live_account:submit_order(Req, Order#{owner_pid => self()}),
            (public_order(Order))#{status => live_status(LiveResult, <<"awaiting_live_close_fill">>),
                                   account_submit_result => LiveResult,
                                   submitted_at => arbiguard_util:now_ms()};
        _ ->
            (public_order(Order))#{status => <<"queued_paper_close">>,
                                   exchange_submit => <<"skipped_paper_account">>,
                                   execution_path => <<"ws_ticker_paper_close_no_exchange_submit">>,
                                   queued_at => arbiguard_util:now_ms()}
    end,
    maybe_unsubscribe_after_dispatch(FinalOrder),
    FinalOrder.

maybe_unsubscribe_after_dispatch(Order) ->
    case maps:get(status, Order, <<"">>) of
        <<"awaiting_live_close_fill">> -> ok;
        <<"partial_live_close_continue">> -> ok;
        <<"submitted_live_rejected">> -> unsubscribe_position_symbols(Order);
        _ -> unsubscribe_position_symbols(Order)
    end.

live_status(LiveResult, Awaiting) when is_map(LiveResult) ->
    case maps:get(status, LiveResult, <<"">>) of
        <<"awaiting_fill">> -> Awaiting;
        <<"partial_filled">> -> <<"partial_live_close_continue">>;
        <<"filled">> -> <<"filled_live_close">>;
        <<"rejected">> -> <<"submitted_live_rejected">>;
        _ -> Awaiting
    end;
live_status(_Other, _Awaiting) ->
    <<"submitted_live_error">>.

maybe_dispatch_ready_orders(State = #state{orders = Orders, ticker_cache = Cache}) ->
    NewOrders = maps:map(fun(_ID, Order) -> maybe_dispatch_order(Order, Cache) end, Orders),
    State#state{orders = NewOrders}.

maybe_dispatch_symbol_orders(Symbol, State = #state{orders = Orders, ticker_cache = Cache}) ->
    NewOrders = maps:map(fun(_ID, Order) ->
        case maps:get(symbol, Order, <<"">>) =:= Symbol of
            true -> maybe_dispatch_order(Order, Cache);
            false -> Order
        end
    end, Orders),
    State#state{orders = NewOrders}.

maybe_dispatch_order(Order, Cache) ->
    case maps:get(status, Order, <<"">>) of
        <<"waiting_ws_ticker">> ->
            maybe_submit_close_from_ticker(Order, Cache);
        <<"partial_live_close_continue">> ->
            maybe_submit_close_from_ticker(Order, Cache);
        _ ->
            Order
    end.

maybe_submit_close_from_ticker(Order, Cache) ->
    Position0 = apply_remaining_notional(maps:get(position, Order, #{}), Order),
    case enrich_close_from_cache(Position0, Cache) of
        {ok, Position} ->
            Req = maps:get(req, Order, #{}),
            lager:info("close executor ws-ready symbol=~s long=~s short=~s long_close=~p short_close=~p long_mark=~p short_mark=~p remaining=~p",
                       [maps:get(symbol, Position, <<"">>), maps:get(long_exchange, Position, <<"">>),
                        maps:get(short_exchange, Position, <<"">>), maps:get(long_close_price, Position, 0),
                        maps:get(short_close_price, Position, 0), maps:get(long_mark_price, Position, 0),
                        maps:get(short_mark_price, Position, 0), maps:get(notional, Position, maps:get(notional_usdt, Position, 0.0))]),
            dispatch_close(Req, (public_order(apply_remaining_order(Order)))#{position => Position});
        wait ->
            Order
    end.

apply_remaining_order(Order) ->
    Remaining = maps:get(remaining_notional, Order, maps:get(target_notional, Order, 0.0)),
    case Remaining > 0 of
        true -> Order#{target_notional => Remaining};
        false -> Order
    end.

apply_remaining_notional(Position, Order) ->
    Remaining = maps:get(remaining_notional, Order, maps:get(notional, Position, maps:get(notional_usdt, Position, 0.0))),
    case Remaining > 0 of
        true -> Position#{notional => Remaining, notional_usdt => Remaining};
        false -> Position
    end.

enrich_close_from_cache(Position, Cache) ->
    Symbol = maps:get(symbol, Position, <<"">>),
    LongEx = maps:get(long_exchange, Position, <<"">>),
    ShortEx = maps:get(short_exchange, Position, <<"">>),
    SymbolRows = maps:get(Symbol, Cache, #{}),
    case {ticker_from_symbol_cache_or_ets(LongEx, Symbol, SymbolRows),
          ticker_from_symbol_cache_or_ets(ShortEx, Symbol, SymbolRows)} of
        {LongRow, ShortRow} when is_map(LongRow), is_map(ShortRow) ->
            %% Close long with bid, close short with ask.
            LongClose = maps:get(bid, LongRow, 0.0),
            ShortClose = maps:get(ask, ShortRow, 0.0),
            case LongClose > 0 andalso ShortClose > 0 of
                true ->
                    {ok, Position#{long_close_price => LongClose,
                                   short_close_price => ShortClose,
                                   long_current_price => LongClose,
                                   short_current_price => ShortClose,
                                   long_trade_mid_price => maps:get(trade_mid_price, LongRow, LongClose),
                                   short_trade_mid_price => maps:get(trade_mid_price, ShortRow, ShortClose),
                                   long_mark_price => maps:get(mark_price, LongRow, LongClose),
                                   short_mark_price => maps:get(mark_price, ShortRow, ShortClose),
                                   long_liquidation_reference_price => maps:get(liquidation_reference_price, LongRow, LongClose),
                                   short_liquidation_reference_price => maps:get(liquidation_reference_price, ShortRow, ShortClose),
                                   execution_price_basis => <<"ws_bid_ask">>,
                                   liquidation_price_basis => <<"mark_price">>,
                                   execution_price_updated_at => min(maps:get(updated_at, LongRow, 0), maps:get(updated_at, ShortRow, 0))}};
                false -> wait
            end;
        _ ->
            wait
    end.

ticker_from_symbol_cache_or_ets(Exchange, Symbol, SymbolRows) ->
    case maps:get(Exchange, SymbolRows, undefined) of
        undefined ->
            case arbiguard_ets:get_ticker(Exchange, Symbol) of
                {ok, Row} -> Row;
                _ -> undefined
            end;
        Row ->
            Row
    end.

public_order(Order) ->
    maps:without([req, position], Order).

merge_order(Old, Update) ->
    maps:merge(Old, Update).

account_scope(Req, Position) ->
    Mode0 = maps:get(account_mode, Req, maps:get(account_mode, Position, <<"paper">>)),
    Mode = case Mode0 of live -> <<"live">>; paper -> <<"paper">>; _ -> arbiguard_util:to_binary(Mode0) end,
    DefaultID = case Mode of <<"live">> -> <<"live-main">>; _ -> <<"paper-main">> end,
    #{mode => Mode, id => arbiguard_util:to_binary(maps:get(account_id, Req, maps:get(account_id, Position, DefaultID)))}.

update_symbol_cache(Row, Cache) ->
    Symbol = maps:get(symbol, Row, <<"">>),
    Exchange = maps:get(exchange, Row, <<"">>),
    SymbolRows0 = maps:get(Symbol, Cache, #{}),
    Cache#{Symbol => SymbolRows0#{Exchange => Row}}.
