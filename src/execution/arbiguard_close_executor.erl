-module(arbiguard_close_executor).
-behaviour(gen_server).

-export([start_link/0, start_link/1, account_name/1,
         track_position/2, track_position/3, reset/0, reset/1, snapshot/0, snapshot/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {account_id = <<"paper-main">>, account_mode = <<"paper">>,
                orders = #{}, last_submit = 0, ticker_cache = #{}}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_link(Config) ->
    AccountID = maps:get(account_id, Config, <<"paper-main">>),
    Name = maps:get(name, Config, account_name(AccountID)),
    gen_server:start_link({local, Name}, ?MODULE, [Config], []).

account_name(AccountID) ->
    list_to_atom("arbiguard_close_executor_" ++ safe_atom_part(AccountID)).

track_position(Req, Position) ->
    gen_server:call(?MODULE, {track_position, Req, Position}).

track_position(Executor, Req, Position) ->
    gen_server:call(Executor, {track_position, Req, Position}).

reset() ->
    gen_server:call(?MODULE, reset).

reset(Executor) ->
    gen_server:call(Executor, reset).

snapshot() ->
    gen_server:call(?MODULE, snapshot).

snapshot(Executor) ->
    gen_server:call(Executor, snapshot).

init([]) ->
    {ok, #state{}};
init([Config]) ->
    {ok, #state{account_id = maps:get(account_id, Config, <<"paper-main">>),
                account_mode = maps:get(account_mode, Config, <<"paper">>)}}.

handle_call({track_position, Req0, Position0}, _From, State) ->
    {PublicOrder, State1} = add_tracked_position(Req0, Position0, State),
    {reply, PublicOrder, State1};
handle_call(reset, _From, State = #state{orders = Orders}) ->
    _ = [unsubscribe_position_symbols(Order) || {_ID, Order} <- maps:to_list(Orders)],
    {reply, #{ok => true, cleared_close_orders => maps:size(Orders)},
     State#state{orders = #{}, last_submit = 0, ticker_cache = #{}}};
handle_call(snapshot, _From, State) ->
    {reply, #{account_id => State#state.account_id,
              account_mode => State#state.account_mode,
              orders => [public_order(O) || O <- maps:values(State#state.orders)],
              last_submit => State#state.last_submit,
              ticker_cache_size => maps:size(State#state.ticker_cache)}, State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({track_position, Req0, Position0}, State) ->
    {_PublicOrder, State1} = add_tracked_position(Req0, Position0, State),
    {noreply, State1};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({ticker_update, Row}, State = #state{ticker_cache = Cache}) ->
    Symbol = maps:get(symbol, Row, <<"">>),
    State1 = State#state{ticker_cache = update_symbol_cache(Row, Cache)},
    {noreply, maybe_dispatch_symbol_orders(Symbol, State1)};
handle_info({live_order_update, OrderUpdate}, State = #state{orders = Orders}) ->
    ID = maps:get(parent_id, OrderUpdate, maps:get(id, OrderUpdate, <<"">>)),
    Status = maps:get(status, OrderUpdate, <<"">>),
    Parent0 = maps:get(ID, Orders, #{}),
    Parent = apply_live_child_update(Parent0, OrderUpdate),
    case Status of
        <<"filled">> -> maybe_finish_or_continue(Parent, State);
        <<"partial_filled">> -> maybe_finish_or_continue(Parent, State);
        _ -> {noreply, State#state{orders = Orders#{ID => Parent}}}
    end;
handle_info({live_funding_settlement, PositionID, Settlement}, State = #state{orders = Orders}) ->
    {FoundID, FoundOrder} = find_order_by_position(PositionID, Settlement, Orders),
    case FoundID of
        undefined ->
            lager:warning("close executor live funding settlement unmatched position=~s exchange=~s symbol=~s side=~s",
                          [PositionID, maps:get(exchange, Settlement, <<"">>),
                           maps:get(symbol, Settlement, <<"">>), maps:get(side, Settlement, <<"">>)]),
            {noreply, State};
        _ ->
            Position0 = maps:get(position, FoundOrder, #{}),
            Position = evaluate_position_strategy(apply_live_funding_settlement(Position0, Settlement),
                                                  maps:get(req, FoundOrder, #{})),
            Order1 = FoundOrder#{position => maps:without([should_close], Position),
                                 close_rule => maps:get(close_rule, Position, <<"watch">>)},
            Order2 = case maps:get(should_close, Position, false) of
                true ->
                    Req = arbiguard_calc:normalize_request(maps:get(req, FoundOrder, #{})),
                    CloseOrder = close_plan(Req, Position),
                    dispatch_close(Req, CloseOrder#{status => <<"waiting_ws_ticker">>,
                                                    req => Req,
                                                    position => Position,
                                                    close_rule => maps:get(close_rule, Position, <<"live_funding_strategy_close">>)});
                false -> Order1
            end,
            {noreply, State#state{orders = Orders#{FoundID => Order2}}}
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

tracking_plan(Req0, Position) ->
    Req = arbiguard_calc:normalize_request(Req0),
    Symbol = maps:get(symbol, Position, <<"">>),
    LongEx = maps:get(long_exchange, Position, <<"">>),
    ShortEx = maps:get(short_exchange, Position, <<"">>),
    Account = account_scope(Req, Position),
    #{id => <<(maps:get(id, Account))/binary, "|", (maps:get(mode, Account))/binary, "|",
              Symbol/binary, "|", LongEx/binary, "|", ShortEx/binary, "|position">>,
      status => <<"planned_track_position">>,
      account_mode => maps:get(mode, Account),
      account_id => maps:get(id, Account),
      symbol => Symbol,
      long_exchange => LongEx,
      short_exchange => ShortEx,
      target_notional => maps:get(notional, Position, maps:get(notional_usdt, Position, 0.0)),
      mode => <<"monitor">>,
      created_at => arbiguard_util:now_ms()}.

add_tracked_position(Req0, Position0, State = #state{orders = Orders}) ->
    Req = with_state_account(arbiguard_calc:normalize_request(Req0), State),
    Position = Position0#{account_id => maps:get(account_id, Req),
                          account_mode => maps:get(account_mode, Req)},
    Order = tracking_plan(Req, Position),
    subscribe_position_symbols(Position),
    register_position_owner(Req, Position),
    NewOrder = Order#{status => <<"tracking_position">>, req => Req, position => Position},
    lager:info("close executor tracking position symbol=~s long=~s short=~s account=~s/~s",
               [maps:get(symbol, Position, <<"">>), maps:get(long_exchange, Position, <<"">>),
                maps:get(short_exchange, Position, <<"">>), maps:get(account_mode, Order, <<"">>),
                maps:get(account_id, Order, <<"">>)]),
    {public_order(NewOrder), State#state{orders = Orders#{maps:get(id, NewOrder) => NewOrder},
                                         last_submit = arbiguard_util:now_ms()}}.

subscribe_position_symbols(Position) ->
    Symbol = maps:get(symbol, Position, <<"">>),
    LongEx = maps:get(long_exchange, Position, <<"">>),
    ShortEx = maps:get(short_exchange, Position, <<"">>),
    catch arbiguard_exchange_ticker:subscribe(LongEx, Symbol, close_execution_order),
    catch arbiguard_exchange_ticker:subscribe(ShortEx, Symbol, close_execution_order),
    ok.

register_position_owner(Req, Position) ->
    AccountID = maps:get(account_id, Req, maps:get(account_id, Position, <<"live-main">>)),
    LongEx = maps:get(long_exchange, Position, <<"">>),
    ShortEx = maps:get(short_exchange, Position, <<"">>),
    LongPosition = Position#{side => <<"long">>, position_side => <<"long">>},
    ShortPosition = Position#{side => <<"short">>, position_side => <<"short">>},
    case LongEx =/= <<"">> of
        true -> arbiguard_ets:put_position_owner(AccountID, LongEx, LongPosition, self());
        false -> ok
    end,
    case ShortEx =/= <<"">> of
        true -> arbiguard_ets:put_position_owner(AccountID, ShortEx, ShortPosition, self());
        false -> ok
    end,
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
            submit_live_child_order(Req, Order, <<"awaiting_live_close_fill">>);
        live ->
            submit_live_child_order(Req, Order, <<"awaiting_live_close_fill">>);
        _ ->
            _ = catch arbiguard_state:apply_close_order(Req, Order, maps:get(position, Order, #{})),
            (public_order(Order))#{status => <<"filled_paper_close">>,
                                   exchange_submit => <<"skipped_paper_account">>,
                                   execution_path => <<"ws_ticker_paper_close_no_exchange_submit">>,
                                   filled_at => arbiguard_util:now_ms()}
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

submit_live_child_order(Req, Order, AwaitingStatus) ->
    ParentID = maps:get(id, Order),
    Remaining = remaining_to_submit(Order),
    ChildID = child_order_id(ParentID),
    ChildOrder = Order#{id => ChildID,
                        parent_id => ParentID,
                        execution_order_id => ParentID,
                        owner_pid => self(),
                        target_notional => Remaining,
                        requested_notional => Remaining},
    register_live_order_owner(Req, ChildOrder),
    LiveResult = catch arbiguard_account_manager:submit_live_order(Req, ChildOrder),
    case live_submit_accepted(LiveResult) of
        true ->
            Pending0 = maps:get(pending_submissions, Order, #{}),
            Pending = Pending0#{ChildID => LiveResult},
            public_order(Order#{status => AwaitingStatus,
                                pending_submissions => Pending,
                                pending_notional => pending_notional(Pending),
                                last_submit_result => LiveResult,
                                submitted_at => arbiguard_util:now_ms()});
        false ->
            public_order(Order#{status => <<"waiting_ws_ticker">>,
                                last_submit_result => LiveResult,
                                last_submit_error => submit_error(LiveResult),
                                submitted_at => arbiguard_util:now_ms()})
    end.

live_submit_accepted(Result) when is_map(Result) ->
    maps:get(status, Result, <<"">>) =:= <<"awaiting_fill">>;
live_submit_accepted(_Result) ->
    false.

submit_error(Result) when is_map(Result) ->
    maps:get(reason, Result, <<"submit_failed">>);
submit_error(_Result) ->
    <<"submit_exception">>.

register_live_order_owner(Req, Order) ->
    AccountID = maps:get(account_id, Req, maps:get(account_id, Order, <<"live-main">>)),
    LongEx = maps:get(long_exchange, Order, <<"">>),
    ShortEx = maps:get(short_exchange, Order, <<"">>),
    case LongEx =/= <<"">> of
        true -> arbiguard_ets:put_order_owner(AccountID, LongEx, Order);
        false -> ok
    end,
    case ShortEx =/= <<"">> of
        true -> arbiguard_ets:put_order_owner(AccountID, ShortEx, Order);
        false -> ok
    end,
    ok.

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
        <<"tracking_position">> ->
            maybe_close_tracked_position(Order, Cache);
        <<"waiting_ws_ticker">> ->
            maybe_submit_close_from_ticker(Order, Cache);
        <<"awaiting_live_close_fill">> ->
            maybe_submit_close_from_ticker(Order, Cache);
        <<"partial_live_close_continue">> ->
            maybe_submit_close_from_ticker(Order, Cache);
        _ ->
            Order
    end.

maybe_close_tracked_position(Order, Cache) ->
    Position0 = maps:get(position, Order, #{}),
    case enrich_close_from_cache(Position0, Cache) of
        {ok, Position1} ->
            Req = arbiguard_calc:normalize_request(maps:get(req, Order, #{})),
            Position = evaluate_position_strategy(maybe_settle_funding(Position1), Req),
            case maps:get(should_close, Position, false) of
                true ->
                    CloseOrder = close_plan(Req, Position),
                    dispatch_close(Req, CloseOrder#{status => <<"waiting_ws_ticker">>,
                                                    req => Req,
                                                    position => Position,
                                                    close_rule => maps:get(close_rule, Position, <<"strategy_close">>)});
                false ->
                    _ = catch arbiguard_state:update_position(Position),
                    Order#{position => maps:without([should_close], Position),
                           close_rule => maps:get(close_rule, Position, <<"watch">>)}
            end;
        wait ->
            Order
    end.

maybe_settle_funding(Position) ->
    case maps:get(account_mode, Position, <<"paper">>) of
        <<"live">> -> recompute_position_pnl(Position);
        live -> recompute_position_pnl(Position);
        _ -> settle_funding(Position)
    end.

maybe_submit_close_from_ticker(Order, Cache) ->
    case remaining_to_submit(Order) > 0 of
        false -> Order;
        true -> maybe_submit_close_from_ticker_1(Order, Cache)
    end.

maybe_submit_close_from_ticker_1(Order, Cache) ->
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
    Remaining = remaining_to_submit(Order),
    case Remaining > 0 of
        true -> Order#{target_notional => Remaining};
        false -> Order
    end.

apply_remaining_notional(Position, Order) ->
    Remaining = remaining_to_submit(Order),
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
            Target = maps:get(notional, Position, maps:get(notional_usdt, Position, 0.0)),
            LongFill = fill_notional(LongRow, bid_levels, bid, Target),
            ShortFill = fill_notional(ShortRow, ask_levels, ask, Target),
            LongClose = maps:get(avg_price, LongFill, 0.0),
            ShortClose = maps:get(avg_price, ShortFill, 0.0),
            LongEnough = maps:get(filled_notional, LongFill, 0.0) >= Target * 0.999,
            ShortEnough = maps:get(filled_notional, ShortFill, 0.0) >= Target * 0.999,
            case Target > 0 andalso LongClose > 0 andalso ShortClose > 0 andalso LongEnough andalso ShortEnough of
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
                                   long_close_depth_notional => maps:get(filled_notional, LongFill, 0.0),
                                   short_close_depth_notional => maps:get(filled_notional, ShortFill, 0.0),
                                   long_close_depth_qty => maps:get(filled_qty, LongFill, 0.0),
                                   short_close_depth_qty => maps:get(filled_qty, ShortFill, 0.0),
                                   execution_price_basis => maps:get(source, LongFill, <<"ws_depth_or_bbo">>),
                                   liquidation_price_basis => <<"mark_price">>,
                                   execution_price_updated_at => min(maps:get(updated_at, LongRow, 0), maps:get(updated_at, ShortRow, 0))}};
                false -> wait
            end;
        _ ->
            wait
    end.

settle_funding(Position0) ->
    Now = arbiguard_util:now_ms(),
    OpenedAt = arbiguard_util:to_int(maps:get(opened_at, Position0, 0), 0),
    Position1 = Position0#{
        long_next_funding_time => advance_funding_time(maps:get(long_next_funding_time, Position0, 0),
                                                       maps:get(long_funding_interval_hours, Position0, 8), OpenedAt),
        short_next_funding_time => advance_funding_time(maps:get(short_next_funding_time, Position0, 0),
                                                        maps:get(short_funding_interval_hours, Position0, 8), OpenedAt)
    },
    Position2 = settle_leg(long, Position1, Now, 0),
    Position3 = settle_leg(short, Position2, Now, 0),
    recompute_position_pnl(Position3).

settle_leg(_Leg, Position, _Now, Count) when Count >= 1000 ->
    Position;
settle_leg(long, Position, Now, Count) ->
    T = arbiguard_util:to_int(maps:get(long_next_funding_time, Position, 0), 0),
    case T > 0 andalso Now >= T of
        true ->
            Notional = f(maps:get(notional, Position, maps:get(notional_usdt, Position, 0)), 0),
            Rate = f(maps:get(long_funding_rate, Position, 0), 0),
            PNL = -Notional * Rate,
            Next = T + interval_ms(maps:get(long_funding_interval_hours, Position, 8)),
            settle_leg(long, Position#{funding_pnl => f(maps:get(funding_pnl, Position, 0), 0) + PNL,
                                       last_funding_settlement_pnl => PNL,
                                       long_funding_settlement_count => maps:get(long_funding_settlement_count, Position, 0) + 1,
                                       last_funding_settled_at => T,
                                       long_next_funding_time => Next}, Now, Count + 1);
        false -> Position
    end;
settle_leg(short, Position, Now, Count) ->
    T = arbiguard_util:to_int(maps:get(short_next_funding_time, Position, 0), 0),
    case T > 0 andalso Now >= T of
        true ->
            Notional = f(maps:get(notional, Position, maps:get(notional_usdt, Position, 0)), 0),
            Rate = f(maps:get(short_funding_rate, Position, 0), 0),
            PNL = Notional * Rate,
            Next = T + interval_ms(maps:get(short_funding_interval_hours, Position, 8)),
            settle_leg(short, Position#{funding_pnl => f(maps:get(funding_pnl, Position, 0), 0) + PNL,
                                        last_funding_settlement_pnl => PNL,
                                        short_funding_settlement_count => maps:get(short_funding_settlement_count, Position, 0) + 1,
                                        last_funding_settled_at => T,
                                        short_next_funding_time => Next}, Now, Count + 1);
        false -> Position
    end.

evaluate_position_strategy(Position0, Req) ->
    Now = arbiguard_util:now_ms(),
    {Threshold, LockRule} = close_threshold(Position0, Now),
    Position = recompute_position_pnl(mark_liquidation_emergency(Position0#{close_threshold => Threshold}, Now)),
    MinProfit = min_close_profit_usdt(Req),
    PriceGapLimit = f(maps:get(price_gap_close_profit_usdt, Req, MinProfit), MinProfit),
    Rules = [
        {emergency_close(Position, Now), emergency_rule(Position, Now)},
        {delist_profit_close(Position, Now), <<"delist_profit_close">>},
        {f(maps:get(unrealized_pnl, Position, 0), 0) >= MinProfit, <<"min_profit_close">>},
        {next_funding_loss_close(Position), next_funding_loss_rule(Position)},
        {after_funding_settlement_close(Position, Req), <<"after_funding_settlement_profit_close">>},
        {f(maps:get(unrealized_pnl, Position, 0), 0) >= PriceGapLimit, <<"price_gap_profit_close">>},
        {progressive_profit_close(Position), LockRule}
    ],
    case first_close_rule(Rules) of
        none -> Position#{should_close => false, close_rule => <<"watch">>};
        Rule -> Position#{should_close => true, close_rule => Rule}
    end.

first_close_rule([]) -> none;
first_close_rule([{true, Rule} | _]) -> Rule;
first_close_rule([_ | Rest]) -> first_close_rule(Rest).

min_close_profit_usdt(Req) ->
    f(maps:get(min_execution_profit_usdt, Req,
               maps:get(price_gap_close_profit_usdt, Req, 10)), 10).

close_threshold(Position, Now) ->
    Next = min_positive(maps:get(long_next_funding_time, Position, 0), maps:get(short_next_funding_time, Position, 0)),
    Start0 = maps:get(last_funding_settled_at, Position, 0),
    Start = case Start0 > 0 of true -> Start0; false -> maps:get(opened_at, Position, 0) end,
    case Next =< Start orelse Next =< Now orelse Start =< 0 of
        true -> {0.50, <<"post_funding_profit_lock">>};
        false ->
            Progress = (Now - Start) / max(1, Next - Start),
            case Progress of
                P when P < 0.2 -> {0.95, <<"profit_lock_95">>};
                P when P < 0.4 -> {0.90, <<"profit_lock_90">>};
                P when P < 0.6 -> {0.85, <<"profit_lock_85">>};
                P when P < 0.8 -> {0.80, <<"profit_lock_80">>};
                _ -> {0.50, <<"profit_lock_50">>}
            end
    end.

progressive_profit_close(Position) ->
    Expected = f(maps:get(expected_net_profit, Position, 0), 0),
    Unrealized = f(maps:get(unrealized_pnl, Position, 0), 0),
    Threshold = max(0.5, f(maps:get(close_threshold, Position, 0.5), 0.5)),
    case Expected =< 0 of
        true -> Unrealized > 0;
        false -> Unrealized >= Expected * Threshold orelse (Threshold =< 0.5 andalso Unrealized > 0)
    end.

next_funding_loss_close(Position) ->
    maps:get(last_funding_settled_at, Position, 0) > 0 andalso
    f(maps:get(last_funding_settlement_pnl, Position, 0), 0) > 0 andalso
    funding_cycle_mismatch(Position) andalso
    next_funding_event_pnl(Position) =< 0 andalso
    progressive_profit_close(Position).

next_funding_loss_rule(Position) ->
    Threshold = f(maps:get(close_threshold, Position, 0.5), 0.5),
    Unrealized = f(maps:get(unrealized_pnl, Position, 0), 0),
    case Threshold of
        T when T >= 0.95 -> <<"next_funding_loss_lock_95">>;
        T when T >= 0.90 -> <<"next_funding_loss_lock_90">>;
        T when T >= 0.85 -> <<"next_funding_loss_lock_85">>;
        T when T >= 0.80 -> <<"next_funding_loss_lock_80">>;
        _ when Unrealized > 0 -> <<"next_funding_loss_profit_close">>;
        _ -> <<"next_funding_loss_watch">>
    end.

after_funding_settlement_close(Position, Req) ->
    maps:get(last_funding_settled_at, Position, 0) > 0 andalso
    not (f(maps:get(last_funding_settlement_pnl, Position, 0), 0) > 0 andalso
         funding_cycle_mismatch(Position) andalso next_funding_event_pnl(Position) =< 0) andalso
    f(maps:get(unrealized_pnl, Position, 0), 0) >= min_close_profit_usdt(Req).

delist_profit_close(Position, Now) ->
    f(maps:get(unrealized_pnl, Position, 0), 0) > 0 andalso
    (delist_within(maps:get(long_delist_time, Position, 0), Now) orelse
     delist_within(maps:get(short_delist_time, Position, 0), Now)).

emergency_close(Position, Now) ->
    Started = maps:get(emergency_started_at, Position, 0),
    Started > 0 andalso (mark_unrealized_pnl(Position) > 0 orelse Now - Started >= 500).

emergency_rule(Position, Now) ->
    case Now - maps:get(emergency_started_at, Position, 0) < 500 andalso mark_unrealized_pnl(Position) > 0 of
        true -> <<"liquidation_hedge_profit_0_5s">>;
        false -> <<"liquidation_hedge_market_1s">>
    end.

mark_liquidation_emergency(Position, Now) ->
    case maps:get(emergency_started_at, Position, 0) > 0 of
        true -> Position;
        false ->
            Position1 = ensure_liquidation_prices(Position),
            case liquidation_risk(Position1) of
                true ->
                    lager:warning("liquidation emergency start symbol=~s long=~s short=~s long_mark=~p long_liq=~p short_mark=~p short_liq=~p",
                                  [maps:get(symbol, Position1, <<"">>), maps:get(long_exchange, Position1, <<"">>),
                                   maps:get(short_exchange, Position1, <<"">>),
                                   maps:get(long_mark_price, Position1, 0), maps:get(long_liquidation_price, Position1, 0),
                                   maps:get(short_mark_price, Position1, 0), maps:get(short_liquidation_price, Position1, 0)]),
                    Position1#{emergency_started_at => Now,
                               emergency_reason => <<"mark_price_near_liquidation">>};
                false ->
                    Position1
            end
    end.

ensure_liquidation_prices(Position) ->
    Leverage = max(1.0, f(maps:get(leverage, Position, 10), 10)),
    LongEntry = f(maps:get(long_entry_price, Position, 0), 0),
    ShortEntry = f(maps:get(short_entry_price, Position, 0), 0),
    LongLiq0 = f(maps:get(long_liquidation_price, Position, 0), 0),
    ShortLiq0 = f(maps:get(short_liquidation_price, Position, 0), 0),
    Position#{long_liquidation_price => choose_positive(LongLiq0, long_liquidation_price(LongEntry, Leverage)),
              short_liquidation_price => choose_positive(ShortLiq0, short_liquidation_price(ShortEntry, Leverage)),
              liquidation_price_basis => maps:get(liquidation_price_basis, Position, <<"mark_price">>)}.

liquidation_risk(Position) ->
    Buffer = f(application:get_env(arbiguard, liquidation_guard_buffer_rate, 0.01), 0.01),
    LongMark = f(maps:get(long_mark_price, Position, maps:get(long_current_price, Position, 0)), 0),
    ShortMark = f(maps:get(short_mark_price, Position, maps:get(short_current_price, Position, 0)), 0),
    LongLiq = f(maps:get(long_liquidation_price, Position, 0), 0),
    ShortLiq = f(maps:get(short_liquidation_price, Position, 0), 0),
    LongRisk = LongMark > 0 andalso LongLiq > 0 andalso LongMark =< LongLiq * (1.0 + Buffer),
    ShortRisk = ShortMark > 0 andalso ShortLiq > 0 andalso ShortMark >= ShortLiq * (1.0 - Buffer),
    LongRisk orelse ShortRisk.

long_liquidation_price(Entry, Leverage) ->
    case Entry > 0 andalso Leverage > 0 of
        true -> Entry * max(0.0, 1.0 - 1.0 / Leverage);
        false -> 0.0
    end.

short_liquidation_price(Entry, Leverage) ->
    case Entry > 0 andalso Leverage > 0 of
        true -> Entry * (1.0 + 1.0 / Leverage);
        false -> 0.0
    end.

choose_positive(Value, _Fallback) when Value > 0 -> Value;
choose_positive(_Value, Fallback) -> Fallback.

recompute_position_pnl(Position) ->
    PricePNL = long_leg_pnl(Position) + short_leg_pnl(Position),
    FundingPNL = f(maps:get(funding_pnl, Position, 0), 0),
    CloseFee = close_fee(Position),
    Position#{price_pnl => PricePNL,
              close_fee => CloseFee,
              unrealized_pnl => PricePNL + FundingPNL - CloseFee,
              updated_at => arbiguard_util:now_ms()}.

long_leg_pnl(Position) ->
    (f(maps:get(long_current_price, Position, maps:get(long_close_price, Position, 0)), 0) -
     f(maps:get(long_entry_price, Position, 0), 0)) * f(maps:get(long_qty, Position, 0), 0).

short_leg_pnl(Position) ->
    (f(maps:get(short_entry_price, Position, 0), 0) -
     f(maps:get(short_current_price, Position, maps:get(short_close_price, Position, 0)), 0)) *
     f(maps:get(short_qty, Position, 0), 0).

mark_unrealized_pnl(Position) ->
    LongMarkPNL = (f(maps:get(long_mark_price, Position, maps:get(long_current_price, Position, 0)), 0) -
                   f(maps:get(long_entry_price, Position, 0), 0)) * f(maps:get(long_qty, Position, 0), 0),
    ShortMarkPNL = (f(maps:get(short_entry_price, Position, 0), 0) -
                    f(maps:get(short_mark_price, Position, maps:get(short_current_price, Position, 0)), 0)) *
                    f(maps:get(short_qty, Position, 0), 0),
    LongMarkPNL + ShortMarkPNL + f(maps:get(funding_pnl, Position, 0), 0) - close_fee(Position).

close_fee(Position) ->
    Notional = f(maps:get(notional, Position, maps:get(notional_usdt, Position, 0)), 0),
    Notional * (max(0, f(maps:get(long_fee_rate, Position, 0.0005), 0.0005)) +
                max(0, f(maps:get(short_fee_rate, Position, 0.0005), 0.0005))).

next_funding_event_pnl(Position) ->
    LongT = maps:get(long_next_funding_time, Position, 0),
    ShortT = maps:get(short_next_funding_time, Position, 0),
    Notional = f(maps:get(notional, Position, maps:get(notional_usdt, Position, 0)), 0),
    case {LongT, ShortT} of
        {0, 0} -> 0;
        {L, S} when L > 0, (S =< 0 orelse L < S) -> -Notional * f(maps:get(long_funding_rate, Position, 0), 0);
        {L, S} when S > 0, (L =< 0 orelse S < L) -> Notional * f(maps:get(short_funding_rate, Position, 0), 0);
        _ -> Notional * (f(maps:get(short_funding_rate, Position, 0), 0) -
                         f(maps:get(long_funding_rate, Position, 0), 0))
    end.

funding_cycle_mismatch(Position) ->
    abs(max(1, f(maps:get(long_funding_interval_hours, Position, 8), 8)) -
        max(1, f(maps:get(short_funding_interval_hours, Position, 8), 8))) >= 0.001.

delist_within(T, Now) ->
    T1 = arbiguard_util:to_int(T, 0),
    T1 > Now andalso T1 - Now =< 12 * 3600 * 1000.

advance_funding_time(T0, IntervalHours, After) ->
    T = arbiguard_util:to_int(T0, 0),
    case T =< 0 orelse After =< 0 of
        true -> T;
        false -> advance_funding_time_loop(T, interval_ms(IntervalHours), After)
    end.

advance_funding_time_loop(T, Step, After) when T =< After ->
    advance_funding_time_loop(T + Step, Step, After);
advance_funding_time_loop(T, _Step, _After) ->
    T.

interval_ms(Hours) ->
    trunc(max(1, f(Hours, 8)) * 3600000).

min_positive(A, B) when A =< 0 -> B;
min_positive(A, B) when B =< 0 -> A;
min_positive(A, B) -> min(A, B).

f(V, D) ->
    arbiguard_util:to_float(V, D).

apply_live_funding_settlement(Position0, Settlement) ->
    Side = maps:get(side, Settlement, <<"">>),
    PNL = f(maps:get(funding_pnl, Settlement, 0), 0),
    Rate = f(maps:get(funding_rate, Settlement, 0), 0),
    Time = arbiguard_util:to_int(maps:get(funding_time, Settlement, maps:get(time, Settlement, arbiguard_util:now_ms())), arbiguard_util:now_ms()),
    Position1 = Position0#{funding_pnl => f(maps:get(funding_pnl, Position0, 0), 0) + PNL,
                           last_funding_settlement_pnl => PNL,
                           last_funding_settled_at => Time,
                           last_live_funding_settlement => Settlement},
    Position2 = case Side of
        <<"long">> ->
            Position1#{long_funding_rate => choose_nonzero(Rate, maps:get(long_funding_rate, Position1, 0)),
                       long_funding_settlement_count => maps:get(long_funding_settlement_count, Position1, 0) + 1};
        <<"short">> ->
            Position1#{short_funding_rate => choose_nonzero(Rate, maps:get(short_funding_rate, Position1, 0)),
                       short_funding_settlement_count => maps:get(short_funding_settlement_count, Position1, 0) + 1};
        _ ->
            Position1#{funding_settlement_count => maps:get(funding_settlement_count, Position1, 0) + 1}
    end,
    recompute_position_pnl(Position2).

choose_nonzero(Value, Fallback) when abs(Value) =< 0.0 -> Fallback;
choose_nonzero(Value, _Fallback) -> Value.

find_order_by_position(PositionID0, Settlement, Orders) ->
    PositionID = arbiguard_util:to_binary(PositionID0),
    Matches = [{ID, Order} || {ID, Order} <- maps:to_list(Orders),
                             funding_settlement_matches(PositionID, Settlement, Order)],
    case Matches of
        [{ID1, Order1} | _] -> {ID1, Order1};
        [] -> {undefined, #{}}
    end.

funding_settlement_matches(PositionID, Settlement, Order) ->
    Position = maps:get(position, Order, #{}),
    PositionIDs = [maps:get(id, Position, <<"">>), maps:get(position_id, Position, <<"">>),
                   maps:get(id, Order, <<"">>)],
    ByID = PositionID =/= <<"">> andalso lists:member(PositionID, PositionIDs),
    Symbol = maps:get(symbol, Settlement, <<"">>),
    Exchange = maps:get(exchange, Settlement, <<"">>),
    Side = maps:get(side, Settlement, <<"">>),
    ByLeg = Symbol =/= <<"">> andalso Symbol =:= maps:get(symbol, Position, <<"">>) andalso
            ((Side =:= <<"long">> andalso Exchange =:= maps:get(long_exchange, Position, <<"">>)) orelse
             (Side =:= <<"short">> andalso Exchange =:= maps:get(short_exchange, Position, <<"">>)) orelse
             (Side =:= <<"">> andalso (Exchange =:= maps:get(long_exchange, Position, <<"">>) orelse
                                      Exchange =:= maps:get(short_exchange, Position, <<"">>)))),
    ByID orelse ByLeg.

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

apply_live_child_update(Parent, Update) ->
    ChildID = maps:get(id, Update, <<"">>),
    Pending0 = maps:get(pending_submissions, Parent, #{}),
    Pending = maps:remove(ChildID, Pending0),
    Filled = maps:get(filled_notional, Update, 0.0),
    Confirmed = maps:get(confirmed_notional, Parent, 0.0) + Filled,
    Reports = [Update | maps:get(fill_reports, Parent, [])],
    Parent#{pending_submissions => Pending,
            pending_notional => pending_notional(Pending),
            confirmed_notional => Confirmed,
            remaining_notional => max(0.0, maps:get(target_notional, Parent, 0.0) - Confirmed - pending_notional(Pending)),
            fill_reports => Reports,
            last_live_update => Update}.

maybe_finish_or_continue(Parent, State = #state{orders = Orders}) ->
    ID = maps:get(id, Parent, <<"">>),
    Target = maps:get(target_notional, Parent, 0.0),
    Confirmed = maps:get(confirmed_notional, Parent, 0.0),
    Pending = maps:get(pending_notional, Parent, 0.0),
    case Target > 0 andalso Confirmed >= Target * 0.999 of
        true ->
            Final = Parent#{status => <<"filled_live_close">>, remaining_notional => 0.0},
            unsubscribe_position_symbols(Final),
            {noreply, State#state{orders = Orders#{ID => Final}}};
        false ->
            Status = case Pending > 0 of
                true -> <<"awaiting_live_close_fill">>;
                false -> <<"waiting_ws_ticker">>
            end,
            {noreply, State#state{orders = Orders#{ID => Parent#{status => Status}}}}
    end.

remaining_to_submit(Order) ->
    Target = maps:get(target_notional, Order, 0.0),
    Confirmed = maps:get(confirmed_notional, Order, 0.0),
    Pending = pending_notional(maps:get(pending_submissions, Order, #{})),
    max(0.0, Target - Confirmed - Pending).

pending_notional(Pending) ->
    lists:sum([maps:get(remaining_notional, O, maps:get(requested_notional, O, maps:get(target_notional, O, 0.0)))
               || {_ID, O} <- maps:to_list(Pending)]).

child_order_id(ParentID) ->
    <<ParentID/binary, "|submit|", (integer_to_binary(arbiguard_util:now_ms()))/binary>>.

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

fill_notional(_Row, _LevelsKey, _PriceKey, Target) when Target =< 0 ->
    #{filled_notional => 0.0, filled_qty => 0.0, avg_price => 0.0, source => <<"empty_target">>};
fill_notional(Row, LevelsKey, PriceKey, Target) ->
    Levels = maps:get(LevelsKey, Row, []),
    case fill_levels(Levels, Target, 0.0, 0.0) of
        #{filled_notional := N, filled_qty := Q} = Fill when N > 0, Q > 0 ->
            Fill#{avg_price => N / Q, source => <<"ws_depth_levels">>};
        _ ->
            Price = f(maps:get(PriceKey, Row, 0), 0),
            Qty = f(maps:get(size_key(PriceKey), Row, 0), 0),
            case Price > 0 of
                true ->
                    MaxNotional = case Qty > 0 of true -> Price * Qty; false -> Target end,
                    Filled = min(Target, MaxNotional),
                    #{filled_notional => Filled,
                      filled_qty => safe_div(Filled, Price),
                      avg_price => Price,
                      source => case Qty > 0 of true -> <<"ws_bbo_size">>; false -> <<"ws_bbo_no_size">> end};
                false ->
                    #{filled_notional => 0.0, filled_qty => 0.0, avg_price => 0.0, source => <<"missing_price">>}
            end
    end.

fill_levels(_Levels, Target, Notional, Qty) when Notional >= Target ->
    #{filled_notional => Target, filled_qty => Qty, avg_price => safe_div(Target, Qty)};
fill_levels([], _Target, Notional, Qty) ->
    #{filled_notional => Notional, filled_qty => Qty, avg_price => safe_div(Notional, Qty)};
fill_levels([Level | Rest], Target, Notional, Qty) ->
    Price = f(maps:get(price, Level, 0), 0),
    LevelQty = f(maps:get(qty, Level, 0), 0),
    LevelNotional = case maps:get(notional, Level, 0) of
        N when N > 0 -> N;
        _ -> Price * LevelQty
    end,
    Need = max(0.0, Target - Notional),
    Take = min(Need, LevelNotional),
    TakeQty = safe_div(Take, Price),
    fill_levels(Rest, Target, Notional + Take, Qty + TakeQty).

size_key(ask) -> ask_size;
size_key(bid) -> bid_size.

safe_div(_A, B) when B =< 0 -> 0.0;
safe_div(A, B) -> A / B.

with_state_account(Req, State) ->
    Req#{account_id => maps:get(account_id, Req, State#state.account_id),
         account_mode => maps:get(account_mode, Req, State#state.account_mode)}.

safe_atom_part(V) ->
    S = binary_to_list(string:lowercase(arbiguard_util:to_binary(V))),
    [case ((C >= $a andalso C =< $z) orelse (C >= $0 andalso C =< $9)) of true -> C; false -> $_ end || C <- S].
