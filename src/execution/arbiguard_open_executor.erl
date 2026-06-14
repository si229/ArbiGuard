-module(arbiguard_open_executor).
-behaviour(gen_server).

-export([start_link/0, start_link/1, account_name/1,
         notify_opportunities/2, notify_opportunities/3,
         submit_order/2, submit_order/3, reset/0, reset/1,
         snapshot/0, snapshot/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {account_id = <<"paper-main">>, account_mode = <<"paper">>,
                live_enabled = false, exchange_tokens = #{}, exchange_accounts = #{},
                orders = #{}, last_opportunities = [], last_notify = 0,
                last_opportunity_updated_at = 0,
                ticker_cache = #{}, last_rejects = []}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_link(Config) ->
    AccountID = maps:get(account_id, Config, <<"paper-main">>),
    Name = maps:get(name, Config, account_name(AccountID)),
    gen_server:start_link({local, Name}, ?MODULE, [Config], []).

account_name(AccountID) ->
    list_to_atom("arbiguard_open_executor_" ++ safe_atom_part(AccountID)).

notify_opportunities(Req, Result) ->
    gen_server:cast(?MODULE, {opportunities, Req, Result}).

notify_opportunities(Executor, Req, Result) ->
    gen_server:cast(Executor, {opportunities, Req, Result}).

submit_order(Req, Opportunity) ->
    gen_server:call(?MODULE, {submit_order, Req, Opportunity}).

submit_order(Executor, Req, Opportunity) ->
    gen_server:call(Executor, {submit_order, Req, Opportunity}).

reset() ->
    gen_server:call(?MODULE, reset).

reset(Executor) ->
    gen_server:call(Executor, reset).

snapshot() ->
    gen_server:call(?MODULE, snapshot, 800).

snapshot(Executor) ->
    gen_server:call(Executor, snapshot, 800).

init([]) ->
    schedule_opportunity_poll(),
    {ok, #state{}};
init([Config]) ->
    schedule_opportunity_poll(),
    {ok, #state{account_id = maps:get(account_id, Config, <<"paper-main">>),
                account_mode = maps:get(account_mode, Config, <<"paper">>),
                live_enabled = maps:get(live_enabled, Config, false),
                exchange_tokens = maps:get(exchange_tokens, Config, #{}),
                exchange_accounts = maps:get(exchange_accounts, Config, #{})}}.

handle_call({submit_order, Req, Opportunity}, _From, State) ->
    {Order, NewState} = create_order(Req, Opportunity, State),
    {reply, Order, NewState};
handle_call(reset, _From, State = #state{orders = Orders}) ->
    _ = [unsubscribe_order_symbols(Order) || {_ID, Order} <- maps:to_list(Orders)],
    {reply, #{ok => true, cleared_open_orders => maps:size(Orders)},
     State#state{orders = #{}, last_opportunities = [], ticker_cache = #{}, last_rejects = []}};
handle_call(snapshot, _From, State) ->
    {reply, #{account_id => State#state.account_id,
              account_mode => State#state.account_mode,
              live_enabled => State#state.live_enabled,
              exchange_count => maps:size(State#state.exchange_accounts),
              token_configured_exchanges => maps:keys(State#state.exchange_tokens),
              orders => [public_order(O) || O <- maps:values(State#state.orders)],
              last_opportunities => State#state.last_opportunities,
              open_rejects => State#state.last_rejects,
              last_notify => State#state.last_notify,
              last_opportunity_updated_at => State#state.last_opportunity_updated_at,
              ticker_cache_size => maps:size(State#state.ticker_cache)}, State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({opportunities, Req, Result}, State) ->
    Ops = maps:get(execution_opportunities, Result, maps:get(opportunities, Result, [])),
    {noreply, process_opportunities(Req, Ops, 0, State)};
handle_cast({account_meta, Meta}, State) ->
    {noreply, apply_account_meta(Meta, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(opportunity_poll, State) ->
    schedule_opportunity_poll(),
    Snapshot = arbiguard_ets:opportunity_snapshot(),
    UpdatedAt = arbiguard_util:to_int(maps:get(updated_at, Snapshot, 0), 0),
    case UpdatedAt > State#state.last_opportunity_updated_at of
        true ->
            Req = maps:get(req, Snapshot, #{}),
            Ops = maps:get(opportunities, Snapshot, []),
            {noreply, process_opportunities(Req, Ops, UpdatedAt, State)};
        false ->
            {noreply, State}
    end;
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
        <<"filled">> ->
            maybe_finish_or_continue(Parent, State);
        <<"partial_filled">> ->
            maybe_finish_or_continue(Parent, State);
        _ ->
            {noreply, State#state{orders = Orders#{ID => Parent}}}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

schedule_opportunity_poll() ->
    Interval = arbiguard_util:to_int(application:get_env(arbiguard, opportunity_poll_ms, 1000), 1000),
    erlang:send_after(max(100, Interval), self(), opportunity_poll),
    ok.

process_opportunities(Req0, Opportunities, UpdatedAt, State) ->
    Req = with_state_account(arbiguard_calc:normalize_request(Req0), State),
    CurrentIDs = opportunity_id_set(Req, Opportunities),
    PrunedState = prune_stale_waiting_orders(CurrentIDs, State),
    NewState = lists:foldl(fun(Op, Acc) -> maybe_create_execution_order(Req, Op, Acc) end,
                           PrunedState, Opportunities),
    NewState#state{last_opportunities = Opportunities,
                   last_rejects = trim_rejects(NewState#state.last_rejects),
                   last_notify = arbiguard_util:now_ms(),
                   last_opportunity_updated_at = max(UpdatedAt, NewState#state.last_opportunity_updated_at)}.

maybe_create_execution_order(Req, Op, State = #state{orders = Orders}) ->
    Req1 = arbiguard_calc:normalize_request(Req),
    MinProfit = maps:get(min_execution_profit_usdt, Req1, 10.0),
    MaxOpen = maps:get(max_open_positions, Req1, 5),
    ID = order_key(Req1, Op),
    CanOpenMore = can_create_more_orders(MaxOpen, Orders),
    Profit = maps:get(estimated_net_profit, Op, 0.0),
    Duplicate = maps:is_key(ID, Orders),
    case CanOpenMore andalso Profit >= MinProfit andalso not Duplicate of
        true ->
            subscribe_order_symbols(Op),
            Order = (order_plan(Req1, Op))#{status => <<"waiting_ws_ticker">>,
                                           req => Req1,
                                           opportunity => Op},
            maybe_dispatch_ready_orders(State#state{orders = Orders#{ID => Order}});
        false ->
            remember_reject(State, reject_reason(CanOpenMore, Duplicate, Profit, MinProfit), Req1, Op)
    end.

can_create_more_orders(MaxOpen, _Orders) when MaxOpen =< 0 ->
    false;
can_create_more_orders(MaxOpen, Orders) ->
    PositionCount = paper_position_count(),
    PendingOpenCount = active_open_order_count(Orders),
    PositionCount + PendingOpenCount < MaxOpen.

paper_position_count() ->
    case catch arbiguard_state:snapshot() of
        Snapshot when is_map(Snapshot) -> length(maps:get(positions, Snapshot, []));
        _ -> 0
    end.

active_open_order_count(Orders) ->
    length([O || {_ID, O} <- maps:to_list(Orders), active_open_status(maps:get(status, O, <<"">>))]).

active_open_status(<<"waiting_ws_ticker">>) -> true;
active_open_status(<<"awaiting_live_open_fill">>) -> true;
active_open_status(<<"partial_live_open_continue">>) -> true;
active_open_status(_) -> false.

opportunity_id_set(Req, Opportunities) ->
    maps:from_list([{order_key(Req, Op), true} || Op <- Opportunities]).

prune_stale_waiting_orders(CurrentIDs, State = #state{orders = Orders}) ->
    Kept = maps:fold(fun(ID, Order, Acc) ->
        case should_prune_order(ID, Order, CurrentIDs) of
            true ->
                unsubscribe_order_symbols(Order),
                lager:info("open executor pruned stale order id=~s symbol=~s long=~s short=~s",
                           [ID, maps:get(symbol, Order, <<"">>), maps:get(long_exchange, Order, <<"">>),
                            maps:get(short_exchange, Order, <<"">>)]),
                Acc;
            false ->
                Acc#{ID => Order}
        end
    end, #{}, Orders),
    State#state{orders = Kept}.

should_prune_order(ID, Order, CurrentIDs) ->
    Status = maps:get(status, Order, <<"">>),
    case terminal_open_status(Status) of
        true ->
            terminal_order_expired(Order);
        false ->
            (not maps:is_key(ID, CurrentIDs))
                andalso prunable_status(Status)
                andalso stale_wait_expired(Order)
    end.

prunable_status(<<"waiting_ws_ticker">>) -> true;
prunable_status(_Status) -> false.

terminal_open_status(<<"filled_paper_open">>) -> true;
terminal_open_status(<<"filled_live_open">>) -> true;
terminal_open_status(<<"submitted_live_rejected">>) -> true;
terminal_open_status(<<"ticker_unavailable">>) -> true;
terminal_open_status(<<"profit_below_threshold_expired">>) -> true;
terminal_open_status(_) -> false.

stale_wait_expired(Order) ->
    GraceMs = arbiguard_util:to_int(application:get_env(arbiguard, execution_order_stale_grace_ms, 15000), 15000),
    CreatedAt = arbiguard_util:to_int(maps:get(created_at, Order, 0), 0),
    CreatedAt =:= 0 orelse arbiguard_util:now_ms() - CreatedAt >= GraceMs.

terminal_order_expired(Order) ->
    GraceMs = arbiguard_util:to_int(application:get_env(arbiguard, execution_order_terminal_grace_ms, 60000), 60000),
    DoneAt0 = arbiguard_util:to_int(maps:get(filled_at, Order, maps:get(submitted_at, Order, 0)), 0),
    DoneAt = case DoneAt0 > 0 of
        true -> DoneAt0;
        false -> arbiguard_util:to_int(maps:get(created_at, Order, 0), 0)
    end,
    DoneAt =:= 0 orelse arbiguard_util:now_ms() - DoneAt >= GraceMs.

reject_reason(false, _Duplicate, _Profit, _MinProfit) -> <<"max_open_positions_reached">>;
reject_reason(_CanOpenMore, true, _Profit, _MinProfit) -> <<"duplicate_open_order">>;
reject_reason(_CanOpenMore, _Duplicate, Profit, MinProfit) when Profit < MinProfit -> <<"profit_below_min_execution_profit">>;
reject_reason(_CanOpenMore, _Duplicate, _Profit, _MinProfit) -> <<"open_filter_rejected">>.

remember_reject(State = #state{last_rejects = Rejects}, Reason, Req, Op) ->
    Reject = #{time => arbiguard_util:now_ms(),
               reason => Reason,
               symbol => maps:get(symbol, Op, <<"">>),
               long_exchange => maps:get(long_exchange, Op, <<"">>),
               short_exchange => maps:get(short_exchange, Op, <<"">>),
               estimated_net_profit => maps:get(estimated_net_profit, Op, 0.0),
               min_execution_profit_usdt => maps:get(min_execution_profit_usdt, Req, 10.0),
               max_open_positions => maps:get(max_open_positions, Req, 5),
               current_positions => paper_position_count(),
               active_open_orders => active_open_order_count(State#state.orders)},
    State#state{last_rejects = trim_rejects([Reject | Rejects])}.

trim_rejects(Rejects) ->
    lists:sublist(Rejects, 50).

create_order(Req, Op, State = #state{orders = Orders}) ->
    Req1 = with_state_account(arbiguard_calc:normalize_request(Req), State),
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

unsubscribe_order_symbols(Order) ->
    Symbol = maps:get(symbol, Order, <<"">>),
    LongEx = maps:get(long_exchange, Order, <<"">>),
    ShortEx = maps:get(short_exchange, Order, <<"">>),
    case Symbol =/= <<"">> of
        true ->
            catch arbiguard_exchange_ticker:unsubscribe(LongEx, Symbol, open_execution_order),
            catch arbiguard_exchange_ticker:unsubscribe(ShortEx, Symbol, open_execution_order),
            lager:info("open executor local unsubscribe done symbol=~s long=~s short=~s",
                       [Symbol, LongEx, ShortEx]);
        false ->
            ok
    end,
    ok.

dispatch_order(Req0, Order, Op) ->
    Req = arbiguard_calc:normalize_request(Req0),
    AccountMode = maps:get(account_mode, Order, maps:get(account_mode, Req, <<"paper">>)),
    FinalOrder = case AccountMode of
        <<"live">> ->
            submit_live_child_order(Req, Order, <<"awaiting_live_open_fill">>);
        live ->
            submit_live_child_order(Req, Order, <<"awaiting_live_open_fill">>);
        _ ->
            ApplyResult = catch arbiguard_state:apply_open_order(Req, Order, Op),
            maybe_handoff_position_to_close(Req, ApplyResult),
            Position = case ApplyResult of
                Result when is_map(Result) -> maps:get(opened_position, Result, #{});
                _ -> #{}
            end,
            OpenFee = maps:get(open_fee, Position, maps:get(open_fee, Op, 0.0)),
            PaperFilledNotional = maps:get(submit_notional, Order, maps:get(target_notional, Order, 0.0)),
            FilledOrder = Order#{status => <<"filled_paper_open">>,
                                 wait_reason => <<"filled">>,
                                 wait_detail => execution_ticker_detail(Op),
                                 target_notional => PaperFilledNotional,
                                 confirmed_notional => PaperFilledNotional,
                                 remaining_notional => 0.0,
                                 long_filled_qty => maps:get(long_qty, Position, maps:get(long_target_qty, Order, 0.0)),
                                 short_filled_qty => maps:get(short_qty, Position, maps:get(short_target_qty, Order, 0.0)),
                                 execution_fee => OpenFee,
                                 actual_pnl => maps:get(unrealized_pnl, Position, -OpenFee)},
            (public_order(FilledOrder))#{
                exchange_submit => <<"skipped_paper_account">>,
                execution_path => <<"ws_ticker_paper_filled_no_exchange_submit">>,
                filled_at => arbiguard_util:now_ms()}
    end,
    maybe_unsubscribe_after_dispatch(FinalOrder),
    FinalOrder.

maybe_unsubscribe_after_dispatch(Order) ->
    case maps:get(status, Order, <<"">>) of
        <<"awaiting_live_open_fill">> -> ok;
        <<"partial_live_open_continue">> -> ok;
        <<"submitted_live_rejected">> -> unsubscribe_order_symbols(Order);
        _ -> unsubscribe_order_symbols(Order)
    end.

submit_live_child_order(Req, Order, AwaitingStatus) ->
    ParentID = maps:get(id, Order),
    Remaining = remaining_to_submit(Order),
    BatchNotional = min(Remaining, maps:get(submit_notional, Order, Remaining)),
    ChildID = child_order_id(ParentID),
    ChildOrder = Order#{id => ChildID,
                        parent_id => ParentID,
                        execution_order_id => ParentID,
                        owner_pid => self(),
                        target_notional => BatchNotional,
                        requested_notional => BatchNotional},
    register_live_order_owner(Req, ChildOrder),
    LiveResult = catch arbiguard_live_order:submit_open(Req, ChildOrder),
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
            maybe_submit_from_ticker(Order, Cache);
        <<"awaiting_live_open_fill">> ->
            maybe_submit_from_ticker(Order, Cache);
        <<"partial_live_open_continue">> ->
            maybe_submit_from_ticker(Order, Cache);
        _ ->
            Order
    end.

maybe_submit_from_ticker(Order, Cache) ->
    case remaining_to_submit(Order) > 0 of
        false -> Order;
        true -> maybe_submit_from_ticker_1(Order, Cache)
    end.

maybe_submit_from_ticker_1(Order, Cache) ->
    Op0 = maps:get(opportunity, Order, #{}),
    Req = arbiguard_calc:normalize_request(maps:get(req, Order, #{})),
    case enrich_opportunity_from_cache(apply_remaining_notional(Op0, Order), Req, Cache) of
        {ok, Op} ->
            lager:info("open executor ws-ready symbol=~s long=~s short=~s long_price=~p short_price=~p long_mark=~p short_mark=~p remaining=~p",
                       [maps:get(symbol, Op, <<"">>), maps:get(long_exchange, Op, <<"">>),
                        maps:get(short_exchange, Op, <<"">>), maps:get(long_price, Op, 0),
                        maps:get(short_price, Op, 0), maps:get(long_mark_price, Op, 0),
                        maps:get(short_mark_price, Op, 0), maps:get(suggested_notional, Op, 0)]),
            Order1 = public_order(Order),
            DispatchNotional = maps:get(suggested_notional, Op, maps:get(target_notional, Order1, 0.0)),
            dispatch_order(Req, Order1#{submit_notional => DispatchNotional}, Op);
        {wait, WaitInfo} ->
            maybe_timeout_waiting_order(
              Order#{wait_reason => maps:get(reason, WaitInfo, <<"waiting_ws_ticker">>),
                     wait_detail => enrich_wait_detail(Order, WaitInfo),
                     wait_checked_at => arbiguard_util:now_ms()});
        wait ->
            maybe_timeout_waiting_order(
              Order#{wait_reason => <<"waiting_ws_ticker">>,
                     wait_detail => enrich_wait_detail(Order, #{}),
                     wait_checked_at => arbiguard_util:now_ms()})
    end.

maybe_timeout_waiting_order(Order) ->
    Now = arbiguard_util:now_ms(),
    CreatedAt = arbiguard_util:to_int(maps:get(created_at, Order, 0), 0),
    TimeoutMs = arbiguard_util:to_int(application:get_env(arbiguard, execution_ticker_wait_timeout_ms, 20000), 20000),
    ProfitTimeoutMs = arbiguard_util:to_int(application:get_env(arbiguard, execution_profit_wait_timeout_ms, TimeoutMs), TimeoutMs),
    WaitReason = maps:get(wait_reason, Order, <<"">>),
    case WaitReason =:= <<"profit_below_threshold_after_ws_price">>
         andalso CreatedAt > 0
         andalso Now - CreatedAt >= ProfitTimeoutMs of
        true ->
            lager:info("open executor profit wait expired symbol=~s long=~s short=~s timeout_ms=~p detail=~p",
                       [maps:get(symbol, Order, <<"">>), maps:get(long_exchange, Order, <<"">>),
                        maps:get(short_exchange, Order, <<"">>), ProfitTimeoutMs, maps:get(wait_detail, Order, #{})]),
            unsubscribe_order_symbols(Order),
            Order#{status => <<"profit_below_threshold_expired">>,
                   finished_at => Now,
                   remaining_notional => remaining_to_submit(Order)};
        false ->
            maybe_timeout_missing_ticker_order(Order, Now, CreatedAt, TimeoutMs)
    end.

maybe_timeout_missing_ticker_order(Order, Now, CreatedAt, TimeoutMs) ->
    case CreatedAt > 0 andalso Now - CreatedAt >= TimeoutMs of
        true ->
            lager:warning("open executor ticker unavailable timeout symbol=~s long=~s short=~s reason=~s timeout_ms=~p detail=~p",
                          [maps:get(symbol, Order, <<"">>), maps:get(long_exchange, Order, <<"">>),
                           maps:get(short_exchange, Order, <<"">>), maps:get(wait_reason, Order, <<"">>),
                           TimeoutMs, maps:get(wait_detail, Order, #{})]),
            unsubscribe_order_symbols(Order),
            Order#{status => <<"ticker_unavailable">>,
                   finished_at => Now,
                   wait_reason => <<"ticker_unavailable_timeout">>,
                   remaining_notional => remaining_to_submit(Order)};
        false ->
            Order
    end.

enrich_wait_detail(Order, WaitInfo) ->
    Symbol = maps:get(symbol, Order, maps:get(symbol, WaitInfo, <<"">>)),
    LongEx = maps:get(long_exchange, Order, maps:get(long_exchange, WaitInfo, <<"">>)),
    ShortEx = maps:get(short_exchange, Order, maps:get(short_exchange, WaitInfo, <<"">>)),
    LongSub = safe_subscription_status(LongEx, Symbol),
    ShortSub = safe_subscription_status(ShortEx, Symbol),
    WaitInfo#{long_subscription => LongSub,
              short_subscription => ShortSub,
              wait_age_ms => max(0, arbiguard_util:now_ms() - arbiguard_util:to_int(maps:get(created_at, Order, 0), 0)),
              ticker_wait_timeout_ms => arbiguard_util:to_int(application:get_env(arbiguard, execution_ticker_wait_timeout_ms, 20000), 20000)}.

safe_subscription_status(Exchange, Symbol) ->
    case catch arbiguard_exchange_ticker:subscription_status(Exchange, Symbol) of
        Status when is_map(Status) -> Status;
        Error -> #{exchange => Exchange, symbol => Symbol, error => Error}
    end.

apply_remaining_notional(Op, Order) ->
    Remaining = remaining_to_submit(Order),
    case Remaining > 0 of
        true -> Op#{suggested_notional => Remaining};
        false -> Op
    end.

enrich_opportunity_from_cache(Op, Req, Cache) ->
    Symbol = maps:get(symbol, Op, <<"">>),
    LongEx = maps:get(long_exchange, Op, <<"">>),
    ShortEx = maps:get(short_exchange, Op, <<"">>),
    SymbolRows = maps:get(Symbol, Cache, #{}),
    case {ticker_from_symbol_cache_or_ets(LongEx, Symbol, SymbolRows),
          ticker_from_symbol_cache_or_ets(ShortEx, Symbol, SymbolRows)} of
        {LongRow, ShortRow} when is_map(LongRow), is_map(ShortRow) ->
            Target = maps:get(suggested_notional, Op, maps:get(execution_notional_usdt, Req, 200.0)),
            LongFill0 = fill_notional(LongRow, ask_levels, ask, Target),
            ShortFill0 = fill_notional(ShortRow, bid_levels, bid, Target),
            FillNotional = min(maps:get(filled_notional, LongFill0, 0.0), maps:get(filled_notional, ShortFill0, 0.0)),
            LongFill = fill_notional(LongRow, ask_levels, ask, FillNotional),
            ShortFill = fill_notional(ShortRow, bid_levels, bid, FillNotional),
            LongPrice = maps:get(avg_price, LongFill, 0.0),
            ShortPrice = maps:get(avg_price, ShortFill, 0.0),
            case FillNotional > 0 andalso LongPrice > 0 andalso ShortPrice > 0 of
                true ->
                    Op1 = Op#{long_price => LongPrice,
                               short_price => ShortPrice,
                               suggested_notional => FillNotional,
                               long_bid => maps:get(bid, LongRow, 0.0),
                               long_ask => LongPrice,
                               long_updated_at => maps:get(updated_at, LongRow, 0),
                               short_bid => ShortPrice,
                               short_ask => maps:get(ask, ShortRow, 0.0),
                               short_updated_at => maps:get(updated_at, ShortRow, 0),
                               long_current_price => LongPrice,
                               short_current_price => ShortPrice,
                               long_trade_mid_price => maps:get(trade_mid_price, LongRow, LongPrice),
                               short_trade_mid_price => maps:get(trade_mid_price, ShortRow, ShortPrice),
                               long_mark_price => maps:get(mark_price, LongRow, LongPrice),
                               short_mark_price => maps:get(mark_price, ShortRow, ShortPrice),
                               long_liquidation_reference_price => maps:get(liquidation_reference_price, LongRow, LongPrice),
                               short_liquidation_reference_price => maps:get(liquidation_reference_price, ShortRow, ShortPrice),
                               long_depth_notional => maps:get(filled_notional, LongFill, 0.0),
                               short_depth_notional => maps:get(filled_notional, ShortFill, 0.0),
                               long_depth_qty => maps:get(filled_qty, LongFill, 0.0),
                               short_depth_qty => maps:get(filled_qty, ShortFill, 0.0),
                               execution_price_basis => maps:get(source, LongFill, <<"ws_depth_or_bbo">>),
                               liquidation_price_basis => <<"mark_price">>,
                               execution_price_updated_at => min(maps:get(updated_at, LongRow, 0), maps:get(updated_at, ShortRow, 0))},
                    require_profitable(refresh_expected_profit(Op1, Req), Req);
                false ->
                    {wait, ticker_wait_info(Op, LongRow, ShortRow)}
            end;
        {LongRow, ShortRow} ->
            {wait, ticker_wait_info(Op, LongRow, ShortRow)}
    end.

ticker_wait_info(Op, LongRow, ShortRow) ->
    LongReady = is_map(LongRow),
    ShortReady = is_map(ShortRow),
    LongAsk = price_or_zero(LongRow, ask),
    ShortBid = price_or_zero(ShortRow, bid),
    Reason = case {LongReady, ShortReady, LongAsk > 0, ShortBid > 0} of
        {false, false, _, _} -> <<"missing_both_tickers">>;
        {false, _, _, _} -> <<"missing_long_ticker">>;
        {_, false, _, _} -> <<"missing_short_ticker">>;
        {_, _, false, false} -> <<"missing_long_ask_and_short_bid">>;
        {_, _, false, _} -> <<"missing_long_ask">>;
        {_, _, _, false} -> <<"missing_short_bid">>;
        _ -> <<"waiting_ws_ticker">>
    end,
    #{reason => Reason,
      symbol => maps:get(symbol, Op, <<"">>),
      long_exchange => maps:get(long_exchange, Op, <<"">>),
      short_exchange => maps:get(short_exchange, Op, <<"">>),
      long_ticker_ready => LongReady,
      short_ticker_ready => ShortReady,
      long_bid => price_or_zero(LongRow, bid),
      long_ask => LongAsk,
      long_updated_at => int_or_zero(LongRow, updated_at),
      short_bid => ShortBid,
      short_ask => price_or_zero(ShortRow, ask),
      short_updated_at => int_or_zero(ShortRow, updated_at)}.

price_or_zero(Row, Key) when is_map(Row) ->
    arbiguard_util:to_float(maps:get(Key, Row, 0), 0);
price_or_zero(_Row, _Key) ->
    0.0.

int_or_zero(Row, Key) when is_map(Row) ->
    arbiguard_util:to_int(maps:get(Key, Row, 0), 0);
int_or_zero(_Row, _Key) ->
    0.

fill_notional(_Row, _LevelsKey, _PriceKey, Target) when Target =< 0 ->
    #{filled_notional => 0.0, filled_qty => 0.0, avg_price => 0.0, source => <<"empty_target">>};
fill_notional(Row, LevelsKey, PriceKey, Target) ->
    Levels = maps:get(LevelsKey, Row, []),
    case fill_levels(Levels, Target, 0.0, 0.0) of
        #{filled_notional := N, filled_qty := Q} = Fill when N > 0, Q > 0 ->
            Fill#{avg_price => N / Q, source => <<"ws_depth_levels">>};
        _ ->
            Price = price_or_zero(Row, PriceKey),
            Qty = price_or_zero(Row, size_key(PriceKey)),
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
    Price = price_or_zero(Level, price),
    LevelQty = price_or_zero(Level, qty),
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

refresh_expected_profit(Op, Req) ->
    LongPrice = maps:get(long_price, Op, 0.0),
    ShortPrice = maps:get(short_price, Op, 0.0),
    Mid = case LongPrice > 0 andalso ShortPrice > 0 of true -> (LongPrice + ShortPrice) / 2; false -> 0.0 end,
    PriceGap = case Mid > 0 of true -> (ShortPrice - LongPrice) / Mid; false -> 0.0 end,
    FundingEdge = next_settlement_return(Op),
    FeeRate = maps:get(long_fee_rate, Op, 0.0005) + maps:get(short_fee_rate, Op, 0.0005),
    ExpectedNetReturn = FundingEdge + PriceGap - FeeRate * 2,
    Notional = maps:get(suggested_notional, Op, maps:get(execution_notional_usdt, Req, 200.0)),
    Op#{pre_ws_estimated_net_profit => maps:get(estimated_net_profit, Op, 0.0),
        pre_ws_expected_net_return => maps:get(expected_net_return, Op, 0.0),
        price_gap_return => PriceGap,
        basis_rate => PriceGap,
        expected_gross_return => FundingEdge + PriceGap,
        expected_net_return => ExpectedNetReturn,
        estimated_net_profit => Notional * ExpectedNetReturn,
        suggested_notional => Notional}.

next_settlement_return(Op) ->
    LongPNL = -arbiguard_util:to_float(maps:get(long_funding_rate, Op, 0), 0),
    ShortPNL = arbiguard_util:to_float(maps:get(short_funding_rate, Op, 0), 0),
    LongNext = maps:get(long_next_funding_time, Op, 0),
    ShortNext = maps:get(short_next_funding_time, Op, 0),
    SameWindow = LongNext > 0 andalso ShortNext > 0 andalso abs(LongNext - ShortNext) =< 60000,
    case SameWindow of
        true ->
            LongPNL + ShortPNL;
        false when LongNext > 0, ShortNext > 0, LongNext < ShortNext ->
            max(0.0, LongPNL);
        false when LongNext > 0, ShortNext > 0, ShortNext < LongNext ->
            max(0.0, ShortPNL);
        false ->
            LongPNL + ShortPNL
    end.

require_profitable(Op, Req) ->
    MinProfit = maps:get(min_execution_profit_usdt, Req, 10.0),
    MaxBasis = maps:get(max_basis_rate, Req, 0.02),
    Profit = maps:get(estimated_net_profit, Op, 0.0),
    Basis = abs(maps:get(price_gap_return, Op, 0.0)),
    case Profit >= MinProfit andalso Basis =< MaxBasis of
        true -> {ok, Op};
        false ->
            Reason = case Basis > MaxBasis of
                true -> <<"basis_too_wide_after_ws_price">>;
                false -> <<"profit_below_threshold_after_ws_price">>
            end,
            lager:info("open executor wait rejected_after_ws symbol=~s reason=~s profit=~p min=~p basis=~p max_basis=~p pre_profit=~p",
                       [maps:get(symbol, Op, <<"">>), Reason, Profit, MinProfit, Basis, MaxBasis,
                        maps:get(pre_ws_estimated_net_profit, Op, 0.0)]),
            {wait, #{reason => Reason,
                     symbol => maps:get(symbol, Op, <<"">>),
                     estimated_net_profit => Profit,
                     pre_ws_estimated_net_profit => maps:get(pre_ws_estimated_net_profit, Op, 0.0),
                     min_execution_profit_usdt => MinProfit,
                     long_price => maps:get(long_price, Op, 0.0),
                     short_price => maps:get(short_price, Op, 0.0),
                     price_gap_return => maps:get(price_gap_return, Op, 0.0),
                     max_basis_rate => MaxBasis,
                     funding_edge_return => maps:get(funding_edge_return, Op, 0.0)}}
    end.

public_order(Order) ->
    Target = maps:get(target_notional, Order, 0.0),
    Status = maps:get(status, Order, <<"">>),
    Confirmed0 = maps:get(confirmed_notional, Order, 0.0),
    Confirmed = case filled_status(Status) andalso abs(Confirmed0) =< 0.0 of
        true -> Target;
        false -> Confirmed0
    end,
    Pending = pending_notional(maps:get(pending_submissions, Order, #{})),
    Remaining0 = maps:get(remaining_notional, Order, max(0.0, Target - Confirmed - Pending)),
    Progress = case Target > 0 of
        true -> min(100.0, max(0.0, Confirmed / Target * 100.0));
        false -> 0.0
    end,
    Ratio = case Target > 0 of true -> min(1.0, max(0.0, Confirmed / Target)); false -> 0.0 end,
    Op = maps:get(opportunity, Order, #{}),
    OpenFee = maps:get(execution_fee, Order, maps:get(open_fee, Op, 0.0)),
    ActualPNL = maps:get(actual_pnl, Order, case filled_status(Status) of true -> -OpenFee; false -> 0.0 end),
    maps:without([req, opportunity],
                 Order#{target_notional => Target,
                        confirmed_notional => Confirmed,
                        pending_notional => Pending,
                        remaining_notional => Remaining0,
                        progress_pct => Progress,
                        long_target_notional => Target,
                        short_target_notional => Target,
                        long_confirmed_notional => Confirmed,
                        short_confirmed_notional => Confirmed,
                        long_filled_qty => maps:get(long_filled_qty, Order, maps:get(long_target_qty, Order, 0.0) * Ratio),
                        short_filled_qty => maps:get(short_filled_qty, Order, maps:get(short_target_qty, Order, 0.0) * Ratio),
                        execution_fee => OpenFee,
                        actual_pnl => ActualPNL}).

filled_status(<<"filled_paper_open">>) -> true;
filled_status(<<"filled_live_open">>) -> true;
filled_status(_) -> false.

execution_ticker_detail(Op) ->
    #{reason => <<"filled">>,
      long_exchange => maps:get(long_exchange, Op, <<"">>),
      short_exchange => maps:get(short_exchange, Op, <<"">>),
      long_bid => maps:get(long_bid, Op, 0.0),
      long_ask => maps:get(long_ask, Op, maps:get(long_price, Op, 0.0)),
      long_updated_at => maps:get(long_updated_at, Op, 0),
      short_bid => maps:get(short_bid, Op, maps:get(short_price, Op, 0.0)),
      short_ask => maps:get(short_ask, Op, 0.0),
      short_updated_at => maps:get(short_updated_at, Op, 0),
      execution_price_updated_at => maps:get(execution_price_updated_at, Op, 0)}.

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
            Final = Parent#{status => <<"filled_live_open">>, remaining_notional => 0.0},
            maybe_handoff_position_to_close(maps:get(req, Final, #{}), #{opened_position => order_to_position(Final)}),
            unsubscribe_order_symbols(Final),
            {noreply, State#state{orders = Orders#{ID => Final}}};
        false ->
            Status = case Pending > 0 of
                true -> <<"awaiting_live_open_fill">>;
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

update_symbol_cache(Row, Cache) ->
    Symbol = maps:get(symbol, Row, <<"">>),
    Exchange = maps:get(exchange, Row, <<"">>),
    SymbolRows0 = maps:get(Symbol, Cache, #{}),
    Cache#{Symbol => SymbolRows0#{Exchange => Row}}.

maybe_handoff_position_to_close(Req, Result) when is_map(Result) ->
    case maps:get(opened_position, Result, undefined) of
        Position when is_map(Position) ->
            _ = catch arbiguard_account_manager:track_position(Req, Position),
            lager:info("open executor handed position to close executor symbol=~s long=~s short=~s",
                       [maps:get(symbol, Position, <<"">>), maps:get(long_exchange, Position, <<"">>),
                        maps:get(short_exchange, Position, <<"">>)]),
            ok;
        _ -> ok
    end;
maybe_handoff_position_to_close(_Req, _Result) ->
    ok.

order_to_position(Order) ->
    Notional = maps:get(confirmed_notional, Order, maps:get(target_notional, Order, 0.0)),
    #{id => maps:get(id, Order, <<"">>),
      account_mode => maps:get(account_mode, Order, <<"live">>),
      account_id => maps:get(account_id, Order, <<"live-main">>),
      symbol => maps:get(symbol, Order, <<"">>),
      long_exchange => maps:get(long_exchange, Order, <<"">>),
      short_exchange => maps:get(short_exchange, Order, <<"">>),
      notional => Notional,
      notional_usdt => Notional,
      opened_at => maps:get(created_at, Order, arbiguard_util:now_ms()),
      updated_at => arbiguard_util:now_ms()}.

with_state_account(Req, State) ->
    Req#{account_id => maps:get(account_id, Req, State#state.account_id),
         account_mode => maps:get(account_mode, Req, State#state.account_mode),
         live_enabled => maps:get(live_enabled, Req, State#state.live_enabled),
         exchange_tokens => maps:get(exchange_tokens, Req, State#state.exchange_tokens),
         exchange_accounts => maps:get(exchange_accounts, Req, State#state.exchange_accounts)}.

apply_account_meta(Meta, State) ->
    State#state{account_id = maps:get(account_id, Meta, State#state.account_id),
                account_mode = maps:get(account_mode, Meta, State#state.account_mode),
                live_enabled = maps:get(live_enabled, Meta, State#state.live_enabled),
                exchange_tokens = maps:get(exchange_tokens, Meta, State#state.exchange_tokens),
                exchange_accounts = maps:get(exchange_accounts, Meta, State#state.exchange_accounts)}.

safe_atom_part(V) ->
    S = binary_to_list(string:lowercase(arbiguard_util:to_binary(V))),
    [case ((C >= $a andalso C =< $z) orelse (C >= $0 andalso C =< $9)) of true -> C; false -> $_ end || C <- S].
