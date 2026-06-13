-module(arbiguard_scanner).
-behaviour(gen_server).

-export([start_link/0, scan/1, scan_once/1, apply_settings/1, snapshot/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {req = #{}, interval_ms = 1000, last_result = #{}, last_scan = 0}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

scan_once(Req) ->
    gen_server:call(?MODULE, {scan_once, Req}, 30000).

apply_settings(Req) ->
    gen_server:call(?MODULE, {apply_settings, Req}, 30000).

snapshot() ->
    gen_server:call(?MODULE, snapshot).

init([]) ->
    Interval = application:get_env(arbiguard, scanner_interval_ms, 1000),
    erlang:send_after(Interval, self(), scan_tick),
    {ok, #state{interval_ms = Interval}}.

handle_call({scan_once, Req}, _From, State) ->
    Result = run_scan(Req),
    arbiguard_executor:notify_opportunities(Req, Result),
    {reply, Result, State#state{req = Req, last_result = Result, last_scan = arbiguard_util:now_ms()}};
handle_call({apply_settings, Req0}, _From, State) ->
    Req = arbiguard_calc:normalize_request(Req0),
    {reply, #{ok => true,
              applied_at => arbiguard_util:now_ms(),
              scanner_interval_ms => State#state.interval_ms,
              next_scan_uses => Req},
     State#state{req = Req}};
handle_call(snapshot, _From, State) ->
    {reply, #{last_scan => State#state.last_scan,
              interval_ms => State#state.interval_ms,
              active_request => State#state.req,
              last_result => State#state.last_result}, State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(scan_tick, State = #state{req = Req, interval_ms = Interval}) ->
    Result = scan_from_ets(Req),
    arbiguard_executor:notify_opportunities(Req, Result),
    erlang:send_after(Interval, self(), scan_tick),
    {noreply, State#state{last_result = Result, last_scan = arbiguard_util:now_ms()}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

scan(Req0) ->
    Req = arbiguard_calc:normalize_request(Req0),
    {Market, Warnings} = load_market(Req),
    Opportunities0 = build_all(Req, Market),
    Opportunities = limit(sort_ops(Opportunities0), maps:get(limit, Req, 30)),
    #{exchange => <<"multi">>,
      strategy => <<"cross_exchange_perp_funding">>,
      capital_usdt => maps:get(capital_usdt, Req),
      max_position_usdt => max_position_usdt(Req),
      execution_notional_usdt => maps:get(execution_notional_usdt, Req),
      min_funding_rate => maps:get(min_funding_rate, Req),
      min_price_gap_rate => maps:get(min_price_gap_rate, Req),
      max_basis_rate => maps:get(max_basis_rate, Req),
      execution_order_mode => maps:get(execution_order_mode, Req),
      min_execution_profit_usdt => maps:get(min_execution_profit_usdt, Req),
      price_gap_close_profit_usdt => maps:get(price_gap_close_profit_usdt, Req),
      paper_leverage => maps:get(paper_leverage, Req),
      monitor_mode => monitor_mode(Opportunities),
      opportunities => Opportunities,
      exchanges => maps:get(exchanges, Req),
      warnings => Warnings,
      updated_at => iso_now()}.

scan_from_ets(Req0) ->
    Req = arbiguard_calc:normalize_request(Req0),
    Enabled = enabled_exchange_set(maps:get(exchanges, Req, [])),
    Req1 = Req#{market_snapshots => merge_ets_market(Enabled)},
    scan(Req1).

run_scan(Req) ->
    case maps:is_key(market_snapshots, Req) of
        true -> scan(Req);
        false -> scan_from_ets(Req)
    end.

merge_ets_market(Enabled) ->
    FundingRows = arbiguard_ets:all_funding(),
    TickerByKey = maps:from_list([{{maps:get(exchange, T, <<"">>), maps:get(symbol, T, <<"">>)}, T} || T <- arbiguard_ets:all_tickers()]),
    [merge_ticker(Row, Ticker)
     || Row <- FundingRows,
        maps:is_key(maps:get(exchange, Row, <<"">>), Enabled),
        Ticker <- [maps:get({maps:get(exchange, Row, <<"">>), maps:get(symbol, Row, <<"">>)}, TickerByKey, #{})]].

enabled_exchange_set(Exchanges) ->
    maps:from_list([{maps:get(id, E, <<"">>), true} || E <- Exchanges]).

merge_ticker(Funding, Ticker) when map_size(Ticker) =:= 0 ->
    Funding;
merge_ticker(Funding, Ticker) ->
    Funding#{
        mark_price => arbiguard_util:to_float(maps:get(mark_price, Ticker, maps:get(price, Ticker, maps:get(mark_price, Funding, 0))), maps:get(mark_price, Funding, 0)),
        bid => maps:get(bid, Ticker, maps:get(bid, Funding, 0)),
        ask => maps:get(ask, Ticker, maps:get(ask, Funding, 0)),
        updated_at => maps:get(updated_at, Ticker, maps:get(updated_at, Funding, 0))
    }.

load_market(Req) ->
    case maps:get(market_snapshots, Req, undefined) of
        Snapshots when is_list(Snapshots) ->
            {group(Snapshots), []};
        _ ->
            Exchanges = maps:get(exchanges, Req),
            RowsAndWarnings = [fetch_exchange(E) || E <- Exchanges],
            Rows = lists:append([R || {R, _W} <- RowsAndWarnings]),
            Warnings = lists:append([W || {_R, W} <- RowsAndWarnings]),
            {group(Rows), Warnings}
    end.

fetch_exchange(Exchange) ->
    case arbiguard_market:fetch(Exchange) of
        {ok, Rows} -> {Rows, []};
        {error, Reason} ->
            Name = maps:get(name, Exchange, maps:get(id, Exchange, <<"unknown">>)),
            {[], [unicode:characters_to_binary(io_lib:format("~s fetch failed: ~p", [Name, Reason]))]}
    end.

group(Rows) ->
    lists:foldl(fun(Row0, Acc) ->
        Row = normalize_row(Row0),
        Symbol = maps:get(symbol, Row, <<"">>),
        case Symbol of
            <<"">> -> Acc;
            _ -> Acc#{Symbol => [Row | maps:get(Symbol, Acc, [])]}
        end
    end, #{}, Rows).

normalize_row(Row) ->
    Row#{
        exchange => lower(maps:get(exchange, Row, <<"">>)),
        symbol => upper(maps:get(symbol, Row, <<"">>)),
        mark_price => arbiguard_util:to_float(maps:get(mark_price, Row, maps:get(price, Row, 0)), 0),
        funding_rate => arbiguard_util:to_float(maps:get(funding_rate, Row, 0), 0),
        next_funding_time => arbiguard_util:to_int(maps:get(next_funding_time, Row, 0), 0),
        funding_interval_hours => arbiguard_util:to_float(maps:get(funding_interval_hours, Row, 8), 8),
        taker_fee_rate => arbiguard_util:to_float(maps:get(taker_fee_rate, Row, 0.0005), 0.0005),
        maker_fee_rate => arbiguard_util:to_float(maps:get(maker_fee_rate, Row, 0.0002), 0.0002),
        max_single_order_usdt => arbiguard_util:to_float(maps:get(max_single_order_usdt, Row, 0), 0),
        max_total_position_usdt => arbiguard_util:to_float(maps:get(max_total_position_usdt, Row, 0), 0),
        quote_volume => arbiguard_util:to_float(maps:get(quote_volume, Row, 0), 0),
        delist_time => arbiguard_util:to_int(maps:get(delist_time, Row, 0), 0)
    }.

build_all(Req, Market) ->
    maps:fold(fun(Symbol, Items, Acc) ->
        Acc ++ build_symbol(Req, Symbol, Items)
    end, [], Market).

build_symbol(Req, Symbol, Items) ->
    lists:filtermap(fun({Long, Short}) ->
        case arbiguard_calc:build_opportunity(Req, Symbol, Long, Short) of
            false -> false;
            Op -> {true, Op}
        end
    end, [{L, S} || L <- Items, S <- Items, L =/= S]).

sort_ops(Ops) ->
    lists:sort(fun(A, B) ->
        maps:get(expected_net_return, A, 0) >= maps:get(expected_net_return, B, 0)
    end, Ops).

limit(Ops, N) when is_integer(N), N > 0, length(Ops) > N ->
    lists:sublist(Ops, N);
limit(Ops, _) ->
    Ops.

monitor_mode(Ops) ->
    case lists:any(fun(Op) -> maps:get(in_execution_window, Op, false) =:= true end, Ops) of
        true -> <<"execution_window">>;
        false -> <<"screening">>
    end.

max_position_usdt(Req) ->
    Capital = maps:get(capital_usdt, Req, 10000.0),
    Pct = maps:get(max_position_pct, Req, 0.1),
    Margin = maps:get(execution_notional_usdt, Req, 200.0),
    Leverage = max(1.0, maps:get(paper_leverage, Req, 10.0)),
    MarginCap = case Pct > 0 of
        true -> Capital * Pct;
        false -> Margin
    end,
    max(0.0, min(Margin, MarginCap) * Leverage).

lower(V) -> string:lowercase(arbiguard_util:to_binary(V)).
upper(V) -> string:uppercase(arbiguard_util:to_binary(V)).

iso_now() ->
    {{Y, M, D}, {H, Min, S}} = calendar:universal_time(),
    list_to_binary(io_lib:format("~4..0B-~2..0B-~2..0BT~2..0B:~2..0B:~2..0BZ", [Y, M, D, H, Min, S])).
