-module(arbiguard_scanner).

-export([scan/1]).

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
    Exec = maps:get(execution_notional_usdt, Req, 200.0),
    max(Exec, Capital * Pct).

lower(V) -> string:lowercase(arbiguard_util:to_binary(V)).
upper(V) -> string:uppercase(arbiguard_util:to_binary(V)).

iso_now() ->
    {{Y, M, D}, {H, Min, S}} = calendar:universal_time(),
    list_to_binary(io_lib:format("~4..0B-~2..0B-~2..0BT~2..0B:~2..0B:~2..0BZ", [Y, M, D, H, Min, S])).
