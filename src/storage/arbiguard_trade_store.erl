-module(arbiguard_trade_store).

-export([init/0, write/1, recent/1, page/1, stats/1, reset_account/2]).

-record(trade_history, {id,
                        time = 0,
                        action = <<"">>,
                        account_id = <<"">>,
                        account_mode = <<"">>,
                        symbol = <<"">>,
                        long_exchange = <<"">>,
                        short_exchange = <<"">>,
                        data = #{}}).

-record(trade_stats, {key,
                      account_mode = <<"">>,
                      account_id = <<"">>,
                      scope = <<"">>,
                      pair = <<"">>,
                      data = #{}}).

init() ->
    Dir0 = application:get_env(arbiguard, mnesia_dir, "data/mnesia"),
    Dir = filename:absname(Dir0),
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    ok = application:load(mnesia),
    ok = application:set_env(mnesia, dir, Dir),
    ensure_schema(),
    ok = application:ensure_started(mnesia),
    ensure_history_table(),
    ensure_stats_table().

write(Trade0) when is_map(Trade0) ->
    Trade = ensure_trade_id(Trade0),
    Rec = #trade_history{id = maps:get(history_id, Trade),
                         time = maps:get(time, Trade, 0),
                         action = maps:get(action, Trade, <<"">>),
                         account_id = maps:get(account_id, Trade, <<"">>),
                         account_mode = maps:get(account_mode, Trade, <<"">>),
                         symbol = maps:get(symbol, Trade, <<"">>),
                         long_exchange = maps:get(long_exchange, Trade, <<"">>),
                         short_exchange = maps:get(short_exchange, Trade, <<"">>),
                         data = Trade},
    case mnesia:transaction(fun() -> mnesia:write(Rec) end) of
        {atomic, ok} ->
            update_stats(Trade),
            Trade;
        {aborted, Reason} ->
            lager:warning("trade history write failed reason=~p trade=~p", [Reason, Trade]),
            Trade
    end;
write(Trade) ->
    Trade.

recent(Limit0) ->
    Limit = max(0, arbiguard_util:to_int(Limit0, 200)),
    Rows = case catch mnesia:dirty_match_object(#trade_history{id = '_',
                                                               time = '_',
                                                               action = '_',
                                                               account_id = '_',
                                                               account_mode = '_',
                                                               symbol = '_',
                                                               long_exchange = '_',
                                                               short_exchange = '_',
                                                               data = '_'}) of
        {'EXIT', Reason} ->
            lager:warning("trade history read failed reason=~p", [Reason]),
            [];
        Items when is_list(Items) ->
            Items;
        _ ->
            []
    end,
    Sorted = lists:sort(fun(A, B) -> A#trade_history.time >= B#trade_history.time end, Rows),
    [R#trade_history.data || R <- lists:sublist(Sorted, Limit)].

page(Params) ->
    AccountMode = normalize_optional(maps:get(account_mode, Params, undefined)),
    AccountID = normalize_optional(maps:get(account_id, Params, undefined)),
    Action = normalize_optional(maps:get(action, Params, undefined)),
    Page = max(1, arbiguard_util:to_int(maps:get(page, Params, 1), 1)),
    PageSize = min(500, max(1, arbiguard_util:to_int(maps:get(page_size, Params, 50), 50))),
    Rows0 = all_history(),
    Rows1 = [R || R <- Rows0,
                  match_optional(AccountMode, R#trade_history.account_mode),
                  match_optional(AccountID, R#trade_history.account_id),
                  match_optional(Action, R#trade_history.action)],
    Sorted = lists:sort(fun(A, B) -> A#trade_history.time >= B#trade_history.time end, Rows1),
    Total = length(Sorted),
    Offset = (Page - 1) * PageSize,
    Items = slice(Sorted, Offset, PageSize),
    #{page => Page,
      page_size => PageSize,
      total => Total,
      total_pages => case Total of 0 -> 0; _ -> (Total + PageSize - 1) div PageSize end,
      trades => [R#trade_history.data || R <- Items]}.

stats(Params) ->
    AccountMode = normalize_optional(maps:get(account_mode, Params, undefined)),
    AccountID = normalize_optional(maps:get(account_id, Params, undefined)),
    Rows = all_stats(),
    Filtered = [R || R <- Rows,
                     match_optional(AccountMode, R#trade_stats.account_mode),
                     match_optional(AccountID, R#trade_stats.account_id)],
    #{profit_breakdown => stats_scope(<<"account">>, Filtered),
      pair_stats => sort_pair_stats([R#trade_stats.data || R <- Filtered, R#trade_stats.scope =:= <<"pair">>])}.

reset_account(AccountMode0, AccountID0) ->
    AccountMode = norm_bin(AccountMode0),
    AccountID = norm_bin(AccountID0),
    _ = mnesia:transaction(fun() ->
        [mnesia:delete({trade_stats, R#trade_stats.key}) || R <- all_stats_tx(),
                                                        R#trade_stats.account_mode =:= AccountMode,
                                                        R#trade_stats.account_id =:= AccountID],
        ok
    end),
    ok.

ensure_schema() ->
    case mnesia:system_info(is_running) of
        yes ->
            ok;
        _ ->
            case mnesia:create_schema([node()]) of
                ok -> ok;
                {error, {_, {already_exists, _}}} -> ok;
                {error, {already_exists, _}} -> ok;
                {error, Reason} ->
                    lager:warning("mnesia schema create skipped reason=~p", [Reason]),
                    ok
            end
    end.

ensure_history_table() ->
    Attrs = record_info(fields, trade_history),
    case mnesia:create_table(trade_history, [{attributes, Attrs},
                                             {disc_copies, [node()]},
                                             {type, set}]) of
        {atomic, ok} ->
            lager:info("trade history mnesia table created"),
            ok;
        {aborted, {already_exists, trade_history}} ->
            ok;
        {aborted, Reason} ->
            lager:warning("trade history mnesia table create skipped reason=~p", [Reason]),
            ok
    end.

ensure_stats_table() ->
    Attrs = record_info(fields, trade_stats),
    case mnesia:create_table(trade_stats, [{attributes, Attrs},
                                           {disc_copies, [node()]},
                                           {type, set}]) of
        {atomic, ok} ->
            lager:info("trade stats mnesia table created"),
            ok;
        {aborted, {already_exists, trade_stats}} ->
            ok;
        {aborted, Reason} ->
            lager:warning("trade stats mnesia table create skipped reason=~p", [Reason]),
            ok
    end.

ensure_trade_id(Trade) ->
    case maps:get(history_id, Trade, undefined) of
        undefined ->
            Trade#{history_id => new_id(Trade)};
        _ ->
            Trade
    end.

new_id(Trade) ->
    Now = arbiguard_util:now_ms(),
    Unique = erlang:unique_integer([monotonic, positive]),
    Hash = erlang:phash2(Trade),
    unicode:characters_to_binary(io_lib:format("trade_~p_~p_~p", [Now, Unique, Hash])).

update_stats(Trade) ->
    case mnesia:transaction(fun() ->
        update_scope_stats(<<"account">>, <<"">>, Trade),
        update_scope_stats(<<"pair">>, pair_key(Trade), Trade)
    end) of
        {atomic, _} -> ok;
        {aborted, Reason} ->
            lager:warning("trade stats update failed reason=~p trade=~p", [Reason, Trade]),
            ok
    end.

update_scope_stats(Scope, Pair, Trade) ->
    AccountMode = norm_bin(maps:get(account_mode, Trade, <<"paper">>)),
    AccountID = norm_bin(maps:get(account_id, Trade, default_account_id(AccountMode))),
    Key = stats_key(AccountMode, AccountID, Scope, Pair),
    Old = case mnesia:read(trade_stats, Key, write) of
        [#trade_stats{data = Data}] -> Data;
        [] -> base_stats(AccountMode, AccountID, Scope, Pair)
    end,
    New = apply_trade_to_stats(Old, Trade),
    mnesia:write(#trade_stats{key = Key,
                              account_mode = AccountMode,
                              account_id = AccountID,
                              scope = Scope,
                              pair = Pair,
                              data = New}).

apply_trade_to_stats(Stats, Trade) ->
    Action = maps:get(action, Trade, <<"">>),
    Net = f(maps:get(net_pnl, Trade, 0)),
    Price = f(maps:get(price_pnl, Trade, 0)),
    Funding = f(maps:get(funding_pnl, Trade, 0)),
    OpenFee = case Action of <<"open">> -> f(maps:get(open_fee, Trade, maps:get(fee, Trade, 0))); _ -> 0.0 end,
    CloseFee = case Action of <<"close">> -> f(maps:get(close_fee, Trade, maps:get(fee, Trade, 0))); _ -> 0.0 end,
    Stats#{
        net_pnl => f(maps:get(net_pnl, Stats, 0)) + Net,
        realized_net_pnl => f(maps:get(realized_net_pnl, Stats, 0)) + Net,
        price_pnl => f(maps:get(price_pnl, Stats, 0)) + Price,
        realized_price_pnl => f(maps:get(realized_price_pnl, Stats, 0)) + Price,
        funding_pnl => f(maps:get(funding_pnl, Stats, 0)) + Funding,
        realized_funding_pnl => f(maps:get(realized_funding_pnl, Stats, 0)) + Funding,
        open_fee => f(maps:get(open_fee, Stats, 0)) + OpenFee,
        close_fee => f(maps:get(close_fee, Stats, 0)) + CloseFee,
        total_fee => f(maps:get(total_fee, Stats, 0)) + OpenFee + CloseFee,
        slippage_pnl => f(maps:get(slippage_pnl, Stats, 0)) + f(maps:get(slippage_pnl, Trade, 0)),
        rollback_pnl => f(maps:get(rollback_pnl, Stats, 0)) + f(maps:get(rollback_pnl, Trade, 0)),
        liquidation_pnl => f(maps:get(liquidation_pnl, Stats, 0)) + f(maps:get(liquidation_pnl, Trade, 0)),
        trade_count => arbiguard_util:to_int(maps:get(trade_count, Stats, 0), 0) + 1,
        open_count => arbiguard_util:to_int(maps:get(open_count, Stats, 0), 0) + case Action of <<"open">> -> 1; _ -> 0 end,
        close_count => arbiguard_util:to_int(maps:get(close_count, Stats, 0), 0) + case Action of <<"close">> -> 1; _ -> 0 end,
        updated_at => arbiguard_util:now_ms()
    }.

base_stats(AccountMode, AccountID, Scope, Pair) ->
    #{account_mode => AccountMode,
      account_id => AccountID,
      scope => Scope,
      pair => Pair,
      net_pnl => 0.0,
      realized_net_pnl => 0.0,
      unrealized_net_pnl => 0.0,
      price_pnl => 0.0,
      realized_price_pnl => 0.0,
      unrealized_price_pnl => 0.0,
      funding_pnl => 0.0,
      realized_funding_pnl => 0.0,
      unrealized_funding_pnl => 0.0,
      open_fee => 0.0,
      close_fee => 0.0,
      estimated_close_fee => 0.0,
      total_fee => 0.0,
      slippage_pnl => 0.0,
      rollback_pnl => 0.0,
      liquidation_pnl => 0.0,
      other_pnl => 0.0,
      trade_count => 0,
      open_count => 0,
      close_count => 0,
      position_count => 0,
      updated_at => 0}.

stats_scope(Scope, Rows) ->
    case [R#trade_stats.data || R <- Rows, R#trade_stats.scope =:= Scope] of
        [Data | _] -> Data;
        [] -> base_stats(<<"">>, <<"">>, Scope, <<"">>)
    end.

sort_pair_stats(Items) ->
    lists:sort(fun(A, B) -> f(maps:get(net_pnl, A, 0)) >= f(maps:get(net_pnl, B, 0)) end, Items).

all_history() ->
    case catch mnesia:dirty_match_object(#trade_history{id = '_',
                                                        time = '_',
                                                        action = '_',
                                                        account_id = '_',
                                                        account_mode = '_',
                                                        symbol = '_',
                                                        long_exchange = '_',
                                                        short_exchange = '_',
                                                        data = '_'}) of
        Items when is_list(Items) -> Items;
        _ -> []
    end.

all_stats() ->
    case catch mnesia:dirty_match_object(#trade_stats{key = '_',
                                                      account_mode = '_',
                                                      account_id = '_',
                                                      scope = '_',
                                                      pair = '_',
                                                      data = '_'}) of
        Items when is_list(Items) -> Items;
        _ -> []
    end.

all_stats_tx() ->
    mnesia:match_object(#trade_stats{key = '_',
                                     account_mode = '_',
                                     account_id = '_',
                                     scope = '_',
                                     pair = '_',
                                     data = '_'}).

slice(List, Offset, Limit) ->
    lists:sublist(drop(Offset, List), Limit).

drop(N, List) when N =< 0 -> List;
drop(_N, []) -> [];
drop(N, [_ | Rest]) -> drop(N - 1, Rest).

match_optional(undefined, _Value) -> true;
match_optional(<<"">>, _Value) -> true;
match_optional(Value, Value) -> true;
match_optional(_Filter, _Value) -> false.

normalize_optional(undefined) -> undefined;
normalize_optional(V) -> norm_bin(V).

stats_key(AccountMode, AccountID, Scope, Pair) ->
    <<AccountMode/binary, "|", AccountID/binary, "|", Scope/binary, "|", Pair/binary>>.

pair_key(Trade) ->
    <<(norm_bin(maps:get(long_exchange, Trade, <<"">>)))/binary, "->",
      (norm_bin(maps:get(short_exchange, Trade, <<"">>)))/binary>>.

default_account_id(<<"live">>) -> <<"live-main">>;
default_account_id(_) -> <<"paper-main">>.

norm_bin(V) -> arbiguard_util:to_binary(V).

f(V) -> arbiguard_util:to_float(V, 0.0).
