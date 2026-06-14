-module(arbiguard_processes).

-export([snapshot/0]).

snapshot() ->
    Exchanges0 = application:get_env(arbiguard, exchanges, arbiguard_calc:default_exchanges()),
    Exchanges = enabled_exchanges(Exchanges0),
    #{ets => ets_info(),
      scanner => safe_call(fun arbiguard_scanner:snapshot/0),
      symbol_watcher => safe_call(fun arbiguard_symbol_watcher:snapshot/0),
      executor => safe_call(fun arbiguard_executor:snapshot/0),
      account => account_brief(),
      live_account => safe_call(fun arbiguard_live_account:snapshot/0),
      exchanges => [exchange_snapshot(E) || E <- Exchanges]}.

enabled_exchanges(Exchanges) ->
    [E || E <- Exchanges, maps:get(enabled, E, true) =:= true].

ets_info() ->
    #{tickers => length(arbiguard_ets:all_tickers()),
      funding => length(arbiguard_ets:all_funding()),
      opportunities => length(arbiguard_ets:all_opportunities())}.

exchange_snapshot(Exchange) ->
    ID = maps:get(id, Exchange),
    #{exchange => ID,
      ticker => safe_call(fun() -> arbiguard_exchange_ticker:snapshot(ID) end),
      funding => safe_call(fun() -> arbiguard_exchange_funding:snapshot(ID) end),
      private_ws => safe_call(fun() -> arbiguard_private_ws:snapshot(ID) end)}.

account_brief() ->
    Snapshot = safe_call(fun arbiguard_state:snapshot/0),
    case is_map(Snapshot) of
        true ->
            #{equity => maps:get(equity, Snapshot, 0),
              balance => maps:get(balance, Snapshot, 0),
              positions => length(maps:get(positions, Snapshot, [])),
              token_exchanges => maps:get(token_exchanges, Snapshot, [])};
        false -> Snapshot
    end.

safe_call(Fun) ->
    try Fun()
    catch
        Class:Reason -> #{error => unicode:characters_to_binary(io_lib:format("~p:~p", [Class, Reason]))}
    end.
