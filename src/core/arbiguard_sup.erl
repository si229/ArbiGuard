-module(arbiguard_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ok = arbiguard_ets:init(),
    Port = application:get_env(arbiguard, http_port, 8771),
    Capital = application:get_env(arbiguard, paper_capital_usdt, 10000.0),
    Exchanges0 = application:get_env(arbiguard, exchanges, arbiguard_calc:default_exchanges()),
    Exchanges = enabled_exchanges(Exchanges0),
    ExchangeChildren = exchange_children(Exchanges),
    CoreChildren = [
        #{id => arbiguard_state,
          start => {arbiguard_state, start_link, [Capital]},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [arbiguard_state]},
        #{id => arbiguard_open_executor,
          start => {arbiguard_open_executor, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [arbiguard_open_executor]},
        #{id => arbiguard_close_executor,
          start => {arbiguard_close_executor, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [arbiguard_close_executor]},
        #{id => arbiguard_account_manager,
          start => {arbiguard_account_manager, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [arbiguard_account_manager]}
    ],
    TailChildren = [
        #{id => arbiguard_symbol_watcher,
          start => {arbiguard_symbol_watcher, start_link, [Exchanges]},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [arbiguard_symbol_watcher]},
        #{id => arbiguard_scanner,
          start => {arbiguard_scanner, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [arbiguard_scanner]},
        #{id => arbiguard_http,
          start => {arbiguard_http, start_link, [Port]},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [arbiguard_http]}
    ],
    Children = CoreChildren ++ ExchangeChildren ++ TailChildren,
    {ok, {{one_for_one, 5, 10}, Children}}.

exchange_children(Exchanges) ->
    lists:append([exchange_child_specs(E) || E <- Exchanges]).

enabled_exchanges(Exchanges) ->
    [E || E <- Exchanges, maps:get(enabled, E, true) =:= true].

exchange_child_specs(Exchange) ->
    [ticker_child(Exchange), funding_child(Exchange)].

ticker_child(Exchange) ->
    #{id => ticker_child_id(maps:get(id, Exchange)),
      start => {arbiguard_exchange_ticker, start_link, [Exchange]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [arbiguard_exchange_ticker]}.

funding_child(Exchange) ->
    #{id => funding_child_id(maps:get(id, Exchange)),
      start => {arbiguard_exchange_funding, start_link, [Exchange]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [arbiguard_exchange_funding]}.

ticker_child_id(ID) ->
    list_to_atom("ticker_" ++ binary_to_list(ID)).

funding_child_id(ID) ->
    list_to_atom("funding_" ++ binary_to_list(ID)).
