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
    Exchanges = arbiguard_calc:default_exchanges(),
    ExchangeChildren = exchange_children(Exchanges),
    CoreChildren = [
        #{id => arbiguard_state,
          start => {arbiguard_state, start_link, [Capital]},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [arbiguard_state]},
        #{id => arbiguard_live_account,
          start => {arbiguard_live_account, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [arbiguard_live_account]},
        #{id => arbiguard_executor,
          start => {arbiguard_executor, start_link, []},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [arbiguard_executor]}
    ],
    TailChildren = [
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
