-module(arbiguard_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Port = application:get_env(arbiguard, http_port, 8771),
    Capital = application:get_env(arbiguard, paper_capital_usdt, 10000.0),
    Children = [
        #{id => arbiguard_state,
          start => {arbiguard_state, start_link, [Capital]},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [arbiguard_state]},
        #{id => arbiguard_http,
          start => {arbiguard_http, start_link, [Port]},
          restart => permanent,
          shutdown => 5000,
          type => worker,
          modules => [arbiguard_http]}
    ],
    {ok, {{one_for_one, 5, 10}, Children}}.
