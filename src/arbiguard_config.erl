-module(arbiguard_config).

-export([snapshot/0, get/2]).

snapshot() ->
    #{http_port => get(http_port, 8771),
      paper_capital_usdt => get(paper_capital_usdt, 10000.0),
      default_scan => get(default_scan, #{}),
      exchanges => get(exchanges, []),
      execution => get(execution, #{})}.

get(Key, Default) ->
    application:get_env(arbiguard, Key, Default).
