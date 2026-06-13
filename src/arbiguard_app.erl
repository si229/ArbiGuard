-module(arbiguard_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    ok = application:ensure_started(inets),
    ok = application:ensure_started(ssl),
    arbiguard_sup:start_link().

stop(_State) ->
    ok.
