-module(arbiguard_symbol_watcher).
-behaviour(gen_server).

-export([start_link/1, refresh/0, snapshot/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {exchanges = [], symbols = #{}, last_refresh = 0, last_error = undefined,
                enabled = true, subscribe_all = true, interval_ms = 60000}).

start_link(Exchanges) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Exchanges], []).

refresh() ->
    gen_server:call(?MODULE, refresh, 120000).

snapshot() ->
    gen_server:call(?MODULE, snapshot).

init([Exchanges0]) ->
    Enabled = application:get_env(arbiguard, symbol_watch_enabled, true),
    SubscribeAll = application:get_env(arbiguard, subscribe_all_tickers, true),
    Interval = application:get_env(arbiguard, symbol_watch_interval_ms, 60000),
    Exchanges = enabled_exchanges(Exchanges0),
    case Enabled of
        true -> self() ! refresh;
        false -> ok
    end,
    {ok, #state{exchanges = Exchanges, enabled = Enabled,
                subscribe_all = SubscribeAll, interval_ms = Interval}}.

handle_call(refresh, _From, State) ->
    {Reply, NewState} = do_refresh(State),
    {reply, Reply, NewState};
handle_call(snapshot, _From, State) ->
    {reply, state_snapshot(State), State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(refresh, State = #state{enabled = true, interval_ms = Interval}) ->
    {_Reply, NewState} = do_refresh(State),
    erlang:send_after(Interval, self(), refresh),
    {noreply, NewState};
handle_info(refresh, State) ->
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_refresh(State = #state{exchanges = Exchanges}) ->
    try
        {Summaries, Symbols} = refresh_exchanges(Exchanges, State#state.symbols, State#state.subscribe_all),
        Reply = #{ok => true, exchanges => Summaries},
        {Reply, State#state{symbols = Symbols, last_refresh = arbiguard_util:now_ms(), last_error = undefined}}
    catch
        Class:Reason ->
            Error = unicode:characters_to_binary(io_lib:format("~p:~p", [Class, Reason])),
            lager:warning("symbol watcher refresh failed reason=~s", [Error]),
            {#{ok => false, error => Error}, State#state{last_error = Error}}
    end.

refresh_exchanges([], Symbols, _SubscribeAll) ->
    {[], Symbols};
refresh_exchanges([Exchange | Rest], Symbols0, SubscribeAll) ->
    ID = maps:get(id, Exchange),
    Old = maps:get(ID, Symbols0, sets:new([{version, 2}])),
    {Summary, New} = refresh_exchange(Exchange, Old, SubscribeAll),
    Symbols1 = Symbols0#{ID => New},
    {Summaries, Symbols} = refresh_exchanges(Rest, Symbols1, SubscribeAll),
    {[Summary | Summaries], Symbols}.

refresh_exchange(Exchange, Old, SubscribeAll) ->
    ID = maps:get(id, Exchange),
    case arbiguard_market:fetch(Exchange) of
        {ok, Rows} ->
            New = sets:from_list([maps:get(symbol, R) || R <- Rows,
                                  maps:get(symbol, R, <<"">>) =/= <<"">>], [{version, 2}]),
            Added = sets:to_list(sets:subtract(New, Old)),
            Removed = sets:to_list(sets:subtract(Old, New)),
            maybe_subscribe_all(SubscribeAll, ID, Added),
            maybe_unsubscribe_all(ID, Removed),
            lager:info("symbol watcher exchange=~s symbols=~p added=~p removed=~p",
                       [ID, sets:size(New), length(Added), length(Removed)]),
            {#{exchange => ID, symbols => sets:size(New), added => length(Added), removed => length(Removed)}, New};
        {error, Reason} ->
            Error = unicode:characters_to_binary(io_lib:format("~p", [Reason])),
            lager:warning("symbol watcher exchange=~s failed reason=~s", [ID, Error]),
            {#{exchange => ID, symbols => sets:size(Old), added => 0, removed => 0, error => Error}, Old}
    end.

maybe_subscribe_all(false, _ID, _Added) ->
    ok;
maybe_subscribe_all(true, ID, Added) ->
    [catch arbiguard_exchange_ticker:subscribe(ID, Symbol, symbol_watcher) || Symbol <- Added],
    ok.

maybe_unsubscribe_all(ID, Removed) ->
    [catch arbiguard_exchange_ticker:unsubscribe(ID, Symbol, symbol_watcher) || Symbol <- Removed],
    ok.

enabled_exchanges(Exchanges) ->
    [E || E <- Exchanges, maps:get(enabled, E, true) =:= true].

state_snapshot(State = #state{symbols = Symbols}) ->
    #{enabled => State#state.enabled,
      subscribe_all => State#state.subscribe_all,
      interval_ms => State#state.interval_ms,
      last_refresh => State#state.last_refresh,
      last_error => State#state.last_error,
      exchanges => [#{exchange => ID, symbols => sets:size(Set)}
                    || {ID, Set} <- maps:to_list(Symbols)]}.
