-module(arbiguard_exchange_funding).
-behaviour(gen_server).

-export([start_link/1, refresh/1, snapshot/1, name/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {exchange, id, refresh_ms = 60000, last_refresh = 0, last_count = 0, last_error = undefined}).

start_link(Exchange) ->
    ID = maps:get(id, Exchange),
    gen_server:start_link({local, name(ID)}, ?MODULE, [Exchange], []).

refresh(ExchangeID) ->
    gen_server:call(name(ExchangeID), refresh, 30000).

snapshot(ExchangeID) ->
    gen_server:call(name(ExchangeID), snapshot).

name(ExchangeID) ->
    list_to_atom("arbiguard_funding_" ++ binary_to_list(string:lowercase(arbiguard_util:to_binary(ExchangeID)))).

init([Exchange]) ->
    RefreshMs = application:get_env(arbiguard, funding_refresh_ms, 60000),
    self() ! refresh,
    {ok, #state{exchange = Exchange, id = maps:get(id, Exchange), refresh_ms = RefreshMs}}.

handle_call(refresh, _From, State) ->
    {Reply, NewState} = do_refresh(State),
    {reply, Reply, NewState};
handle_call(snapshot, _From, State) ->
    {reply, #{exchange => State#state.id,
              refresh_ms => State#state.refresh_ms,
              last_refresh => State#state.last_refresh,
              last_count => State#state.last_count,
              last_error => State#state.last_error}, State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(refresh, State = #state{refresh_ms = RefreshMs}) ->
    {_Reply, NewState} = do_refresh(State),
    erlang:send_after(RefreshMs, self(), refresh),
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_refresh(State = #state{exchange = Exchange, id = ID}) ->
    case arbiguard_market:fetch(Exchange) of
        {ok, Rows} ->
            [store_market_row(Row) || Row <- Rows],
            lager:log(info, self(), "funding refresh exchange=~s rows=~p", [ID, length(Rows)]),
            {ok, State#state{last_refresh = arbiguard_util:now_ms(), last_count = length(Rows), last_error = undefined}};
        {error, Reason} ->
            lager:log(warning, self(), "funding refresh failed exchange=~s reason=~p", [ID, Reason]),
            {{error, Reason}, State#state{last_refresh = arbiguard_util:now_ms(), last_error = Reason}}
    end.

store_market_row(Row) ->
    ok = arbiguard_ets:put_funding(Row),
    %% REST funding snapshots also carry mark price. Use them as a baseline
    %% until the ticker WS process replaces prices with live ticker values.
    ok = arbiguard_ets:put_ticker(Row),
    ok.
