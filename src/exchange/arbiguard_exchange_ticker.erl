-module(arbiguard_exchange_ticker).
-behaviour(gen_server).

-export([start_link/1, start_ws/1, subscribe/3, unsubscribe/3, upsert_ticker/2, snapshot/1, name/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {exchange, id, subscriptions = #{}, ws_enabled = false,
                ws_connected = false, ws_status = <<"stopped">>, ws_error = undefined}).

start_link(Exchange) ->
    ID = maps:get(id, Exchange),
    gen_server:start_link({local, name(ID)}, ?MODULE, [Exchange], []).

start_ws(ExchangeID) ->
    gen_server:call(name(ExchangeID), start_ws).

subscribe(ExchangeID, Symbol, Reason) ->
    gen_server:call(name(ExchangeID), {subscribe, Symbol, Reason}).

unsubscribe(ExchangeID, Symbol, Reason) ->
    gen_server:call(name(ExchangeID), {unsubscribe, Symbol, Reason}).

upsert_ticker(ExchangeID, Row) ->
    gen_server:cast(name(ExchangeID), {upsert_ticker, Row}).

snapshot(ExchangeID) ->
    gen_server:call(name(ExchangeID), snapshot).

name(ExchangeID) ->
    list_to_atom("arbiguard_ticker_" ++ binary_to_list(string:lowercase(arbiguard_util:to_binary(ExchangeID)))).

init([Exchange]) ->
    ID = maps:get(id, Exchange),
    case application:get_env(arbiguard, ticker_ws_enabled, true) of
        true -> self() ! start_ws;
        false -> ok
    end,
    {ok, #state{exchange = Exchange, id = ID}}.

handle_call(start_ws, _From, State) ->
    {reply, ok, do_start_ws(State)};
handle_call({subscribe, Symbol0, Reason}, _From, State = #state{subscriptions = Subs}) ->
    Symbol = norm_symbol(Symbol0),
    NewSubs = Subs#{Symbol => Reason},
    lager:log(info, self(), "ticker subscribe exchange=~s symbol=~s reason=~p", [State#state.id, Symbol, Reason]),
    {reply, ok, State#state{subscriptions = NewSubs}};
handle_call({unsubscribe, Symbol0, Reason}, _From, State = #state{subscriptions = Subs}) ->
    Symbol = norm_symbol(Symbol0),
    lager:log(info, self(), "ticker unsubscribe exchange=~s symbol=~s reason=~p", [State#state.id, Symbol, Reason]),
    {reply, ok, State#state{subscriptions = maps:remove(Symbol, Subs)}};
handle_call(snapshot, _From, State = #state{subscriptions = Subs}) ->
    {reply, #{exchange => State#state.id,
              ws_enabled => State#state.ws_enabled,
              ws_connected => State#state.ws_connected,
              ws_status => State#state.ws_status,
              ws_error => State#state.ws_error,
              subscriptions => maps:keys(Subs)}, State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({upsert_ticker, Row0}, State = #state{id = ID}) ->
    Row = Row0#{exchange => ID,
                symbol => norm_symbol(maps:get(symbol, Row0, <<"">>)),
                updated_at => arbiguard_util:now_ms()},
    ok = arbiguard_ets:put_ticker(Row),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(start_ws, State) ->
    {noreply, do_start_ws(State)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_start_ws(State = #state{ws_enabled = true}) ->
    State;
do_start_ws(State) ->
    %% There is no concrete websocket adapter wired yet. Keep subscription
    %% ownership here, but do not report a fake connection.
    Error = <<"websocket_adapter_not_implemented">>,
    lager:log(warning, self(), "ticker ws not connected exchange=~s reason=~s", [State#state.id, Error]),
    State#state{ws_enabled = true,
                ws_connected = false,
                ws_status = <<"adapter_missing">>,
                ws_error = Error}.

norm_symbol(V) ->
    string:uppercase(arbiguard_util:to_binary(V)).
