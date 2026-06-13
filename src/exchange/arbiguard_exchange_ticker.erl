-module(arbiguard_exchange_ticker).
-behaviour(gen_server).

-export([start_link/1, start_ws/1, subscribe/3, unsubscribe/3, upsert_ticker/2, snapshot/1, name/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {exchange, id, subscriptions = #{}, ws_enabled = false,
                ws_connected = false, ws_status = <<"stopped">>, ws_error = undefined,
                ws_conn = undefined, ws_stream = undefined}).

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
    lager:info("ticker subscribe exchange=~s symbol=~s reason=~p", [State#state.id, Symbol, Reason]),
    NewState = maybe_ws_subscribe(State#state{subscriptions = Subs#{Symbol => Reason}}, Symbol),
    {reply, ok, NewState};
handle_call({unsubscribe, Symbol0, Reason}, _From, State = #state{subscriptions = Subs}) ->
    Symbol = norm_symbol(Symbol0),
    lager:info("ticker unsubscribe exchange=~s symbol=~s reason=~p", [State#state.id, Symbol, Reason]),
    NewState = maybe_ws_unsubscribe(State, Symbol),
    {reply, ok, NewState#state{subscriptions = maps:remove(Symbol, Subs)}};
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
handle_info({gun_ws, _ConnPid, _StreamRef, {text, Data}}, State) ->
    handle_ws_payload(Data, State),
    {noreply, State};
handle_info({gun_ws, _ConnPid, _StreamRef, {binary, Data}}, State) ->
    handle_ws_payload(Data, State),
    {noreply, State};
handle_info({gun_down, ConnPid, _Proto, Reason, _KilledStreams}, State = #state{ws_conn = ConnPid}) ->
    lager:warning("ticker ws down exchange=~s reason=~p", [State#state.id, Reason]),
    erlang:send_after(3000, self(), start_ws),
    {noreply, State#state{ws_connected = false, ws_status = <<"reconnecting">>, ws_error = fmt(Reason),
                          ws_conn = undefined, ws_stream = undefined}};
handle_info({gun_error, ConnPid, _StreamRef, Reason}, State = #state{ws_conn = ConnPid}) ->
    lager:warning("ticker ws error exchange=~s reason=~p", [State#state.id, Reason]),
    {noreply, State#state{ws_error = fmt(Reason)}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{ws_conn = ConnPid}) ->
    catch gun:close(ConnPid),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_start_ws(State = #state{ws_enabled = true, ws_connected = true}) ->
    State;
do_start_ws(State) ->
    case ws_endpoint(State#state.id) of
        {ok, Host, Port, Path} ->
            connect_ws(State, Host, Port, Path);
        {error, Reason} ->
            lager:warning("ticker ws unsupported exchange=~s reason=~s", [State#state.id, Reason]),
            State#state{ws_enabled = true, ws_connected = false, ws_status = <<"unsupported">>, ws_error = Reason}
    end.

connect_ws(State, Host, Port, Path) ->
    case gun:open(binary_to_list(Host), Port, #{transport => tls}) of
        {ok, ConnPid} ->
            case gun:await_up(ConnPid, 10000) of
                {ok, _Protocol} ->
                    StreamRef = gun:ws_upgrade(ConnPid, binary_to_list(Path)),
                    await_ws_upgrade(State, ConnPid, StreamRef, Host, Path);
                {error, Reason} ->
                    lager:warning("ticker ws await_up failed exchange=~s reason=~p", [State#state.id, Reason]),
                    catch gun:close(ConnPid),
                    reconnect(State, fmt(Reason))
            end;
        {error, Reason} ->
            lager:warning("ticker ws open failed exchange=~s reason=~p", [State#state.id, Reason]),
            reconnect(State, fmt(Reason))
    end.

await_ws_upgrade(State, ConnPid, StreamRef, Host, Path) ->
    case gun:await(ConnPid, StreamRef, 10000) of
        {upgrade, [<<"websocket">>], _Headers} ->
            Connected = State#state{ws_enabled = true, ws_connected = true,
                                    ws_status = <<"connected">>, ws_error = undefined,
                                    ws_conn = ConnPid, ws_stream = StreamRef},
            lager:info("ticker ws connected exchange=~s host=~s path=~s", [State#state.id, Host, Path]),
            replay_subscriptions(Connected);
        Other ->
            lager:warning("ticker ws upgrade failed exchange=~s result=~p", [State#state.id, Other]),
            catch gun:close(ConnPid),
            reconnect(State, fmt(Other))
    end.

reconnect(State, Reason) ->
    erlang:send_after(3000, self(), start_ws),
    State#state{ws_enabled = true, ws_connected = false, ws_status = <<"reconnecting">>, ws_error = Reason,
                ws_conn = undefined, ws_stream = undefined}.

replay_subscriptions(State = #state{id = <<"binance">>}) ->
    State;
replay_subscriptions(State = #state{subscriptions = Subs}) ->
    lists:foldl(fun(Symbol, Acc) -> maybe_ws_subscribe(Acc, Symbol) end, State, maps:keys(Subs)).

maybe_ws_subscribe(State = #state{ws_connected = true, ws_conn = ConnPid, ws_stream = StreamRef}, Symbol) ->
    case subscribe_payload(State#state.id, Symbol) of
        undefined -> State;
        Payload ->
            ok = gun:ws_send(ConnPid, StreamRef, {text, Payload}),
            State
    end;
maybe_ws_subscribe(State, _Symbol) ->
    State.

maybe_ws_unsubscribe(State = #state{ws_connected = true, ws_conn = ConnPid, ws_stream = StreamRef}, Symbol) ->
    case unsubscribe_payload(State#state.id, Symbol) of
        undefined -> State;
        Payload ->
            ok = gun:ws_send(ConnPid, StreamRef, {text, Payload}),
            State
    end;
maybe_ws_unsubscribe(State, _Symbol) ->
    State.

ws_endpoint(<<"binance">>) -> {ok, <<"fstream.binance.com">>, 443, <<"/ws/!bookTicker">>};
ws_endpoint(<<"okx">>) -> {ok, <<"ws.okx.com">>, 8443, <<"/ws/v5/public">>};
ws_endpoint(<<"gate">>) -> {ok, <<"fx-ws.gateio.ws">>, 443, <<"/v4/ws/usdt">>};
ws_endpoint(<<"htx">>) -> {ok, <<"api.hbdm.com">>, 443, <<"/linear-swap-ws">>};
ws_endpoint(<<"weex">>) -> {ok, <<"wspap.weex.com">>, 443, <<"/v2/ws/public">>};
ws_endpoint(_) -> {error, <<"websocket_endpoint_not_configured">>}.

subscribe_payload(<<"okx">>, Symbol) ->
    arbiguard_json:encode(#{op => <<"subscribe">>, args => [#{channel => <<"tickers">>, instId => okx_symbol(Symbol)}]});
subscribe_payload(<<"gate">>, Symbol) ->
    arbiguard_json:encode(#{time => erlang:system_time(second), channel => <<"futures.tickers">>,
                            event => <<"subscribe">>, payload => [gate_symbol(Symbol)]});
subscribe_payload(<<"htx">>, Symbol) ->
    Contract = htx_symbol(Symbol),
    arbiguard_json:encode(#{sub => <<"market.", Contract/binary, ".ticker">>, id => Symbol});
subscribe_payload(<<"weex">>, Symbol) ->
    arbiguard_json:encode(#{op => <<"subscribe">>, args => [#{instType => <<"mc">>, channel => <<"ticker">>, instId => Symbol}]});
subscribe_payload(_, _Symbol) ->
    undefined.

unsubscribe_payload(<<"okx">>, Symbol) ->
    arbiguard_json:encode(#{op => <<"unsubscribe">>, args => [#{channel => <<"tickers">>, instId => okx_symbol(Symbol)}]});
unsubscribe_payload(<<"gate">>, Symbol) ->
    arbiguard_json:encode(#{time => erlang:system_time(second), channel => <<"futures.tickers">>,
                            event => <<"unsubscribe">>, payload => [gate_symbol(Symbol)]});
unsubscribe_payload(_, _Symbol) ->
    undefined.

handle_ws_payload(Data, State) ->
    case decode_ws(Data) of
        {ok, Msg} -> maybe_write_ticker(Msg, State);
        _ -> ok
    end.

decode_ws(Data) ->
    try {ok, arbiguard_json:decode(Data)}
    catch _:_ -> error
    end.

maybe_write_ticker(Msg, #state{id = <<"binance">> = ID}) ->
    case maps:get(<<"s">>, Msg, maps:get(s, Msg, undefined)) of
        undefined -> ok;
        Symbol ->
            Bid = arbiguard_util:to_float(maps:get(<<"b">>, Msg, maps:get(b, Msg, 0)), 0),
            Ask = arbiguard_util:to_float(maps:get(<<"a">>, Msg, maps:get(a, Msg, 0)), 0),
            write_ticker(ID, Symbol, Bid, Ask, 0)
    end;
maybe_write_ticker(Msg, #state{id = <<"okx">> = ID}) ->
    Data = maps:get(<<"data">>, Msg, maps:get(data, Msg, [])),
    [write_ticker(ID,
                  undo_okx_symbol(maps:get(<<"instId">>, Row, maps:get(instId, Row, <<"">>))),
                  arbiguard_util:to_float(maps:get(<<"bidPx">>, Row, maps:get(bidPx, Row, 0)), 0),
                  arbiguard_util:to_float(maps:get(<<"askPx">>, Row, maps:get(askPx, Row, 0)), 0),
                  arbiguard_util:to_float(maps:get(<<"last">>, Row, maps:get(last, Row, 0)), 0))
     || Row <- Data],
    ok;
maybe_write_ticker(_Msg, _State) ->
    ok.

write_ticker(ID, Symbol0, Bid, Ask, Last) ->
    Symbol = norm_symbol(Symbol0),
    Mark = case Last > 0 of true -> Last; false -> (Bid + Ask) / 2 end,
    ok = arbiguard_ets:put_ticker(#{exchange => ID, symbol => Symbol, bid => Bid, ask => Ask,
                                    mark_price => Mark, updated_at => arbiguard_util:now_ms()}).

okx_symbol(Symbol) ->
    binary:replace(norm_symbol(Symbol), <<"USDT">>, <<"-USDT-SWAP">>).

undo_okx_symbol(Inst) ->
    binary:replace(binary:replace(Inst, <<"-USDT-SWAP">>, <<"USDT">>), <<"-">>, <<>>, [global]).

gate_symbol(Symbol) ->
    binary:replace(norm_symbol(Symbol), <<"USDT">>, <<"_USDT">>).

htx_symbol(Symbol) ->
    binary:replace(norm_symbol(Symbol), <<"USDT">>, <<"-USDT">>).

fmt(Term) ->
    unicode:characters_to_binary(io_lib:format("~p", [Term])).

norm_symbol(V) ->
    string:uppercase(arbiguard_util:to_binary(V)).
