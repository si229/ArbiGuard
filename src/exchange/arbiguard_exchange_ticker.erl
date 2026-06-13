-module(arbiguard_exchange_ticker).
-behaviour(gen_server).

-export([start_link/1, start_ws/1, set_ws_endpoint/4, subscribe/3, unsubscribe/3, upsert_ticker/2, snapshot/1, name/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {exchange, id, subscriptions = #{}, ws_enabled = false,
                ws_connected = false, ws_status = <<"stopped">>, ws_error = undefined,
                ws_conn = undefined, ws_stream = undefined}).

start_link(Exchange) ->
    ID = maps:get(id, Exchange),
    gen_server:start_link({local, name(ID)}, ?MODULE, [Exchange], []).

start_ws(ExchangeID) ->
    gen_server:call(name(ExchangeID), start_ws).

set_ws_endpoint(ExchangeID, Host, Port, Path) ->
    gen_server:call(name(ExchangeID), {set_ws_endpoint, Host, Port, Path}).

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
handle_call({set_ws_endpoint, Host0, Port0, Path0}, _From, State) ->
    Host = arbiguard_util:to_binary(Host0),
    Port = arbiguard_util:to_int(Port0, 443),
    Path = ensure_path(arbiguard_util:to_binary(Path0)),
    Exchange0 = State#state.exchange,
    Exchange = Exchange0#{ws_host => Host, ws_port => Port, ws_path => Path},
    lager:warning("ticker ws endpoint updated exchange=~s host=~s port=~p path=~s",
                  [State#state.id, Host, Port, Path]),
    ClosedState = close_ws(State),
    NewState0 = ClosedState#state{exchange = Exchange,
                                  ws_connected = false,
                                  ws_status = <<"endpoint_updated">>,
                                  ws_error = undefined,
                                  ws_conn = undefined,
                                  ws_stream = undefined},
    {reply, #{ok => true, exchange => State#state.id, ws_host => Host, ws_port => Port, ws_path => Path},
     do_start_ws(NewState0)};
handle_call({subscribe, Symbol0, Reason}, _From, State = #state{subscriptions = Subs}) ->
    Symbol = norm_symbol(Symbol0),
    OldReasons = maps:get(Symbol, Subs, []),
    AlreadyWsSubscribed = has_exchange_ws_reason(OldReasons),
    NewReasons = add_reason(Reason, OldReasons),
    ShouldWsSubscribe = has_exchange_ws_reason(NewReasons) andalso not AlreadyWsSubscribed,
    lager:info("ticker local subscribe exchange=~s symbol=~s reason=~p ws_subscribe=~p reasons=~p",
               [State#state.id, Symbol, Reason, ShouldWsSubscribe, NewReasons]),
    State1 = State#state{subscriptions = Subs#{Symbol => NewReasons}},
    NewState = case ShouldWsSubscribe of
        true -> maybe_ws_subscribe(State1, Symbol);
        false -> State1
    end,
    {reply, ok, NewState};
handle_call({unsubscribe, Symbol0, Reason}, _From, State = #state{subscriptions = Subs}) ->
    Symbol = norm_symbol(Symbol0),
    OldReasons = maps:get(Symbol, Subs, []),
    NewReasons = remove_reason(Reason, OldReasons),
    ShouldWsUnsubscribe = has_exchange_ws_reason(OldReasons) andalso not has_exchange_ws_reason(NewReasons),
    lager:info("ticker local unsubscribe exchange=~s symbol=~s reason=~p ws_unsubscribe=~p remaining=~p",
               [State#state.id, Symbol, Reason, ShouldWsUnsubscribe, NewReasons]),
    State1 = case ShouldWsUnsubscribe of
        true -> maybe_ws_unsubscribe(State, Symbol);
        false -> State
    end,
    State2 = case NewReasons of
        [] -> State1#state{subscriptions = maps:remove(Symbol, Subs)};
        _ -> State1#state{subscriptions = Subs#{Symbol => NewReasons}}
    end,
    {reply, ok, State2};
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
    handle_ws_payload(maybe_gunzip(Data), State),
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
    case ws_endpoint(State) of
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

close_ws(State = #state{ws_conn = undefined}) ->
    State;
close_ws(State = #state{ws_conn = ConnPid}) ->
    catch gun:close(ConnPid),
    State.

replay_subscriptions(State = #state{id = <<"binance">>}) ->
    State;
replay_subscriptions(State = #state{subscriptions = Subs}) ->
    lists:foldl(fun
        (Symbol, Acc) ->
            case has_exchange_ws_reason(maps:get(Symbol, Subs, [])) of
                true -> maybe_ws_subscribe(Acc, Symbol);
                false -> Acc
            end
    end, State, maps:keys(Subs)).

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

ws_endpoint(#state{exchange = Exchange}) ->
    Host = maps:get(ws_host, Exchange, <<"">>),
    Port = arbiguard_util:to_int(maps:get(ws_port, Exchange, 443), 443),
    Path = ensure_path(maps:get(ws_path, Exchange, <<"/">>)),
    case Host =/= <<"">> andalso Port > 0 of
        true -> {ok, Host, Port, Path};
        false -> {error, <<"websocket_endpoint_not_configured">>}
    end.

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
unsubscribe_payload(<<"htx">>, Symbol) ->
    Contract = htx_symbol(Symbol),
    arbiguard_json:encode(#{unsub => <<"market.", Contract/binary, ".ticker">>, id => Symbol});
unsubscribe_payload(<<"weex">>, Symbol) ->
    arbiguard_json:encode(#{op => <<"unsubscribe">>, args => [#{instType => <<"mc">>, channel => <<"ticker">>, instId => Symbol}]});
unsubscribe_payload(<<"binance">>, _Symbol) ->
    %% Binance uses the all-market !bookTicker stream in this process, so a
    %% per-symbol unsubscribe is not available for the current connection.
    undefined;
unsubscribe_payload(_, _Symbol) ->
    undefined.

handle_ws_payload(Data, State) ->
    case decode_ws(Data) of
        {ok, Msg} ->
            maybe_reply_ws(Msg, State),
            maybe_write_ticker(Msg, State);
        _ -> ok
    end.

decode_ws(Data) ->
    try {ok, arbiguard_json:decode(Data)}
    catch _:_ -> error
    end.

maybe_reply_ws(Msg, #state{id = <<"htx">>, ws_conn = ConnPid, ws_stream = StreamRef})
        when ConnPid =/= undefined, StreamRef =/= undefined ->
    case map_get_any([ping], Msg, undefined) of
        undefined -> ok;
        Ping -> catch gun:ws_send(ConnPid, StreamRef, {text, arbiguard_json:encode(#{pong => Ping})})
    end;
maybe_reply_ws(_Msg, _State) ->
    ok.

maybe_write_ticker(Msg, State = #state{id = <<"binance">> = ID}) ->
    case map_get_any([s, <<"s">>], Msg, undefined) of
        undefined -> ok;
        Symbol ->
            Bid = arbiguard_util:to_float(map_get_any([b, <<"b">>], Msg, 0), 0),
            Ask = arbiguard_util:to_float(map_get_any([a, <<"a">>], Msg, 0), 0),
            write_ticker(ID, Symbol, Bid, Ask, 0, 0, <<"not_in_bookticker">>, State)
    end;
maybe_write_ticker(Msg, State = #state{id = <<"okx">> = ID}) ->
    Data = map_get_any([data, <<"data">>], Msg, []),
    [write_ticker(ID,
                  undo_okx_symbol(map_get_any([instId, <<"instId">>], Row, <<"">>)),
                  arbiguard_util:to_float(map_get_any([bidPx, <<"bidPx">>], Row, 0), 0),
                  arbiguard_util:to_float(map_get_any([askPx, <<"askPx">>], Row, 0), 0),
                  arbiguard_util:to_float(map_get_any([last, <<"last">>], Row, 0), 0),
                  arbiguard_util:to_float(map_get_any([markPx, mark_price, <<"markPx">>, <<"mark_price">>], Row, 0), 0),
                  <<"ws_ticker">>,
                  State)
     || Row <- Data],
    ok;
maybe_write_ticker(Msg, State = #state{id = <<"gate">> = ID}) ->
    Rows0 = map_get_any([result, <<"result">>], Msg, []),
    Rows = case is_list(Rows0) of true -> Rows0; false -> [Rows0] end,
    [write_ticker(ID,
                  undo_gate_symbol(map_get_any([contract, <<"contract">>], Row, <<"">>)),
                  arbiguard_util:to_float(map_get_any([highest_bid, bid1, <<"highest_bid">>, <<"bid1">>], Row, 0), 0),
                  arbiguard_util:to_float(map_get_any([lowest_ask, ask1, <<"lowest_ask">>, <<"ask1">>], Row, 0), 0),
                  arbiguard_util:to_float(map_get_any([last, <<"last">>], Row, 0), 0),
                  arbiguard_util:to_float(map_get_any([mark_price, markPrice, <<"mark_price">>, <<"markPrice">>], Row, 0), 0),
                  <<"ws_ticker">>,
                  State)
     || Row <- Rows],
    ok;
maybe_write_ticker(Msg, State = #state{id = <<"htx">> = ID}) ->
    Tick = map_get_any([tick, <<"tick">>], Msg, #{}),
    Ch = map_get_any([ch, <<"ch">>], Msg, <<"">>),
    Symbol = htx_symbol_from_channel(Ch),
    Bid = side_price(map_get_any([bid, <<"bid">>], Tick, [])),
    Ask = side_price(map_get_any([ask, <<"ask">>], Tick, [])),
    Last = arbiguard_util:to_float(map_get_any([close, <<"close">>], Tick, 0), 0),
    Mark = arbiguard_util:to_float(map_get_any([mark_price, markPrice, <<"mark_price">>, <<"markPrice">>], Tick, 0), 0),
    case Symbol of <<"">> -> ok; _ -> write_ticker(ID, Symbol, Bid, Ask, Last, Mark, <<"ws_ticker">>, State) end;
maybe_write_ticker(Msg, State = #state{id = <<"weex">> = ID}) ->
    Rows0 = map_get_any([data, <<"data">>], Msg, []),
    Rows = case is_list(Rows0) of true -> Rows0; false -> [Rows0] end,
    [write_ticker(ID,
                  map_get_any([symbol, instId, <<"symbol">>, <<"instId">>], Row, <<"">>),
                  arbiguard_util:to_float(map_get_any([bidPr, bid, bidPx, bestBid, <<"bidPr">>, <<"bid">>, <<"bidPx">>, <<"bestBid">>], Row, 0), 0),
                  arbiguard_util:to_float(map_get_any([askPr, ask, askPx, bestAsk, <<"askPr">>, <<"ask">>, <<"askPx">>, <<"bestAsk">>], Row, 0), 0),
                  arbiguard_util:to_float(map_get_any([lastPr, last, lastPrice, <<"lastPr">>, <<"last">>, <<"lastPrice">>], Row, 0), 0),
                  arbiguard_util:to_float(map_get_any([markPrice, mark_price, markPx, <<"markPrice">>, <<"mark_price">>, <<"markPx">>], Row, 0), 0),
                  <<"ws_ticker">>,
                  State)
     || Row <- Rows],
    ok;
maybe_write_ticker(_Msg, _State) ->
    ok.

write_ticker(ID, Symbol0, Bid, Ask, Last, Mark0, MarkSource, State) ->
    Symbol = norm_symbol(Symbol0),
    Prev = case arbiguard_ets:get_ticker(ID, Symbol) of {ok, PrevRow} -> PrevRow; _ -> #{} end,
    TradeMid = trade_mid(Bid, Ask, Last),
    Mark = case Mark0 > 0 of
        true -> Mark0;
        false -> maps:get(mark_price, Prev, 0)
    end,
    MarkSource1 = case Mark0 > 0 of
        true -> MarkSource;
        false -> maps:get(mark_price_source, Prev, <<"preserved_or_missing">>)
    end,
    Row = Prev#{exchange => ID,
                symbol => Symbol,
                bid => Bid,
                ask => Ask,
                last_price => Last,
                trade_mid_price => TradeMid,
                mark_price => Mark,
                mark_price_source => MarkSource1,
                liquidation_reference_price => Mark,
                updated_at => arbiguard_util:now_ms()},
    ok = arbiguard_ets:put_ticker(Row),
    forward_ticker(Row, State).

trade_mid(Bid, Ask, _Last) when Bid > 0, Ask > 0 ->
    (Bid + Ask) / 2;
trade_mid(_Bid, _Ask, Last) ->
    Last.

forward_ticker(Row, #state{subscriptions = Subs}) ->
    Symbol = maps:get(symbol, Row),
    Reasons = maps:get(Symbol, Subs, []),
    maybe_send_ticker(lists:member(open_execution_order, Reasons), arbiguard_open_executor, Row),
    maybe_send_ticker(lists:member(close_execution_order, Reasons), arbiguard_close_executor, Row),
    ok.

maybe_send_ticker(true, PidName, Row) ->
    PidName ! {ticker_update, Row};
maybe_send_ticker(false, _PidName, _Row) ->
    ok.

okx_symbol(Symbol) ->
    binary:replace(norm_symbol(Symbol), <<"USDT">>, <<"-USDT-SWAP">>).

undo_okx_symbol(Inst) ->
    binary:replace(binary:replace(Inst, <<"-USDT-SWAP">>, <<"USDT">>), <<"-">>, <<>>, [global]).

gate_symbol(Symbol) ->
    binary:replace(norm_symbol(Symbol), <<"USDT">>, <<"_USDT">>).

htx_symbol(Symbol) ->
    binary:replace(norm_symbol(Symbol), <<"USDT">>, <<"-USDT">>).

undo_gate_symbol(Symbol) ->
    binary:replace(norm_symbol(Symbol), <<"_">>, <<>>, [global]).

htx_symbol_from_channel(Ch0) ->
    Ch = arbiguard_util:to_binary(Ch0),
    Parts = binary:split(Ch, <<".">>, [global]),
    case Parts of
        [<<"market">>, Contract, <<"ticker">>] -> binary:replace(Contract, <<"-">>, <<>>, [global]);
        _ -> <<"">>
    end.

side_price([Price | _]) ->
    arbiguard_util:to_float(Price, 0);
side_price(Value) ->
    arbiguard_util:to_float(Value, 0).

add_reason(Reason, Reasons) ->
    lists:usort([Reason | Reasons]).

remove_reason(Reason, Reasons) ->
    [R || R <- Reasons, R =/= Reason].

has_exchange_ws_reason(Reasons) ->
    lists:any(fun exchange_ws_reason/1, Reasons).

exchange_ws_reason(symbol_watcher) -> true;
exchange_ws_reason(_) -> false.

map_get_any([], _Map, Default) ->
    Default;
map_get_any([Key | Rest], Map, Default) ->
    case maps:find(Key, Map) of
        {ok, Value} -> Value;
        error -> map_get_any(Rest, Map, Default)
    end.

maybe_gunzip(Data) ->
    try zlib:gunzip(Data)
    catch _:_ -> Data
    end.

ensure_path(<<"/", _/binary>> = Path) -> Path;
ensure_path(Path) -> <<"/", Path/binary>>.

fmt(Term) ->
    unicode:characters_to_binary(io_lib:format("~p", [Term])).

norm_symbol(V) ->
    string:uppercase(arbiguard_util:to_binary(V)).
