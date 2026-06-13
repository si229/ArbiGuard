-module(arbiguard_exchange_ticker).
-behaviour(gen_server).

-export([start_link/1, start_ws/1, set_ws_endpoint/4, subscribe/3, subscribe/4,
         unsubscribe/3, unsubscribe/4, upsert_ticker/2, snapshot/1, name/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {exchange, id, subscriptions = #{}, ws_enabled = false,
                ws_connected = false, ws_status = <<"stopped">>, ws_error = undefined,
                ws_conn = undefined, ws_stream = undefined, heartbeat_ref = undefined}).

start_link(Exchange) ->
    ID = maps:get(id, Exchange),
    gen_server:start_link({local, name(ID)}, ?MODULE, [Exchange], []).

start_ws(ExchangeID) ->
    gen_server:call(name(ExchangeID), start_ws).

set_ws_endpoint(ExchangeID, Host, Port, Path) ->
    gen_server:call(name(ExchangeID), {set_ws_endpoint, Host, Port, Path}).

subscribe(ExchangeID, Symbol, Reason) ->
    subscribe(ExchangeID, Symbol, Reason, self()).

subscribe(ExchangeID, Symbol, Reason, SubscriberPid) ->
    gen_server:call(name(ExchangeID), {subscribe, Symbol, Reason, SubscriberPid}).

unsubscribe(ExchangeID, Symbol, Reason) ->
    unsubscribe(ExchangeID, Symbol, Reason, self()).

unsubscribe(ExchangeID, Symbol, Reason, SubscriberPid) ->
    gen_server:call(name(ExchangeID), {unsubscribe, Symbol, Reason, SubscriberPid}).

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
handle_call({subscribe, Symbol0, Reason, SubscriberPid}, _From, State = #state{subscriptions = Subs}) ->
    Symbol = norm_symbol(Symbol0),
    OldReasonMap = normalize_subscription(maps:get(Symbol, Subs, #{})),
    OldReasons = maps:keys(OldReasonMap),
    AlreadyWsSubscribed = has_exchange_ws_reason(OldReasons),
    NewReasonMap = add_subscriber(Reason, SubscriberPid, OldReasonMap),
    NewReasons = maps:keys(NewReasonMap),
    ShouldWsSubscribe = has_exchange_ws_reason(NewReasons) andalso not AlreadyWsSubscribed,
    lager:info("ticker local subscribe exchange=~s symbol=~s reason=~p subscriber=~p ws_subscribe=~p reasons=~p",
               [State#state.id, Symbol, Reason, SubscriberPid, ShouldWsSubscribe, NewReasons]),
    State1 = State#state{subscriptions = Subs#{Symbol => NewReasonMap}},
    NewState = case ShouldWsSubscribe of
        true -> maybe_ws_subscribe(State1, Symbol);
        false -> State1
    end,
    {reply, ok, NewState};
handle_call({unsubscribe, Symbol0, Reason, SubscriberPid}, _From, State = #state{subscriptions = Subs}) ->
    Symbol = norm_symbol(Symbol0),
    OldReasonMap = normalize_subscription(maps:get(Symbol, Subs, #{})),
    OldReasons = maps:keys(OldReasonMap),
    NewReasonMap = remove_subscriber(Reason, SubscriberPid, OldReasonMap),
    NewReasons = maps:keys(NewReasonMap),
    ShouldWsUnsubscribe = has_exchange_ws_reason(OldReasons) andalso not has_exchange_ws_reason(NewReasons),
    lager:info("ticker local unsubscribe exchange=~s symbol=~s reason=~p subscriber=~p ws_unsubscribe=~p remaining=~p",
               [State#state.id, Symbol, Reason, SubscriberPid, ShouldWsUnsubscribe, NewReasons]),
    State1 = case ShouldWsUnsubscribe of
        true -> maybe_ws_unsubscribe(State, Symbol);
        false -> State
    end,
    State2 = case NewReasons of
        [] -> State1#state{subscriptions = maps:remove(Symbol, Subs)};
        _ -> State1#state{subscriptions = Subs#{Symbol => NewReasonMap}}
    end,
    {reply, ok, State2};
handle_call(snapshot, _From, State = #state{subscriptions = Subs}) ->
    {WsHost, WsPort, WsPath, WsURL} = ws_endpoint_info(State),
    {reply, #{exchange => State#state.id,
              ws_enabled => State#state.ws_enabled,
              ws_connected => State#state.ws_connected,
              ws_status => State#state.ws_status,
              ws_error => State#state.ws_error,
              ws_host => WsHost,
              ws_port => WsPort,
              ws_path => WsPath,
              ws_url => WsURL,
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
handle_info(ws_heartbeat, State = #state{ws_connected = true}) ->
    send_ws_heartbeat(State),
    {noreply, schedule_heartbeat(State)};
handle_info(ws_heartbeat, State) ->
    {noreply, State#state{heartbeat_ref = undefined}};
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
    HostName = binary_to_list(Host),
    OpenOpts = #{transport => tls,
                 protocols => [http],
                 tcp_opts => [inet],
                 tls_opts => [{server_name_indication, HostName},
                              {verify, verify_none}]},
    case gun:open(HostName, Port, OpenOpts) of
        {ok, ConnPid} ->
            case gun:await_up(ConnPid, 10000) of
                {ok, _Protocol} ->
                    StreamRef = gun:ws_upgrade(ConnPid, binary_to_list(Path), ws_headers(State, Host)),
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
            replay_subscriptions(schedule_heartbeat(Connected));
        Other ->
            lager:warning("ticker ws upgrade failed exchange=~s result=~p", [State#state.id, Other]),
            catch gun:close(ConnPid),
            reconnect(State, ws_upgrade_error(Other))
    end.

ws_upgrade_error({response, _Fin, Status, Headers}) ->
    unicode:characters_to_binary(io_lib:format("ws_upgrade_http_~p ~p", [Status, compact_headers(Headers)]));
ws_upgrade_error(timeout) ->
    <<"ws_upgrade_timeout">>;
ws_upgrade_error(Other) ->
    fmt(Other).

compact_headers(Headers) when is_list(Headers) ->
    lists:sublist(Headers, 5);
compact_headers(Headers) ->
    Headers.

reconnect(State, Reason) ->
    erlang:send_after(3000, self(), start_ws),
    State#state{ws_enabled = true, ws_connected = false, ws_status = <<"reconnecting">>, ws_error = Reason,
                ws_conn = undefined, ws_stream = undefined}.

ws_headers(#state{id = <<"okx">>}, Host) ->
    [{<<"host">>, Host},
     {<<"user-agent">>, <<"ArbiGuard/0.1">>},
     {<<"origin">>, <<"https://www.okx.com">>}];
ws_headers(#state{id = <<"binance">>}, Host) ->
    [{<<"host">>, Host},
     {<<"user-agent">>, <<"ArbiGuard/0.1">>},
     {<<"origin">>, <<"https://www.binance.com">>}];
ws_headers(#state{id = <<"gate">>}, Host) ->
    [{<<"host">>, Host},
     {<<"user-agent">>, <<"ArbiGuard/0.1">>},
     {<<"origin">>, <<"https://www.gate.io">>}];
ws_headers(#state{id = <<"htx">>}, Host) ->
    [{<<"host">>, Host},
     {<<"user-agent">>, <<"ArbiGuard/0.1">>},
     {<<"origin">>, <<"https://www.htx.com">>}];
ws_headers(#state{id = <<"weex">>}, Host) ->
    [{<<"host">>, Host},
     {<<"user-agent">>, <<"ArbiGuard/0.1">>},
     {<<"origin">>, <<"https://www.weex.com">>}];
ws_headers(_State, Host) ->
    [{<<"host">>, Host},
     {<<"user-agent">>, <<"ArbiGuard/0.1">>}].

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
            case has_exchange_ws_reason(maps:keys(normalize_subscription(maps:get(Symbol, Subs, #{})))) of
                true -> maybe_ws_subscribe(Acc, Symbol);
                false -> Acc
            end
    end, State, maps:keys(Subs)).

maybe_ws_subscribe(State = #state{ws_connected = true, ws_conn = ConnPid, ws_stream = StreamRef}, Symbol) ->
    case subscribe_payload(State#state.id, Symbol) of
        undefined -> State;
        Payload ->
            SendResult = catch gun:ws_send(ConnPid, StreamRef, {text, Payload}),
            lager:info("ticker ws subscribe sent exchange=~s symbol=~s result=~p payload=~s",
                       [State#state.id, Symbol, SendResult, Payload]),
            State
    end;
maybe_ws_subscribe(State, _Symbol) ->
    State.

maybe_ws_unsubscribe(State = #state{ws_connected = true, ws_conn = ConnPid, ws_stream = StreamRef}, Symbol) ->
    case unsubscribe_payload(State#state.id, Symbol) of
        undefined -> State;
        Payload ->
            SendResult = catch gun:ws_send(ConnPid, StreamRef, {text, Payload}),
            lager:info("ticker ws unsubscribe sent exchange=~s symbol=~s result=~p payload=~s",
                       [State#state.id, Symbol, SendResult, Payload]),
            State
    end;
maybe_ws_unsubscribe(State, _Symbol) ->
    State.

schedule_heartbeat(State = #state{heartbeat_ref = Ref}) ->
    case Ref of
        undefined -> ok;
        _ -> erlang:cancel_timer(Ref)
    end,
    State#state{heartbeat_ref = erlang:send_after(15000, self(), ws_heartbeat)}.

send_ws_heartbeat(#state{id = <<"okx">>, ws_conn = ConnPid, ws_stream = StreamRef}) ->
    catch gun:ws_send(ConnPid, StreamRef, {text, <<"ping">>});
send_ws_heartbeat(#state{id = <<"gate">>, ws_conn = ConnPid, ws_stream = StreamRef}) ->
    Payload = arbiguard_json:encode(#{time => erlang:system_time(second), channel => <<"futures.ping">>}),
    catch gun:ws_send(ConnPid, StreamRef, {text, Payload});
send_ws_heartbeat(_State) ->
    ok.

ws_endpoint(#state{exchange = Exchange}) ->
    Host = maps:get(ws_host, Exchange, <<"">>),
    Port = arbiguard_util:to_int(maps:get(ws_port, Exchange, 443), 443),
    Path = ensure_path(maps:get(ws_path, Exchange, <<"/">>)),
    case Host =/= <<"">> andalso Port > 0 of
        true -> {ok, Host, Port, Path};
        false -> {error, <<"websocket_endpoint_not_configured">>}
    end.

ws_endpoint_info(State) ->
    case ws_endpoint(State) of
        {ok, Host, Port, Path} ->
            PortPart = case Port of
                443 -> <<"">>;
                _ -> unicode:characters_to_binary(io_lib:format(":~p", [Port]))
            end,
            {Host, Port, Path, <<"wss://", Host/binary, PortPart/binary, Path/binary>>};
        {error, _Reason} ->
            {<<"">>, 0, <<"">>, <<"">>}
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
            maybe_log_ws_control(Msg, State),
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

maybe_log_ws_control(Msg, #state{id = ID}) ->
    case is_ws_control(Msg) of
        true -> lager:info("ticker ws control exchange=~s msg=~p", [ID, Msg]);
        false -> ok
    end.

is_ws_control(Msg) when is_map(Msg) ->
    has_any_key([event, <<"event">>, status, <<"status">>, subbed, <<"subbed">>, unsubbed, <<"unsubbed">>,
                 error, <<"error">>, code, <<"code">>, op, <<"op">>], Msg)
        andalso not has_any_key([data, <<"data">>, tick, <<"tick">>, result, <<"result">>, s, <<"s">>], Msg);
is_ws_control(_) ->
    false.

has_any_key(Keys, Msg) ->
    lists:any(fun(Key) -> maps:is_key(Key, Msg) end, Keys).

maybe_write_ticker(Msg, State = #state{id = <<"binance">> = ID}) ->
    case map_get_any([s, <<"s">>], Msg, undefined) of
        undefined -> ok;
        Symbol ->
            Bid = arbiguard_util:to_float(map_get_any([b, <<"b">>], Msg, 0), 0),
            Ask = arbiguard_util:to_float(map_get_any([a, <<"a">>], Msg, 0), 0),
            write_ticker(ID, Symbol, Bid, Ask, 0, 0, <<"not_in_bookticker">>, State)
    end;
maybe_write_ticker(Msg, State = #state{id = <<"okx">> = ID}) ->
    Data = rows_from_value(map_get_any([data, <<"data">>], Msg, [])),
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
    Rows = rows_from_value(map_get_any([result, <<"result">>], Msg, [])),
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
    Rows = rows_from_value(map_get_any([data, <<"data">>], Msg, [])),
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
    ReasonMap = normalize_subscription(maps:get(Symbol, Subs, #{})),
    maybe_send_ticker(maps:get(open_execution_order, ReasonMap, []), Row),
    maybe_send_ticker(maps:get(close_execution_order, ReasonMap, []), Row),
    ok.

maybe_send_ticker(Subscribers, Row) ->
    [Pid ! {ticker_update, Row} || Pid <- Subscribers, is_pid(Pid)],
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

normalize_subscription(Subscription) when is_map(Subscription) ->
    maps:map(fun(_Reason, Pids) -> normalize_pids(Pids) end, Subscription);
normalize_subscription(Reasons) when is_list(Reasons) ->
    %% Compatibility with older state shape: Symbol => [Reason].
    maps:from_list([{Reason, []} || Reason <- Reasons]);
normalize_subscription(_) ->
    #{}.

normalize_pids(Pids) when is_list(Pids) ->
    lists:usort([Pid || Pid <- Pids, is_pid(Pid)]);
normalize_pids(Pid) when is_pid(Pid) ->
    [Pid];
normalize_pids(_) ->
    [].

add_subscriber(Reason, SubscriberPid, ReasonMap) ->
    OldPids = maps:get(Reason, ReasonMap, []),
    ReasonMap#{Reason => lists:usort([SubscriberPid | OldPids])}.

remove_subscriber(Reason, SubscriberPid, ReasonMap) ->
    OldPids = maps:get(Reason, ReasonMap, []),
    NewPids = [Pid || Pid <- OldPids, Pid =/= SubscriberPid],
    case NewPids of
        [] -> maps:remove(Reason, ReasonMap);
        _ -> ReasonMap#{Reason => NewPids}
    end.

has_exchange_ws_reason(Reasons) ->
    lists:any(fun exchange_ws_reason/1, Reasons).

exchange_ws_reason(symbol_watcher) -> true;
exchange_ws_reason(open_execution_order) -> true;
exchange_ws_reason(close_execution_order) -> true;
exchange_ws_reason(_) -> false.

map_get_any([], _Map, Default) ->
    Default;
map_get_any(_Keys, Map, Default) when not is_map(Map) ->
    Default;
map_get_any([Key | Rest], Map, Default) ->
    case maps:find(Key, Map) of
        {ok, Value} -> Value;
        error -> map_get_any(Rest, Map, Default)
    end.

rows_from_value(Rows) when is_list(Rows) ->
    [Row || Row <- Rows, is_map(Row)];
rows_from_value(Row) when is_map(Row) ->
    [Row];
rows_from_value(_) ->
    [].

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
