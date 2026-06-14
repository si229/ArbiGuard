-module(arbiguard_private_ws).
-behaviour(gen_server).

-export([start_link/1, start_link/2, snapshot/1, snapshot/2, name/1, name/2, inject_event/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {account_id = <<"live-main">>, exchange, id, ws_enabled = false, ws_connected = false,
                ws_status = <<"stopped">>, ws_error = undefined,
                ws_conn = undefined, ws_stream = undefined, heartbeat_ref = undefined,
                logged_in = false, subscribed = false, last_event = undefined, last_event_at = 0}).

start_link(Exchange) ->
    ID = maps:get(id, Exchange),
    gen_server:start_link({local, name(ID)}, ?MODULE, [<<"live-main">>, Exchange], []).

start_link(AccountID, Exchange) ->
    ID = maps:get(id, Exchange),
    gen_server:start_link({local, name(AccountID, ID)}, ?MODULE, [AccountID, Exchange], []).

snapshot(ExchangeID) ->
    gen_server:call(name(ExchangeID), snapshot).

snapshot(AccountID, ExchangeID) ->
    gen_server:call(name(AccountID, ExchangeID), snapshot).

inject_event(ExchangeID, Event) ->
    gen_server:cast(name(ExchangeID), {inject_event, Event}).

name(ExchangeID) ->
    list_to_atom("arbiguard_private_ws_" ++ binary_to_list(string:lowercase(arbiguard_util:to_binary(ExchangeID)))).

name(AccountID, ExchangeID) ->
    list_to_atom("arbiguard_private_ws_" ++ safe_atom_part(AccountID) ++ "_" ++ safe_atom_part(ExchangeID)).

init([AccountID, Exchange]) ->
    ID = maps:get(id, Exchange),
    case application:get_env(arbiguard, private_ws_enabled, true) of
        true -> self() ! start_ws;
        false -> ok
    end,
    {ok, #state{account_id = arbiguard_util:to_binary(AccountID), exchange = Exchange, id = ID}}.

handle_call(snapshot, _From, State) ->
    {Host, Port, Path, URL} = ws_endpoint_info(State),
    {reply, #{exchange => State#state.id,
              account_id => State#state.account_id,
              ws_enabled => State#state.ws_enabled,
              ws_connected => State#state.ws_connected,
              ws_status => State#state.ws_status,
              ws_error => State#state.ws_error,
              ws_host => Host,
              ws_port => Port,
              ws_path => Path,
              ws_url => URL,
              logged_in => State#state.logged_in,
              subscribed => State#state.subscribed,
              last_event => State#state.last_event,
              last_event_at => State#state.last_event_at}, State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({inject_event, Event}, State) ->
    {noreply, dispatch_private_event(Event, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(start_ws, State) ->
    {noreply, do_start_ws(State)};
handle_info({gun_ws, _ConnPid, _StreamRef, {text, Data}}, State) ->
    {noreply, handle_ws_payload(Data, State)};
handle_info({gun_ws, _ConnPid, _StreamRef, {binary, Data}}, State) ->
    {noreply, handle_ws_payload(maybe_gunzip(Data), State)};
handle_info(ws_heartbeat, State = #state{ws_connected = true}) ->
    send_ws_heartbeat(State),
    {noreply, schedule_heartbeat(State)};
handle_info(ws_heartbeat, State) ->
    {noreply, State#state{heartbeat_ref = undefined}};
handle_info({gun_down, ConnPid, _Proto, Reason, _KilledStreams}, State = #state{ws_conn = ConnPid}) ->
    lager:warning("private ws down exchange=~s reason=~p", [State#state.id, Reason]),
    erlang:send_after(3000, self(), start_ws),
    {noreply, State#state{ws_connected = false, logged_in = false, subscribed = false,
                          ws_status = <<"reconnecting">>, ws_error = fmt(Reason),
                          ws_conn = undefined, ws_stream = undefined}};
handle_info({gun_error, ConnPid, _StreamRef, Reason}, State = #state{ws_conn = ConnPid}) ->
    lager:warning("private ws error exchange=~s reason=~p", [State#state.id, Reason]),
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
            State#state{ws_enabled = true, ws_connected = false,
                        ws_status = <<"unsupported">>, ws_error = Reason}
    end.

connect_ws(State, Host, Port, Path) ->
    HostName = binary_to_list(Host),
    OpenOpts = #{transport => tls,
                 protocols => [http],
                 tcp_opts => [inet],
                 tls_opts => [{server_name_indication, HostName}, {verify, verify_none}]},
    case gun:open(HostName, Port, OpenOpts) of
        {ok, ConnPid} ->
            case gun:await_up(ConnPid, 10000) of
                {ok, _Protocol} ->
                    StreamRef = gun:ws_upgrade(ConnPid, binary_to_list(Path), ws_headers(State, Host)),
                    await_ws_upgrade(State, ConnPid, StreamRef, Host, Path);
                {error, Reason} ->
                    catch gun:close(ConnPid),
                    reconnect(State, fmt(Reason))
            end;
        {error, Reason} ->
            reconnect(State, fmt(Reason))
    end.

await_ws_upgrade(State, ConnPid, StreamRef, Host, Path) ->
    case gun:await(ConnPid, StreamRef, 10000) of
        {upgrade, [<<"websocket">>], _Headers} ->
            Connected = State#state{ws_enabled = true, ws_connected = true,
                                    ws_status = <<"connected">>, ws_error = undefined,
                                    ws_conn = ConnPid, ws_stream = StreamRef},
            lager:info("private ws connected exchange=~s host=~s path=~s", [State#state.id, Host, Path]),
            login_and_subscribe(schedule_heartbeat(Connected));
        Other ->
            catch gun:close(ConnPid),
            reconnect(State, ws_upgrade_error(Other))
    end.

login_and_subscribe(State) ->
    case arbiguard_exchange_account:get_token(State#state.account_id, State#state.id) of
        undefined ->
            State#state{ws_status = <<"waiting_token">>, ws_error = <<"live_token_not_configured">>};
        Token ->
            State1 = send_login(State, Token),
            send_private_subscriptions(State1)
    end.

send_login(State = #state{ws_conn = ConnPid, ws_stream = StreamRef}, Token) ->
    case login_payload(State#state.id, Token) of
        undefined ->
            State#state{logged_in = false, ws_status = <<"login_not_implemented">>, ws_error = <<"private_ws_login_not_implemented">>};
        Payload ->
            Result = catch gun:ws_send(ConnPid, StreamRef, {text, Payload}),
            lager:info("private ws login sent exchange=~s result=~p", [State#state.id, Result]),
            State#state{logged_in = true, ws_status = <<"login_sent">>, ws_error = undefined}
    end.

send_private_subscriptions(State = #state{ws_conn = ConnPid, ws_stream = StreamRef}) ->
    Payloads = private_subscribe_payloads(State#state.id),
    [begin
         Result = catch gun:ws_send(ConnPid, StreamRef, {text, Payload}),
         lager:info("private ws subscribe sent exchange=~s result=~p payload=~s", [State#state.id, Result, Payload])
     end || Payload <- Payloads],
    State#state{subscribed = Payloads =/= [], ws_status = <<"subscribed">>}.

reconnect(State, Reason) ->
    erlang:send_after(3000, self(), start_ws),
    State#state{ws_enabled = true, ws_connected = false, logged_in = false, subscribed = false,
                ws_status = <<"reconnecting">>, ws_error = Reason,
                ws_conn = undefined, ws_stream = undefined}.

handle_ws_payload(<<"ping">>, State = #state{ws_conn = ConnPid, ws_stream = StreamRef}) ->
    catch gun:ws_send(ConnPid, StreamRef, {text, <<"pong">>}),
    State;
handle_ws_payload(<<"pong">>, State) ->
    State;
handle_ws_payload(Data, State) ->
    case decode_ws(Data) of
        {ok, Msg} ->
            maybe_reply_ws(Msg, State),
            dispatch_private_event(Msg, State);
        error ->
            State
    end.

dispatch_private_event(Msg, State) ->
    Events = normalize_private_events(State#state.id, Msg),
    [dispatch_normalized_event(State#state.account_id, State#state.id, Event) || Event <- Events],
    case Events of
        [] -> State;
        _ -> State#state{last_event = lists:last(Events), last_event_at = arbiguard_util:now_ms()}
    end.

dispatch_normalized_event(AccountID, ExchangeID, #{event_type := order} = Event) ->
    arbiguard_exchange_account:report_order_event(AccountID, ExchangeID, Event);
dispatch_normalized_event(AccountID, ExchangeID, #{event_type := balance} = Event) ->
    arbiguard_exchange_account:report_balance(AccountID, ExchangeID, Event);
dispatch_normalized_event(AccountID, ExchangeID, #{event_type := position} = Event) ->
    arbiguard_exchange_account:report_position(AccountID, ExchangeID, Event);
dispatch_normalized_event(AccountID, ExchangeID, #{event_type := funding} = Event) ->
    arbiguard_exchange_account:report_funding_settlement(AccountID, ExchangeID, Event#{exchange => ExchangeID});
dispatch_normalized_event(AccountID, ExchangeID, #{event_type := liquidation} = Event) ->
    arbiguard_exchange_account:report_liquidation(AccountID, ExchangeID, Event);
dispatch_normalized_event(_AccountID, _ExchangeID, _Event) ->
    ok.

normalize_private_events(ID, Msg) ->
    Rows0 = rows_from_private_msg(Msg),
    Rows = case Rows0 of [] -> [Msg]; _ -> Rows0 end,
    lists:filtermap(fun(Row) -> normalize_private_event(ID, Row, Msg) end, Rows).

normalize_private_event(_ID, Row, Raw) ->
    Type = classify_private_event(Row),
    case Type of
        ignored -> false;
        _ -> {true, (common_event(Type, Row))#{raw => Raw}}
    end.

classify_private_event(Row) ->
    Text = string:lowercase(arbiguard_util:to_binary(
        <<(map_get_any([e, <<"e">>, event, <<"event">>, channel, <<"channel">>, arg, <<"arg">>, topic, <<"topic">>], Row, <<"">>))/binary, " ",
          (map_get_any([type, <<"type">>, status, <<"status">>, m, <<"m">>], Row, <<"">>))/binary>>)),
    OrderLike = has_any_key([order_id, <<"order_id">>, ordId, <<"ordId">>, i, <<"i">>, client_oid, <<"client_oid">>], Row)
        orelse binary:match(Text, <<"order">>) =/= nomatch,
    BalanceLike = has_any_key([available_balance, <<"available_balance">>, wallet_balance, <<"wallet_balance">>, balance, <<"balance">>, equity, <<"equity">>], Row)
        orelse binary:match(Text, <<"balance">>) =/= nomatch,
    PositionLike = has_any_key([position_amt, <<"position_amt">>, pos, <<"pos">>, positions, <<"positions">>, liquidation_price, <<"liquidation_price">>], Row)
        orelse binary:match(Text, <<"position">>) =/= nomatch,
    LiquidationLike = binary:match(Text, <<"liquidation">>) =/= nomatch
        orelse binary:match(Text, <<"adl">>) =/= nomatch
        orelse binary:match(Text, <<"force">>) =/= nomatch,
    FundingLike = binary:match(Text, <<"funding">>) =/= nomatch,
    case true of
        _ when OrderLike -> order;
        _ when BalanceLike -> balance;
        _ when PositionLike -> position;
        _ when LiquidationLike -> liquidation;
        _ when FundingLike -> funding;
        _ -> ignored
    end.

common_event(Type, Row) ->
    Price = to_float_any([price, <<"price">>, fill_price, <<"fill_price">>, avgPx, <<"avgPx">>, p, <<"p">>], Row),
    Qty = to_float_any([qty, <<"qty">>, fill_qty, <<"fill_qty">>, sz, <<"sz">>, q, <<"q">>], Row),
    Notional0 = to_float_any([notional, <<"notional">>, filled_notional, <<"filled_notional">>, accFillSz, <<"accFillSz">>], Row),
    #{event_type => Type,
      order_id => to_binary_any([order_id, <<"order_id">>, ordId, <<"ordId">>, i, <<"i">>, client_oid, <<"client_oid">>], Row),
      client_order_id => to_binary_any([client_order_id, <<"client_order_id">>, clOrdId, <<"clOrdId">>, c, <<"c">>], Row),
      symbol => normalize_symbol(to_binary_any([symbol, <<"symbol">>, instId, <<"instId">>, contract, <<"contract">>, s, <<"s">>], Row)),
      side => string:lowercase(to_binary_any([side, <<"side">>, 'S', <<"S">>, posSide, <<"posSide">>], Row)),
      status => string:lowercase(to_binary_any([status, <<"status">>, state, <<"state">>, 'X', <<"X">>], Row)),
      filled_price => Price,
      filled_qty => Qty,
      filled_notional => case Notional0 > 0 of true -> Notional0; false -> Price * Qty end,
      delta_filled_notional => Price * Qty,
      fee => to_float_any([fee, <<"fee">>, commission, <<"commission">>], Row),
      asset => to_binary_any([asset, <<"asset">>, ccy, <<"ccy">>, marginCoin, <<"marginCoin">>], Row),
      wallet_balance => to_float_any([wallet_balance, <<"wallet_balance">>, walletBalance, <<"walletBalance">>], Row),
      available_balance => to_float_any([available_balance, <<"available_balance">>, available, <<"available">>, availBal, <<"availBal">>], Row),
      margin_balance => to_float_any([margin_balance, <<"margin_balance">>, equity, <<"equity">>], Row),
      unrealized_pnl => to_float_any([unrealized_pnl, <<"unrealized_pnl">>, upl, <<"upl">>, unrealizedProfit, <<"unrealizedProfit">>], Row),
      qty => Qty,
      entry_price => to_float_any([entry_price, <<"entry_price">>, avgPx, <<"avgPx">>, entryPrice, <<"entryPrice">>], Row),
      mark_price => to_float_any([mark_price, <<"mark_price">>, markPx, <<"markPx">>], Row),
      liquidation_price => to_float_any([liquidation_price, <<"liquidation_price">>, liqPx, <<"liqPx">>], Row)}.

rows_from_private_msg(Msg) when is_map(Msg) ->
    lists:append([rows_from_value(maps:get(Key, Msg, [])) || Key <- [data, <<"data">>, result, <<"result">>, event, <<"event">>]]);
rows_from_private_msg(_) ->
    [].

rows_from_value(Rows) when is_list(Rows) -> [Row || Row <- Rows, is_map(Row)];
rows_from_value(Row) when is_map(Row) -> [Row];
rows_from_value(_) -> [].

login_payload(<<"binance">>, _Token) -> undefined;
login_payload(<<"okx">>, Token) ->
    ApiKey = token_get(api_key, Token),
    Passphrase = token_get(passphrase, Token),
    Secret = token_get(api_secret, Token),
    Timestamp = integer_to_binary(erlang:system_time(second)),
    Sign = base64:encode(crypto:mac(hmac, sha256, Secret, <<Timestamp/binary, "GET/users/self/verify">>)),
    Login = #{op => <<"login">>,
              args => [#{apiKey => ApiKey,
                         passphrase => Passphrase,
                         timestamp => Timestamp,
                         sign => Sign}]},
    arbiguard_json:encode(Login);
login_payload(<<"gate">>, _Token) -> undefined;
login_payload(<<"htx">>, _Token) -> undefined;
login_payload(<<"weex">>, _Token) -> undefined;
login_payload(_, _) -> undefined.

private_subscribe_payloads(<<"okx">>) ->
    [arbiguard_json:encode(#{op => <<"subscribe">>, args => [#{channel => <<"orders">>, instType => <<"SWAP">>},
                                                            #{channel => <<"account">>},
                                                            #{channel => <<"positions">>, instType => <<"SWAP">>} ]})];
private_subscribe_payloads(_) ->
    [].

send_ws_heartbeat(#state{id = <<"okx">>, ws_conn = ConnPid, ws_stream = StreamRef}) ->
    catch gun:ws_send(ConnPid, StreamRef, {text, <<"ping">>});
send_ws_heartbeat(#state{id = <<"weex">>, ws_conn = ConnPid, ws_stream = StreamRef}) ->
    catch gun:ws_send(ConnPid, StreamRef, {text, <<"ping">>});
send_ws_heartbeat(_State) ->
    ok.

maybe_reply_ws(Msg, #state{id = <<"htx">>, ws_conn = ConnPid, ws_stream = StreamRef}) ->
    case map_get_any([ping], Msg, undefined) of
        undefined -> ok;
        Ping -> catch gun:ws_send(ConnPid, StreamRef, {text, arbiguard_json:encode(#{pong => Ping})})
    end;
maybe_reply_ws(_Msg, _State) ->
    ok.

schedule_heartbeat(State = #state{heartbeat_ref = Ref}) ->
    case Ref of undefined -> ok; _ -> erlang:cancel_timer(Ref) end,
    State#state{heartbeat_ref = erlang:send_after(15000, self(), ws_heartbeat)}.

ws_endpoint(#state{exchange = Exchange}) ->
    Host = maps:get(private_ws_host, Exchange, <<"">>),
    Port = arbiguard_util:to_int(maps:get(private_ws_port, Exchange, 443), 443),
    Path = ensure_path(maps:get(private_ws_path, Exchange, <<"/">>)),
    case Host =/= <<"">> andalso Port > 0 of
        true -> {ok, Host, Port, Path};
        false -> {error, <<"private_websocket_endpoint_not_configured">>}
    end.

ws_endpoint_info(State) ->
    case ws_endpoint(State) of
        {ok, Host, Port, Path} ->
            PortPart = case Port of 443 -> <<"">>; _ -> unicode:characters_to_binary(io_lib:format(":~p", [Port])) end,
            {Host, Port, Path, <<"wss://", Host/binary, PortPart/binary, Path/binary>>};
        {error, _} -> {<<"">>, 0, <<"">>, <<"">>}
    end.

ws_headers(#state{id = <<"okx">>}, Host) ->
    [{<<"host">>, Host}, {<<"user-agent">>, <<"ArbiGuard/0.1">>}, {<<"origin">>, <<"https://www.okx.com">>}];
ws_headers(_State, Host) ->
    [{<<"host">>, Host}, {<<"user-agent">>, <<"ArbiGuard/0.1">>}].

decode_ws(Data) ->
    try {ok, arbiguard_json:decode(Data)} catch _:_ -> error end.

ws_upgrade_error({response, _Fin, Status, Headers}) ->
    unicode:characters_to_binary(io_lib:format("ws_upgrade_http_~p ~p", [Status, lists:sublist(Headers, 5)]));
ws_upgrade_error(timeout) -> <<"ws_upgrade_timeout">>;
ws_upgrade_error(Other) -> fmt(Other).

maybe_gunzip(Data) ->
    try zlib:gunzip(Data) catch _:_ -> Data end.

ensure_path(<<"/", _/binary>> = Path) -> Path;
ensure_path(Path) -> <<"/", Path/binary>>.

has_any_key(Keys, Msg) when is_map(Msg) ->
    lists:any(fun(Key) -> maps:is_key(Key, Msg) end, Keys);
has_any_key(_Keys, _Msg) -> false.

map_get_any([], _Map, Default) -> Default;
map_get_any(_Keys, Map, Default) when not is_map(Map) -> Default;
map_get_any([Key | Rest], Map, Default) ->
    case maps:find(Key, Map) of {ok, Value} -> Value; error -> map_get_any(Rest, Map, Default) end.

to_binary_any(Keys, Row) ->
    arbiguard_util:to_binary(map_get_any(Keys, Row, <<"">>)).

to_float_any(Keys, Row) ->
    arbiguard_util:to_float(map_get_any(Keys, Row, 0.0), 0.0).

token_get(Key, Token) ->
    arbiguard_util:to_binary(maps:get(Key, Token, maps:get(atom_to_binary(Key), Token, <<"">>))).

normalize_symbol(Inst) ->
    S0 = string:uppercase(arbiguard_util:to_binary(Inst)),
    S1 = binary:replace(S0, <<"-USDT-SWAP">>, <<"USDT">>),
    binary:replace(S1, <<"-">>, <<>>, [global]).

fmt(Term) ->
    unicode:characters_to_binary(io_lib:format("~p", [Term])).

safe_atom_part(V) ->
    S = binary_to_list(string:lowercase(arbiguard_util:to_binary(V))),
    [case ((C >= $a andalso C =< $z) orelse (C >= $0 andalso C =< $9)) of true -> C; false -> $_ end || C <- S].
