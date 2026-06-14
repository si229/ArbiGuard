-module(arbiguard_http).
-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {listen, port}).

start_link(Port) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Port], []).

init([Port]) ->
    {ok, Listen} = gen_tcp:listen(Port, [binary, {packet, raw}, {active, false},
                                        {reuseaddr, true}, {ip, {127,0,0,1}}]),
    self() ! accept,
    lager:info("ArbiGuard admin listening on http://127.0.0.1:~p", [Port]),
    {ok, #state{listen = Listen, port = Port}}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(accept, State = #state{listen = Listen}) ->
    case gen_tcp:accept(Listen) of
        {ok, Sock} ->
            spawn(fun() -> handle_socket(Sock) end),
            self() ! accept,
            {noreply, State};
        {error, closed} ->
            {stop, normal, State};
        {error, _} ->
            self() ! accept,
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{listen = Listen}) ->
    catch gen_tcp:close(Listen),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_socket(Sock) ->
    Response =
        case recv_request(Sock) of
            {ok, Req} -> route(Req);
            {error, Reason} -> json_response(400, #{error => fmt(Reason)})
        end,
    ok = gen_tcp:send(Sock, Response),
    gen_tcp:close(Sock).

recv_request(Sock) ->
    case gen_tcp:recv(Sock, 0, 5000) of
        {ok, Bin} -> parse_request(Sock, Bin);
        Error -> Error
    end.

parse_request(Sock, Bin) ->
    case binary:split(Bin, <<"\r\n\r\n">>) of
        [Head, Body0] ->
            Lines = binary:split(Head, <<"\r\n">>, [global]),
            [RequestLine | HeaderLines] = Lines,
            [Method, Path0 | _] = binary:split(RequestLine, <<" ">>, [global]),
            Headers = parse_headers(HeaderLines, #{}),
            Len = arbiguard_util:to_int(maps:get(<<"content-length">>, Headers, <<"0">>), 0),
            Body = recv_body(Sock, Body0, Len),
            {ok, #{method => Method,
                   path => strip_query(Path0),
                   query_params => parse_query(Path0),
                   headers => Headers,
                   body => Body}};
        _ ->
            {error, bad_request}
    end.

recv_body(_Sock, Body, Len) when byte_size(Body) >= Len ->
    binary:part(Body, 0, Len);
recv_body(Sock, Body, Len) ->
    Need = Len - byte_size(Body),
    case gen_tcp:recv(Sock, Need, 5000) of
        {ok, More} -> <<Body/binary, More/binary>>;
        _ -> Body
    end.

parse_headers([], Acc) ->
    Acc;
parse_headers([Line | Rest], Acc) ->
    case binary:split(Line, <<":">>) of
        [K, V] ->
            parse_headers(Rest, Acc#{lower_trim(K) => trim(V)});
        _ ->
            parse_headers(Rest, Acc)
    end.

route(#{method := <<"GET">>, path := <<"/api/health">>}) ->
    json_response(200, #{ok => true, app => <<"ArbiGuard">>});
route(#{method := <<"GET">>, path := <<"/assets/app.css">>}) ->
    static_response(<<"text/css; charset=utf-8">>, ["www", "assets", "app.css"]);
route(#{method := <<"GET">>, path := <<"/assets/app.js">>}) ->
    static_response(<<"application/javascript; charset=utf-8">>, ["www", "assets", "app.js"]);
route(#{method := <<"GET">>, path := <<"/api/config">>}) ->
    json_response(200, arbiguard_config:snapshot());
route(#{method := <<"POST">>, path := <<"/api/config/exchange/ws">>, body := Body}) ->
    Payload = safe_decode(Body),
    Result = arbiguard_runtime_config:set_exchange_ws_endpoint(
               maps:get(exchange, Payload, <<"">>),
               maps:get(ws_host, Payload, <<"">>),
               maps:get(ws_port, Payload, 443),
               maps:get(ws_path, Payload, <<"/">>)),
    json_response(200, Result);
route(#{method := <<"POST">>, path := <<"/api/config/exchange/limits">>, body := Body}) ->
    Payload = safe_decode(Body),
    Result = arbiguard_runtime_config:set_exchange_limits(
               maps:get(exchange, Payload, <<"">>),
               maps:get(max_single_order_usdt, Payload, 0),
               maps:get(max_total_position_usdt, Payload, 0)),
    json_response(200, Result);
route(#{method := <<"GET">>, path := <<"/api/processes">>}) ->
    json_response(200, arbiguard_processes:snapshot());
route(#{method := <<"GET">>, path := <<"/api/executor/state">>}) ->
    json_response(200, arbiguard_executor:snapshot());
route(#{method := <<"GET">>, path := <<"/api/accounts">>}) ->
    json_response(200, arbiguard_account_manager:snapshot());
route(#{method := <<"POST">>, path := <<"/api/accounts">>, body := Body}) ->
    json_response(200, arbiguard_account_manager:create_account(safe_decode(Body)));
route(#{method := <<"GET">>, path := <<"/api/funding/state">>}) ->
    json_response(200, arbiguard_state:snapshot());
route(#{method := <<"GET">>, path := <<"/api/trades/history">>, query_params := Query}) ->
    json_response(200, arbiguard_trade_store:page(Query));
route(#{method := <<"POST">>, path := <<"/api/trades/history">>, body := Body}) ->
    json_response(200, arbiguard_trade_store:page(safe_decode(Body)));
route(#{method := <<"GET">>, path := <<"/api/trades/stats">>, query_params := Query}) ->
    json_response(200, arbiguard_trade_store:stats(Query));
route(#{method := <<"POST">>, path := <<"/api/trades/stats">>, body := Body}) ->
    json_response(200, arbiguard_trade_store:stats(safe_decode(Body)));
route(#{method := <<"GET">>, path := <<"/api/live/state">>}) ->
    json_response(200, live_state());
route(#{method := <<"POST">>, path := <<"/api/debug/exchange/order">>, body := Body}) ->
    json_response(200, #{ok => false, reason => <<"debug_order_removed_use_live_test_order">>, payload => safe_decode(Body)});
route(#{method := <<"POST">>, path := <<"/api/live/test-order">>, body := Body}) ->
    json_response(200, arbiguard_account_manager:test_order(safe_decode(Body)));
route(#{method := <<"POST">>, path := <<"/api/live/enabled">>, body := Body}) ->
    Payload = safe_decode(Body),
    AccountID = maps:get(account_id, Payload, <<"live-main">>),
    json_response(200, arbiguard_account_manager:set_live_enabled(AccountID, maps:get(enabled, Payload, false)));
route(#{method := <<"POST">>, path := <<"/api/live/token">>, body := Body}) ->
    Payload = safe_decode(Body),
    Exchange = maps:get(exchange, Payload, <<"">>),
    AccountID = maps:get(account_id, Payload, <<"live-main">>),
    Token = maps:remove(exchange, Payload),
    json_response(200, arbiguard_account_manager:set_exchange_token(AccountID, Exchange, Token));
route(#{method := <<"POST">>, path := <<"/api/funding/paper/reset">>, body := Body}) ->
    Payload = safe_decode(Body),
    ExecutorReset = arbiguard_executor:reset(),
    Paper = arbiguard_state:reset_paper(Payload),
    json_response(200, Paper#{executor_reset => ExecutorReset});
route(#{method := <<"POST">>, path := <<"/api/funding/apply-settings">>, body := Body}) ->
    Payload = safe_decode(Body),
    json_response(200, arbiguard_scanner:apply_settings(Payload));
route(#{method := <<"POST">>, path := <<"/api/funding/scan">>, body := Body}) ->
    Payload = safe_decode(Body),
    Result = arbiguard_scanner:scan_once(Payload),
    Paper = arbiguard_state:snapshot(),
    json_response(200, Result#{paper_account => Paper});
route(#{method := <<"GET">>, path := <<"/">>}) ->
    html_response(200, priv_index_html());
route(_) ->
    json_response(404, #{error => <<"not_found">>}).

safe_decode(<<>>) ->
    #{};
safe_decode(Body) ->
    try arbiguard_json:decode(Body)
    catch _:Reason -> #{decode_error => fmt(Reason)}
    end.

live_state() ->
    Accounts = arbiguard_account_manager:snapshot(),
    List = maps:get(accounts, Accounts, []),
    Live = case [A || A <- List, maps:get(id, A, <<"">>) =:= <<"live-main">>] of
        [Hit | _] -> Hit;
        [] -> #{}
    end,
    ExchangeAccounts = maps:get(exchange_accounts, Live, #{}),
    TokenConfigured = maps:get(token_configured_exchanges, Live, []),
    ExchangeStates = live_exchange_states(maps:get(id, Live, <<"live-main">>), maps:keys(ExchangeAccounts)),
    #{enabled => maps:get(live_enabled, Live, false),
      account_id => maps:get(id, Live, <<"live-main">>),
      exchange_accounts => maps:keys(ExchangeAccounts),
      token_configured_exchanges => TokenConfigured,
      exchange_states => ExchangeStates,
      accounts => List,
      orders => lists:append([maps:get(orders, S, []) || S <- ExchangeStates]),
      balances => live_balances_by_exchange(ExchangeStates),
      positions => lists:append([maps:get(positions, S, []) || S <- ExchangeStates]),
      liquidations => lists:append([maps:get(liquidations, S, []) || S <- ExchangeStates]),
      logs => lists:append([maps:get(logs, S, []) || S <- ExchangeStates])}.

live_exchange_states(AccountID, Exchanges) ->
    [live_exchange_state(AccountID, ExchangeID) || ExchangeID <- Exchanges].

live_exchange_state(AccountID, ExchangeID) ->
    case catch arbiguard_exchange_account:snapshot(AccountID, ExchangeID) of
        Snapshot when is_map(Snapshot) -> Snapshot;
        {'EXIT', Reason} -> #{account_id => AccountID, exchange => ExchangeID,
                              error => fmt(Reason), balances => #{},
                              positions => [], orders => [], liquidations => [], logs => []};
        Other -> #{account_id => AccountID, exchange => ExchangeID,
                   error => fmt(Other), balances => #{},
                   positions => [], orders => [], liquidations => [], logs => []}
    end.

live_balances_by_exchange(ExchangeStates) ->
    maps:from_list([{maps:get(exchange, S, <<"">>), maps:get(balances, S, #{})} || S <- ExchangeStates]).

json_response(Code, Term) ->
    Body = arbiguard_json:encode(Term),
    status(Code, <<"application/json; charset=utf-8">>, Body).

html_response(Code, Body) ->
    status(Code, <<"text/html; charset=utf-8">>, Body).

static_response(ContentType, Parts) ->
    case read_priv_file(Parts) of
        {ok, Body} -> status(200, ContentType, Body);
        {error, _} -> json_response(404, #{error => <<"static_not_found">>})
    end.

status(Code, ContentType, Body) ->
    Reason = case Code of 200 -> <<"OK">>; 400 -> <<"Bad Request">>; 404 -> <<"Not Found">>; _ -> <<"OK">> end,
    [<<"HTTP/1.1 ">>, integer_to_binary(Code), <<" ">>, Reason, <<"\r\n">>,
     <<"Content-Type: ">>, ContentType, <<"\r\n">>,
     <<"Access-Control-Allow-Origin: *\r\n">>,
     <<"Content-Length: ">>, integer_to_binary(iolist_size(Body)), <<"\r\n\r\n">>,
     Body].

priv_index_html() ->
    case read_priv_file(["www", "index.html"]) of
        {ok, Body} -> Body;
        {error, _} -> admin_html()
    end.

read_priv_file(Parts) ->
    case code:priv_dir(arbiguard) of
        {error, Reason} ->
            {error, Reason};
        Dir ->
            file:read_file(filename:join([Dir | Parts]))
    end.

admin_html() ->
    case read_priv_file(["www", "fallback.html"]) of
        {ok, Body} -> Body;
        {error, _} -> minimal_admin_html()
    end.

minimal_admin_html() ->
    unicode:characters_to_binary(
        "<!doctype html><html><head><meta charset='utf-8'><title>ArbiGuard</title></head>"
        "<body><h1>ArbiGuard</h1><p>Web assets are missing. Check priv/www/index.html.</p></body></html>").

strip_query(Path) ->
    hd(binary:split(Path, <<"?">>)).

parse_query(Path) ->
    case binary:split(Path, <<"?">>) of
        [_OnlyPath] -> #{};
        [_Path, Query] -> parse_query_pairs(binary:split(Query, <<"&">>, [global]), #{})
    end.

parse_query_pairs([], Acc) ->
    Acc;
parse_query_pairs([Pair | Rest], Acc) ->
    case binary:split(Pair, <<"=">>) of
        [K, V] -> parse_query_pairs(Rest, Acc#{url_decode(K) => url_decode(V)});
        [K] when K =/= <<"">> -> parse_query_pairs(Rest, Acc#{url_decode(K) => <<"">>});
        _ -> parse_query_pairs(Rest, Acc)
    end.

url_decode(Bin) ->
    url_decode(Bin, <<>>).

url_decode(<<>>, Acc) ->
    Acc;
url_decode(<<$+, Rest/binary>>, Acc) ->
    url_decode(Rest, <<Acc/binary, " ">>);
url_decode(<<$%, A, B, Rest/binary>>, Acc) ->
    case catch binary_to_integer(<<A, B>>, 16) of
        N when is_integer(N) -> url_decode(Rest, <<Acc/binary, N>>);
        _ -> url_decode(Rest, <<Acc/binary, $%, A, B>>)
    end;
url_decode(<<C, Rest/binary>>, Acc) ->
    url_decode(Rest, <<Acc/binary, C>>).

lower_trim(Bin) ->
    string:lowercase(trim(Bin)).

trim(Bin) ->
    list_to_binary(string:trim(binary_to_list(Bin))).

fmt(Term) ->
    unicode:characters_to_binary(io_lib:format("~p", [Term])).
