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
    io:format("ArbiGuard admin listening on http://127.0.0.1:~p~n", [Port]),
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
            {ok, #{method => Method, path => strip_query(Path0), headers => Headers, body => Body}};
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
route(#{method := <<"GET">>, path := <<"/api/config">>}) ->
    json_response(200, arbiguard_config:snapshot());
route(#{method := <<"GET">>, path := <<"/api/funding/state">>}) ->
    json_response(200, arbiguard_state:snapshot());
route(#{method := <<"POST">>, path := <<"/api/funding/paper/reset">>, body := Body}) ->
    Payload = safe_decode(Body),
    json_response(200, arbiguard_state:reset_paper(Payload));
route(#{method := <<"POST">>, path := <<"/api/funding/scan">>, body := Body}) ->
    Payload = safe_decode(Body),
    Result = arbiguard_scanner:scan(Payload),
    Paper = arbiguard_state:submit_scan(Payload, Result),
    json_response(200, Result#{paper_account => Paper});
route(#{method := <<"GET">>, path := <<"/">>}) ->
    html_response(200, index_html());
route(_) ->
    json_response(404, #{error => <<"not_found">>}).

safe_decode(<<>>) ->
    #{};
safe_decode(Body) ->
    try arbiguard_json:decode(Body)
    catch _:Reason -> #{decode_error => fmt(Reason)}
    end.

json_response(Code, Term) ->
    Body = arbiguard_json:encode(Term),
    status(Code, <<"application/json; charset=utf-8">>, Body).

html_response(Code, Body) ->
    status(Code, <<"text/html; charset=utf-8">>, Body).

status(Code, ContentType, Body) ->
    Reason = case Code of 200 -> <<"OK">>; 400 -> <<"Bad Request">>; 404 -> <<"Not Found">>; _ -> <<"OK">> end,
    [<<"HTTP/1.1 ">>, integer_to_binary(Code), <<" ">>, Reason, <<"\r\n">>,
     <<"Content-Type: ">>, ContentType, <<"\r\n">>,
     <<"Access-Control-Allow-Origin: *\r\n">>,
     <<"Content-Length: ">>, integer_to_binary(iolist_size(Body)), <<"\r\n\r\n">>,
     Body].

index_html() ->
    <<"<html><head><meta charset=\"utf-8\"><title>ArbiGuard</title></head>"
      "<body><h1>ArbiGuard</h1><p>Use /api/funding/scan and /api/funding/state.</p></body></html>">>.

strip_query(Path) ->
    hd(binary:split(Path, <<"?">>)).

lower_trim(Bin) ->
    string:lowercase(trim(Bin)).

trim(Bin) ->
    list_to_binary(string:trim(binary_to_list(Bin))).

fmt(Term) ->
    unicode:characters_to_binary(io_lib:format("~p", [Term])).
