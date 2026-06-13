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
route(#{method := <<"GET">>, path := <<"/api/funding/state">>}) ->
    json_response(200, arbiguard_state:snapshot());
route(#{method := <<"GET">>, path := <<"/api/live/state">>}) ->
    json_response(200, arbiguard_live_account:snapshot());
route(#{method := <<"POST">>, path := <<"/api/live/enabled">>, body := Body}) ->
    Payload = safe_decode(Body),
    json_response(200, arbiguard_live_account:set_enabled(maps:get(enabled, Payload, false)));
route(#{method := <<"POST">>, path := <<"/api/live/token">>, body := Body}) ->
    Payload = safe_decode(Body),
    Exchange = maps:get(exchange, Payload, <<"">>),
    Token = maps:remove(exchange, Payload),
    json_response(200, #{ok => arbiguard_live_account:set_exchange_token(Exchange, Token)});
route(#{method := <<"POST">>, path := <<"/api/funding/paper/reset">>, body := Body}) ->
    Payload = safe_decode(Body),
    json_response(200, arbiguard_state:reset_paper(Payload));
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

priv_index_html() ->
    case code:priv_dir(arbiguard) of
        {error, _} ->
            admin_html();
        Dir ->
            Path = filename:join([Dir, "www", "index.html"]),
            case file:read_file(Path) of
                {ok, Body} -> Body;
                {error, _} -> admin_html()
            end
    end.

admin_html() ->
    unicode:characters_to_binary([
        "<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>",
        "<title>ArbiGuard</title><style>",
        "body{margin:0;background:#eef2f6;color:#102033;font-family:Arial,sans-serif}header{background:#111b27;color:white;padding:18px 28px}",
        "main{padding:20px 28px}.panel{background:white;border:1px solid #d7e0ea;border-radius:8px;padding:18px;margin-bottom:18px}",
        ".top{display:flex;justify-content:space-between;gap:12px;align-items:center}.actions{display:flex;gap:10px;flex-wrap:wrap}",
        "button{border:0;border-radius:7px;padding:11px 16px;font-weight:700;cursor:pointer;background:#e8eef5;color:#102033}.primary{background:#2563eb;color:white}.danger{background:#fee2e2;color:#991b1b}",
        ".status{margin-top:12px;padding:10px 12px;border-radius:6px;background:#eaf2ff;color:#1d4ed8;border:1px solid #bfdbfe}",
        ".grid{display:grid;grid-template-columns:repeat(6,minmax(150px,1fr));gap:12px}.card{border:1px solid #d7e0ea;border-radius:8px;padding:12px;background:#fbfdff}.label{font-size:13px;color:#63758a}.value{font-size:22px;font-weight:800;margin-top:6px}",
        ".form{display:grid;grid-template-columns:repeat(6,minmax(150px,1fr));gap:12px;margin-top:14px}label{display:block;font-size:13px;color:#52667c;margin-bottom:5px}input,select{width:100%;box-sizing:border-box;border:1px solid #cfd9e5;border-radius:7px;padding:10px 11px;font-size:14px}",
        ".table-wrap{overflow:auto;border-top:1px solid #d7e0ea;margin-top:14px}table{width:100%;border-collapse:collapse;min-width:960px}th,td{padding:10px 11px;border-bottom:1px solid #d7e0ea;text-align:left;white-space:nowrap}th{color:#52667c;font-size:13px;background:#fbfdff}",
        ".pos{color:#059669}.neg{color:#dc2626}.warn{color:#b45309}.muted{color:#6b7280}.tabs{display:flex;gap:8px;margin-top:10px}.tabs button.active{background:#2563eb;color:white}",
        "@media(max-width:1100px){.grid,.form{grid-template-columns:repeat(2,minmax(150px,1fr))}.top{align-items:flex-start;flex-direction:column}}",
        "</style></head><body><header><h1>ArbiGuard</h1></header><main>",
        "<section class='panel'><div class='top'><div><h2>Funding / Price Arbitrage</h2><div class='muted'>ETS scanner, paper account, and separated live account state.</div></div>",
        "<div class='actions'><button class='primary' onclick='scanOnce()'>Scan Once</button><button onclick='refreshAll()'>Refresh</button><button class='danger' onclick='resetPaper()'>Reset Paper</button></div></div>",
        "<div id='status' class='status'>Loaded.</div><div class='form'>",
        "<div><label>Account Mode</label><select id='account_mode'><option value='paper'>paper</option><option value='live'>live</option></select></div>",
        "<div><label>Capital USDT</label><input id='capital_usdt' value='10000'></div><div><label>Margin USDT</label><input id='execution_notional_usdt' value='200'></div>",
        "<div><label>Leverage</label><input id='paper_leverage' value='10'></div><div><label>Max Open Positions</label><input id='max_open_positions' value='5'></div>",
        "<div><label>Max Position Pct</label><input id='max_position_pct' value='0.1'></div><div><label>Min Funding Edge</label><input id='min_funding_rate' value='0.0003'></div>",
        "<div><label>Min Price Gap</label><input id='min_price_gap_rate' value='0.002'></div><div><label>Max Basis</label><input id='max_basis_rate' value='0.02'></div>",
        "<div><label>Min Profit USDT</label><input id='min_execution_profit_usdt' value='5'></div><div><label>Limit</label><input id='limit' value='30'></div>",
        "</div></section>",
        "<section class='panel'><h2>Overview</h2><div class='grid' id='cards'></div></section>",
        "<section class='panel'><h2>Processes / ETS</h2><div class='table-wrap'><table><thead><tr><th>Exchange</th><th>WS URL</th><th>WS Enabled</th><th>WS Connected</th><th>WS Status</th><th>Subs</th><th>Funding Rows</th><th>Last Refresh</th><th>Error</th></tr></thead><tbody id='processRows'></tbody></table></div></section>",
        "<section class='panel'><h2>Opportunities</h2><div class='table-wrap'><table><thead><tr><th>Symbol</th><th>Type</th><th>Method</th><th>Long</th><th>Short</th><th>Notional</th><th>Net Return</th><th>Profit</th><th>Funding Edge</th><th>Price Gap</th><th>Countdown</th></tr></thead><tbody id='opRows'></tbody></table></div></section>",
        "<section class='panel'><h2>Paper Positions</h2><div class='table-wrap'><table><thead><tr><th>Symbol</th><th>Long</th><th>Short</th><th>Notional</th><th>Long Price</th><th>Short Price</th><th>Unrealized</th><th>Expected</th><th>Created</th></tr></thead><tbody id='positionRows'></tbody></table></div></section>",
        "<section class='panel'><h2>Records</h2><div class='tabs'><button id='tabLogs' class='active' onclick='setLogTab(\"trades\")'>Trades</button><button id='tabSkipped' onclick='setLogTab(\"skipped\")'>Skipped</button><button id='tabLive' onclick='setLogTab(\"live\")'>Live Requests</button></div><div class='table-wrap'><table><thead><tr><th>Time</th><th>Action</th><th>Symbol</th><th>Long</th><th>Short</th><th>Notional</th><th>PNL</th><th>Status/Reason</th></tr></thead><tbody id='logRows'></tbody></table></div></section>",
        "<script>",
        "let lastState={},logTab='trades';const $=id=>document.getElementById(id);const n=v=>Number(v||0);const money=v=>n(v).toFixed(2)+'U';const pct=v=>(n(v)*100).toFixed(4)+'%';const cls=v=>n(v)>=0?'pos':'neg';",
        "const display=v=>(v===undefined||v===null||v==='undefined'||v==='')?'-':v;const esc=v=>String(display(v)).replace(/[&<>\"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',\"'\":'&#39;'}[c]));",
        "async function api(path,opt={}){const r=await fetch(path,{headers:{'content-type':'application/json'},...opt});if(!r.ok)throw new Error(await r.text());return await r.json();}",
        "function payload(){let p=['capital_usdt','execution_notional_usdt','paper_leverage','max_open_positions','max_position_pct','min_funding_rate','min_price_gap_rate','max_basis_rate','min_execution_profit_usdt','limit'].reduce((a,k)=>(a[k]=n($(k).value),a),{});p.account_mode=$('account_mode').value;return p}",
        "function setStatus(s){$('status').textContent=s}function card(k,v,c=''){return `<div class='card'><div class='label'>${k}</div><div class='value ${c}'>${v}</div></div>`}",
        "function renderCards(proc,state,live){const ets=proc.ets||{},acc=proc.account||{};$('cards').innerHTML=[card('Paper Equity',money(state.equity??acc.equity),cls(state.equity??acc.equity)),card('Paper Balance',money(state.balance??acc.balance)),card('Positions',(state.positions||[]).length),card('Live Enabled',String(live.enabled)),card('Live Tokens',(live.token_exchanges||[]).length),card('ETS Ticker',ets.tickers||0),card('ETS Funding',ets.funding||0),card('ETS Opps',ets.opportunities||0)].join('')}",
        "function renderProcesses(proc){$('processRows').innerHTML=(proc.exchanges||[]).map(e=>{const t=e.ticker||{},f=e.funding||{};return `<tr><td>${esc(e.exchange)}</td><td>${esc(t.ws_url||'-')}</td><td>${esc(t.ws_enabled)}</td><td class='${t.ws_connected?'pos':'warn'}'>${esc(t.ws_connected)}</td><td>${esc(t.ws_status||'-')}</td><td>${(t.subscriptions||[]).length}</td><td>${esc(f.last_count??'-')}</td><td>${esc(f.last_refresh_ms??'-')}</td><td>${esc(t.ws_error||f.last_error||'-')}</td></tr>`}).join('')||'<tr><td colspan=9>No process status.</td></tr>'}",
        "function renderOps(ops){$('opRows').innerHTML=(ops||[]).map(o=>`<tr><td>${esc(o.symbol)}</td><td>${esc(o.opportunity_type)}</td><td>${esc(o.method||o.arbitrage_method)}</td><td>${esc(o.long_exchange)}</td><td>${esc(o.short_exchange)}</td><td>${money(o.notional_usdt??o.suggested_notional)}</td><td class='${cls(o.expected_net_return)}'>${pct(o.expected_net_return)}</td><td class='${cls(o.estimated_net_profit_usdt??o.estimated_net_profit)}'>${money(o.estimated_net_profit_usdt??o.estimated_net_profit)}</td><td>${pct(o.funding_edge??o.funding_edge_return)}</td><td>${pct(o.price_gap_rate??o.price_gap_return)}</td><td>${esc(o.settlement_countdown||'-')}</td></tr>`).join('')||'<tr><td colspan=11>No opportunities.</td></tr>'}",
        "function renderPositions(state){$('positionRows').innerHTML=(state.positions||[]).map(p=>`<tr><td>${esc(p.symbol)}</td><td>${esc(p.long_exchange)}</td><td>${esc(p.short_exchange)}</td><td>${money(p.notional_usdt||p.notional)}</td><td>${esc(p.long_current_price??p.long_entry_price??'-')}</td><td>${esc(p.short_current_price??p.short_entry_price??'-')}</td><td class='${cls(p.unrealized_pnl)}'>${money(p.unrealized_pnl)}</td><td class='${cls(p.expected_net_profit)}'>${money(p.expected_net_profit)}</td><td>${esc(p.opened_at??'-')}</td></tr>`).join('')||'<tr><td colspan=9>No positions.</td></tr>'}",
        "function setLogTab(tab){logTab=tab;$('tabLogs').className=tab==='trades'?'active':'';$('tabSkipped').className=tab==='skipped'?'active':'';$('tabLive').className=tab==='live'?'active':'';renderLogs(lastState)}",
        "function renderLogs(state){let logs=logTab==='live'?(state.live_logs||[]):(state.logs||[]);if(logTab==='skipped')logs=logs.filter(x=>String(x.action||'').includes('skip'));if(logTab==='trades')logs=logs.filter(x=>!String(x.action||'').includes('skip'));$('logRows').innerHTML=logs.slice(0,200).map(x=>`<tr><td>${esc(x.time||x.ts_ms||'-')}</td><td>${esc(x.action)}</td><td>${esc(x.symbol)}</td><td>${esc(x.long_exchange)}</td><td>${esc(x.short_exchange)}</td><td>${money(x.notional_usdt||x.notional)}</td><td class='${cls(x.net_pnl)}'>${money(x.net_pnl)}</td><td>${esc(x.status||x.reason||'-')}</td></tr>`).join('')||'<tr><td colspan=8>No records.</td></tr>'}",
        "async function refreshAll(){try{setStatus('Refreshing...');const [proc,state,exec,live]=await Promise.all([api('/api/processes'),api('/api/funding/state'),api('/api/executor/state'),api('/api/live/state')]);state.live_logs=live.logs||[];lastState=state;renderCards(proc,state,live);renderProcesses(proc);renderOps(exec.last_opportunities||[]);renderPositions(state);renderLogs(state);setStatus('Refreshed.')}catch(e){setStatus('Refresh failed: '+e.message)}}",
        "async function scanOnce(){try{setStatus('Scanning ETS...');const r=await api('/api/funding/scan',{method:'POST',body:JSON.stringify(payload())});lastState=r.paper_account||{};renderOps(r.opportunities||[]);renderPositions(lastState);renderLogs(lastState);await refreshAll();setStatus(`Scan done: ${(r.opportunities||[]).length} opportunities.`)}catch(e){setStatus('Scan failed: '+e.message)}}",
        "async function resetPaper(){try{setStatus('Resetting paper account...');await api('/api/funding/paper/reset',{method:'POST',body:JSON.stringify({capital_usdt:n($('capital_usdt').value)})});await refreshAll();setStatus('Paper account reset.')}catch(e){setStatus('Reset failed: '+e.message)}}",
        "refreshAll();setInterval(refreshAll,5000);</script></main></body></html>"
    ]).

strip_query(Path) ->
    hd(binary:split(Path, <<"?">>)).

lower_trim(Bin) ->
    string:lowercase(trim(Bin)).

trim(Bin) ->
    list_to_binary(string:trim(binary_to_list(Bin))).

fmt(Term) ->
    unicode:characters_to_binary(io_lib:format("~p", [Term])).
