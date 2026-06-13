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
    lager:log(info, self(), "ArbiGuard admin listening on http://127.0.0.1:~p", [Port]),
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
route(#{method := <<"POST">>, path := <<"/api/funding/scan">>, body := Body}) ->
    Payload = safe_decode(Body),
    Result = arbiguard_scanner:scan_once(Payload),
    Paper = arbiguard_state:snapshot(),
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
    unicode:characters_to_binary([
        "<!doctype html><html lang='zh-CN'><head><meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width,initial-scale=1'>",
        "<title>ArbiGuard</title>",
        "<style>",
        ":root{font-family:Arial,'Microsoft YaHei',sans-serif;color:#102033;background:#eef2f6}",
        "body{margin:0}header{background:#111b27;color:#fff;padding:18px 28px}h1{margin:0;font-size:28px}",
        "main{padding:20px 28px}.panel{background:#fff;border:1px solid #d7e0ea;border-radius:8px;padding:18px;margin-bottom:18px}",
        ".top{display:flex;justify-content:space-between;gap:12px;align-items:center}.actions{display:flex;gap:10px;flex-wrap:wrap}",
        "button{border:0;border-radius:7px;padding:11px 16px;font-weight:700;cursor:pointer;background:#e8eef5;color:#102033}",
        "button.primary{background:#2563eb;color:#fff}button.danger{background:#fee2e2;color:#991b1b}",
        ".status{margin-top:12px;padding:10px 12px;border-radius:6px;background:#eaf2ff;color:#1d4ed8;border:1px solid #bfdbfe}",
        ".grid{display:grid;grid-template-columns:repeat(6,minmax(150px,1fr));gap:12px}.card{border:1px solid #d7e0ea;border-radius:8px;padding:12px;background:#fbfdff}",
        ".label{font-size:13px;color:#63758a}.value{font-size:22px;font-weight:800;margin-top:6px}",
        ".form{display:grid;grid-template-columns:repeat(6,minmax(150px,1fr));gap:12px;margin-top:14px}",
        "label{display:block;font-size:13px;color:#52667c;margin-bottom:5px}input{width:100%;box-sizing:border-box;border:1px solid #cfd9e5;border-radius:7px;padding:10px 11px;font-size:14px}",
        ".table-wrap{overflow:auto;border-top:1px solid #d7e0ea;margin-top:14px}table{width:100%;border-collapse:collapse;min-width:960px}",
        "th,td{padding:10px 11px;border-bottom:1px solid #d7e0ea;text-align:left;white-space:nowrap}th{color:#52667c;font-size:13px;background:#fbfdff}",
        ".pos{color:#059669}.neg{color:#dc2626}.muted{color:#6b7280}.tabs{display:flex;gap:8px;margin-top:10px}",
        ".tabs button.active{background:#2563eb;color:#fff}@media(max-width:1100px){.grid,.form{grid-template-columns:repeat(2,minmax(150px,1fr))}.top{align-items:flex-start;flex-direction:column}}",
        "</style></head><body><header><h1>ArbiGuard</h1></header><main>",
        "<section class='panel'><div class='top'><div><h2>资金费 / 价差套利</h2><div class='muted'>读取 ETS 中的 ticker 和 funding，扫描后进入模拟执行流程。</div></div>",
        "<div class='actions'><button class='primary' onclick='scanOnce()'>扫描一次</button><button onclick='refreshAll()'>刷新状态</button><button class='danger' onclick='resetPaper()'>重置模拟盘</button></div></div>",
        "<div id='status' class='status'>页面已加载，等待刷新。</div>",
        "<div class='form'>",
        "<div><label>模拟本金(U)</label><input id='capital_usdt' value='10000'></div>",
        "<div><label>单币名义仓位(U)</label><input id='execution_notional_usdt' value='200'></div>",
        "<div><label>单币仓位比例</label><input id='max_position_pct' value='0.1'></div>",
        "<div><label>最低资金费差</label><input id='min_funding_rate' value='0.0003'></div>",
        "<div><label>最低价格差</label><input id='min_price_gap_rate' value='0.002'></div>",
        "<div><label>最大价格偏离</label><input id='max_basis_rate' value='0.02'></div>",
        "<div><label>最低执行盈利(U)</label><input id='min_execution_profit_usdt' value='5'></div>",
        "<div><label>展示数量</label><input id='limit' value='30'></div>",
        "</div></section>",
        "<section class='panel'><h2>账户概览</h2><div class='grid' id='cards'></div></section>",
        "<section class='panel'><h2>进程 / ETS 状态</h2><div class='table-wrap'><table><thead><tr><th>交易所</th><th>Ticker WS</th><th>订阅数</th><th>Funding条数</th><th>最后刷新</th><th>错误</th></tr></thead><tbody id='processRows'></tbody></table></div></section>",
        "<section class='panel'><h2>扫描机会</h2><div class='table-wrap'><table><thead><tr><th>币种</th><th>类型</th><th>套利方式</th><th>做多</th><th>做空</th><th>名义仓位</th><th>净回报率</th><th>预期盈利</th><th>资金费差</th><th>价格差</th><th>结算倒计时</th></tr></thead><tbody id='opRows'></tbody></table></div></section>",
        "<section class='panel'><h2>模拟持仓</h2><div class='table-wrap'><table><thead><tr><th>币种</th><th>做多</th><th>做空</th><th>名义仓位</th><th>多头价</th><th>空头价</th><th>当前盈亏</th><th>预期盈利</th><th>创建时间</th></tr></thead><tbody id='positionRows'></tbody></table></div></section>",
        "<section class='panel'><h2>记录</h2><div class='tabs'><button id='tabLogs' class='active' onclick='setLogTab(\"trades\")'>成交/持仓记录</button><button id='tabSkipped' onclick='setLogTab(\"skipped\")'>未执行记录</button></div><div class='table-wrap'><table><thead><tr><th>时间</th><th>动作</th><th>币种</th><th>做多</th><th>做空</th><th>名义仓位</th><th>净盈亏</th><th>原因</th></tr></thead><tbody id='logRows'></tbody></table></div></section>",
        "</main><script>",
        "let lastState={}, logTab='trades';",
        "const $=id=>document.getElementById(id);",
        "const n=v=>Number(v||0);",
        "const money=v=>n(v).toFixed(2)+'U';",
        "const pct=v=>(n(v)*100).toFixed(4)+'%';",
        "const cls=v=>n(v)>=0?'pos':'neg';",
        "const esc=v=>String(v??'').replace(/[&<>\"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',\"'\":'&#39;'}[c]));",
        "async function api(path,opt={}){const r=await fetch(path,{headers:{'content-type':'application/json'},...opt});if(!r.ok)throw new Error(await r.text());return await r.json();}",
        "function payload(){return ['capital_usdt','execution_notional_usdt','max_position_pct','min_funding_rate','min_price_gap_rate','max_basis_rate','min_execution_profit_usdt','limit'].reduce((a,k)=>(a[k]=n($(k).value),a),{});}",
        "function setStatus(s){$('status').textContent=s;}",
        "function card(k,v,c=''){return `<div class='card'><div class='label'>${k}</div><div class='value ${c}'>${v}</div></div>`;}",
        "function renderCards(proc,state){const ets=proc.ets||{};const acc=proc.account||{};$('cards').innerHTML=[card('账户权益',money(state.equity??acc.equity),cls(state.equity??acc.equity)),card('账户余额',money(state.balance??acc.balance)),card('持仓数',(state.positions||[]).length),card('记录数',(state.logs||[]).length),card('ETS Ticker',ets.tickers||0),card('ETS Funding',ets.funding||0),card('ETS机会',ets.opportunities||0)].join('');}",
        "function renderProcesses(proc){const rows=(proc.exchanges||[]).map(e=>{const t=e.ticker||{},f=e.funding||{};return `<tr><td>${esc(e.exchange)}</td><td>${esc(t.ws_enabled)}</td><td>${(t.subscriptions||[]).length}</td><td>${esc(f.last_count??'-')}</td><td>${esc(f.last_refresh_ms??'-')}</td><td>${esc(f.last_error??'-')}</td></tr>`}).join('');$('processRows').innerHTML=rows||'<tr><td colspan=6>暂无进程状态。</td></tr>';}",
        "function renderOps(ops){$('opRows').innerHTML=(ops||[]).map(o=>`<tr><td>${esc(o.symbol)}</td><td>${esc(o.opportunity_type)}</td><td>${esc(o.method)}</td><td>${esc(o.long_exchange)}</td><td>${esc(o.short_exchange)}</td><td>${money(o.notional_usdt)}</td><td class='${cls(o.expected_net_return)}'>${pct(o.expected_net_return)}</td><td class='${cls(o.estimated_net_profit_usdt)}'>${money(o.estimated_net_profit_usdt)}</td><td>${pct(o.funding_edge)}</td><td>${pct(o.price_gap_rate)}</td><td>${esc(o.settlement_countdown||'-')}</td></tr>`).join('')||'<tr><td colspan=11>暂无机会。</td></tr>';}",
        "function renderPositions(state){$('positionRows').innerHTML=(state.positions||[]).map(p=>`<tr><td>${esc(p.symbol)}</td><td>${esc(p.long_exchange)}</td><td>${esc(p.short_exchange)}</td><td>${money(p.notional_usdt)}</td><td>${esc(p.long_price??'-')}</td><td>${esc(p.short_price??'-')}</td><td class='${cls(p.unrealized_pnl_usdt)}'>${money(p.unrealized_pnl_usdt)}</td><td class='${cls(p.expected_profit_usdt)}'>${money(p.expected_profit_usdt)}</td><td>${esc(p.created_at_ms??'-')}</td></tr>`).join('')||'<tr><td colspan=9>暂无持仓。</td></tr>';}",
        "function setLogTab(tab){logTab=tab;$('tabLogs').className=tab==='trades'?'active':'';$('tabSkipped').className=tab==='skipped'?'active':'';renderLogs(lastState);}",
        "function renderLogs(state){let logs=state.logs||[];logs=logs.filter(x=>logTab==='skipped'?String(x.action||'').includes('skip')||String(x.action||'').includes('跳'):!(String(x.action||'').includes('skip')||String(x.action||'').includes('跳')));$('logRows').innerHTML=logs.slice(0,200).map(x=>`<tr><td>${esc(x.time||x.ts_ms||'-')}</td><td>${esc(x.action)}</td><td>${esc(x.symbol)}</td><td>${esc(x.long_exchange)}</td><td>${esc(x.short_exchange)}</td><td>${money(x.notional_usdt)}</td><td class='${cls(x.net_pnl_usdt)}'>${money(x.net_pnl_usdt)}</td><td>${esc(x.reason||'-')}</td></tr>`).join('')||'<tr><td colspan=8>暂无记录。</td></tr>';}",
        "async function refreshAll(){try{setStatus('正在刷新状态...');const [proc,state,exec]=await Promise.all([api('/api/processes'),api('/api/funding/state'),api('/api/executor/state')]);lastState=state;renderCards(proc,state);renderProcesses(proc);renderOps(exec.last_opportunities||[]);renderPositions(state);renderLogs(state);setStatus('状态已刷新。')}catch(e){setStatus('刷新失败: '+e.message)}}",
        "async function scanOnce(){try{setStatus('正在扫描 ETS 数据...');const r=await api('/api/funding/scan',{method:'POST',body:JSON.stringify(payload())});lastState=r.paper_account||{};renderOps(r.opportunities||[]);renderPositions(lastState);renderLogs(lastState);await refreshAll();setStatus(`扫描完成：命中 ${(r.opportunities||[]).length} 个机会。`)}catch(e){setStatus('扫描失败: '+e.message)}}",
        "async function resetPaper(){try{setStatus('正在重置模拟盘...');await api('/api/funding/paper/reset',{method:'POST',body:JSON.stringify({capital_usdt:n($('capital_usdt').value)})});await refreshAll();setStatus('模拟盘已重置。')}catch(e){setStatus('重置失败: '+e.message)}}",
        "refreshAll();setInterval(refreshAll,5000);",
        "</script></body></html>"
    ]).

strip_query(Path) ->
    hd(binary:split(Path, <<"?">>)).

lower_trim(Bin) ->
    string:lowercase(trim(Bin)).

trim(Bin) ->
    list_to_binary(string:trim(binary_to_list(Bin))).

fmt(Term) ->
    unicode:characters_to_binary(io_lib:format("~p", [Term])).
