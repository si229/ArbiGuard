let lastState = {};
let lastLive = {};
let logTab = "trades";
let tradePage = {page: 1, page_size: 50, total: 0, total_pages: 0, trades: []};
let liveConfigDirty = false;
let liveConfigDraft = [];
const knownExchanges = ["binance", "gate", "okx", "htx", "weex"];

const $ = id => document.getElementById(id);
const num = v => Number(v || 0);
const money = v => `${num(v).toFixed(2)}U`;
const pct = v => `${(num(v) * 100).toFixed(4)}%`;
const cls = v => num(v) >= 0 ? "pos" : "neg";

function display(v) {
  return v === undefined || v === null || v === "undefined" || v === "" ? "-" : v;
}

function esc(v) {
  return String(display(v)).replace(/[&<>"']/g, c => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#39;"
  }[c]));
}

function timeMs(v) {
  const x = num(v);
  if (x <= 0) return "-";
  const d = new Date(x);
  const pad = (n, w = 2) => String(n).padStart(w, "0");
  return `${d.getFullYear()}.${d.getMonth() + 1}.${d.getDate()} ${pad(d.getHours())}.${pad(d.getMinutes())}.${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}

function countdown(t) {
  const ms = num(t) - Date.now();
  if (ms <= 0) return "-";
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  return h > 0 ? `${h}小时${m}分${sec}秒` : `${m}分${sec}秒`;
}

function nearestFunding(a, b) {
  const xs = [num(a), num(b)].filter(x => x > 0);
  return xs.length ? Math.min(...xs) : 0;
}

function quoteCell(bid, ask, ts) {
  return `${esc(bid ?? "-")} / ${esc(ask ?? "-")}<br><span class="muted">@ ${esc(timeMs(ts))}</span>`;
}

function statusText(s) {
  return ({
    filled_paper_open: "模拟开仓已成交",
    waiting_ws_ticker: "等待行情",
    filled_live_open: "实盘开仓已成交",
    awaiting_live_open_fill: "等待实盘成交",
    partial_live_open_continue: "实盘部分成交继续"
  })[s] || display(s);
}

async function api(path, opt = {}) {
  const r = await fetch(path, {headers: {"content-type": "application/json"}, ...opt});
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

function setStatus(s) {
  $("globalStatus").textContent = s;
}

function card(k, v, c = "") {
  return `<div class="card"><div class="label">${esc(k)}</div><div class="value ${c}">${esc(v)}</div></div>`;
}

function payload() {
  const keys = [
    "capital_usdt",
    "execution_notional_usdt",
    "paper_leverage",
    "max_open_positions",
    "max_position_pct",
    "min_execution_profit_usdt",
    "limit"
  ];
  const p = keys.reduce((a, k) => (a[k] = num($(k).value), a), {});
  p.account_mode = $("account_mode").value;
  return p;
}

function setMainTab(tab) {
  const views = {
    dashboard: ["dashboardView", "mainTabDashboard"],
    liveConfig: ["liveConfigView", "mainTabLiveConfig"],
    localDebug: ["localDebugView", "mainTabLocalDebug"]
  };
  Object.entries(views).forEach(([name, [viewId, tabId]]) => {
    $(viewId).className = name === tab ? "view active" : "view";
    $(tabId).className = name === tab ? "active" : "";
  });
}

function renderCards(proc, state, live) {
  const ets = proc.ets || {};
  const acc = proc.account || {};
  const ex = proc.executor || {};
  const sw = proc.symbol_watcher || {};
  const watched = (sw.exchanges || []).reduce((a, x) => a + num(x.symbols), 0);
  $("cards").innerHTML = [
    card("模拟权益", money(state.equity ?? acc.equity), cls(state.equity ?? acc.equity)),
    card("模拟余额", money(state.balance ?? acc.balance)),
    card("当前持仓", (state.positions || []).length),
    card("实盘启用", String(live.enabled)),
    card("开仓执行单", (ex.orders || []).length),
    card("平仓执行单", (ex.close_orders || []).length),
    card("监听币种", watched),
    card("ETS 行情", ets.tickers || 0),
    card("ETS 资金费", ets.funding || 0)
  ].join("");
}

function renderExchangeFunds(state) {
  const balances = state.exchange_balances || {};
  const equity = state.exchange_equity || {};
  const margin = state.exchange_margin || {};
  const pnl = state.exchange_unrealized_pnl || {};
  const ids = [...new Set([...Object.keys(balances), ...Object.keys(equity), ...Object.keys(margin), ...Object.keys(pnl)])].sort();
  $("exchangeFundsRows").innerHTML = ids.map(id => {
    const eq = num(equity[id]);
    const u = num(pnl[id]);
    return `<tr><td>${esc(id)}</td><td>${money(balances[id])}</td><td class="${cls(eq)}">${money(eq)}</td><td>${money(margin[id])}</td><td class="${cls(u)}">${money(u)}</td></tr>`;
  }).join("") || `<tr><td colspan="5">暂无交易所资金数据。</td></tr>`;
}

function renderExchangeFees(config) {
  const rows = config.exchanges || [];
  const el = $("exchangeFeeRows");
  if (!el) return;
  el.innerHTML = rows.map(e => {
    const maker = num(e.maker_fee_rate);
    const taker = num(e.taker_fee_rate);
    const rebate = num(e.fee_rebate_rate);
    const effective = taker * Math.max(0, 1 - rebate);
    return `<tr data-exchange="${esc(e.id)}">
      <td>${esc(e.id)}</td>
      <td><input class="fee-maker" value="${maker}"></td>
      <td><input class="fee-taker" value="${taker}"></td>
      <td><input class="fee-rebate" value="${rebate}"></td>
      <td>${effective.toFixed(8)}</td>
      <td><button onclick="saveExchangeFee('${esc(e.id)}')">保存费率</button></td>
    </tr>`;
  }).join("") || `<tr><td colspan="6">暂无交易所费率配置。</td></tr>`;
}

async function saveExchangeFee(exchange) {
  try {
    const row = document.querySelector(`#exchangeFeeRows tr[data-exchange="${CSS.escape(exchange)}"]`);
    if (!row) return;
    const body = {
      exchange,
      maker_fee_rate: num(row.querySelector(".fee-maker")?.value),
      taker_fee_rate: num(row.querySelector(".fee-taker")?.value),
      fee_rebate_rate: num(row.querySelector(".fee-rebate")?.value)
    };
    const r = await api("/api/config/exchange/fees", {method: "POST", body: JSON.stringify(body)});
    if (r.ok === false) throw new Error(display(r.error || r.reason));
    setStatus(`已更新 ${exchange} 费率：实际 taker=${num(r.effective_taker_fee_rate).toFixed(8)}，下一轮扫描生效。`);
    await refreshAll();
  } catch (e) {
    setStatus(`保存交易所费率失败：${e.message}`);
  }
}

function renderProcesses(proc) {
  $("processRows").innerHTML = (proc.exchanges || []).map(e => {
    const t = e.ticker || {};
    const f = e.funding || {};
    const p = e.private_ws || {};
    return `<tr>
      <td>${esc(e.exchange)}</td>
      <td>${esc(t.ws_url || "-")}</td>
      <td class="${t.ws_connected ? "pos" : "warn"}">${esc(t.ws_connected)}</td>
      <td>${esc(t.ws_status || "-")}</td>
      <td>${(t.subscriptions || []).length}</td>
      <td>${esc(p.ws_url || "-")}</td>
      <td class="${p.ws_connected ? "pos" : "warn"}">${esc(p.ws_connected)}</td>
      <td class="${p.logged_in ? "pos" : "warn"}">${esc(p.logged_in)}</td>
      <td class="${p.subscribed ? "pos" : "warn"}">${esc(p.subscribed)}</td>
      <td>${esc(f.last_count ?? "-")}</td>
      <td>${esc(p.ws_error || t.ws_error || f.last_error || "-")}</td>
    </tr>`;
  }).join("") || `<tr><td colspan="11">暂无进程状态。</td></tr>`;
}

function renderOrders(exec) {
  const rows = (exec.orders || []).slice(0, 200);
  $("orderRows").innerHTML = rows.map(o => {
    const d = o.wait_detail || {};
    const progress = num(o.progress_pct);
    const reason = String(o.status || "").startsWith("filled") ? "-" : (o.wait_reason || d.reason || "-");
    return `<tr>
      <td>${esc(o.symbol)}</td><td>${esc(o.long_exchange)}</td><td>${esc(o.short_exchange)}</td>
      <td>${esc(statusText(o.status))}</td><td>${esc(reason)}</td>
      <td>${money(o.target_notional)}</td><td>${money(o.confirmed_notional)}</td><td>${progress.toFixed(1)}%</td>
      <td class="neg">${money(o.execution_fee)}</td><td class="${cls(o.actual_pnl)}">${money(o.actual_pnl)}</td>
      <td>${quoteCell(d.long_bid, d.long_ask, d.long_updated_at)}</td>
      <td>${quoteCell(d.short_bid, d.short_ask, d.short_updated_at)}</td>
      <td class="${cls(o.expected_net_profit)}">${money(o.expected_net_profit)}</td>
    </tr>`;
  }).join("") || `<tr><td colspan="13">暂无开仓执行单。</td></tr>`;
}

function renderOpenRejects(exec) {
  const rows = (exec.open_rejects || []).slice(0, 80);
  $("openRejectRows").innerHTML = rows.map(r => `<tr>
    <td>${esc(timeMs(r.time))}</td><td>${esc(r.symbol)}</td><td>${esc(r.long_exchange)}</td><td>${esc(r.short_exchange)}</td>
    <td>${esc(r.reason)}</td><td class="${cls(r.estimated_net_profit)}">${money(r.estimated_net_profit)}</td>
    <td>${money(r.min_execution_profit_usdt)}</td><td>${esc(r.current_positions)}/${esc(r.max_open_positions)}</td><td>${esc(r.active_open_orders)}</td>
  </tr>`).join("") || `<tr><td colspan="9">暂无过滤记录。</td></tr>`;
}

function renderOps(ops) {
  $("opRows").innerHTML = (ops || []).map(o => `<tr>
    <td>${esc(o.symbol)}</td><td>${esc(o.opportunity_type)}</td><td>${esc(o.method || o.arbitrage_method)}</td>
    <td>${esc(o.long_exchange)}</td><td>${esc(o.short_exchange)}</td><td>${money(o.notional_usdt ?? o.suggested_notional)}</td>
    <td class="${cls(o.expected_net_return)}">${pct(o.expected_net_return)}</td>
    <td class="${cls(o.estimated_net_profit_usdt ?? o.estimated_net_profit)}">${money(o.estimated_net_profit_usdt ?? o.estimated_net_profit)}</td>
    <td>${pct(o.funding_edge ?? o.funding_edge_return)}</td><td>${pct(o.price_gap_rate ?? o.price_gap_return)}</td>
    <td>${esc(countdown(nearestFunding(o.long_next_funding_time, o.short_next_funding_time)))}</td>
  </tr>`).join("") || `<tr><td colspan="11">暂无套利机会。</td></tr>`;
}

function renderOpportunityStats(result) {
  const el = $("opStats");
  if (!el) return;
  const stats = result?.opportunity_exchange_stats || {};
  const total = stats.total ?? result?.opportunity_count ?? 0;
  const executable = stats.executable ?? result?.execution_opportunity_count ?? 0;
  const byExchange = stats.by_exchange || [];
  const executableByExchange = {};
  (stats.executable_by_exchange || []).forEach(x => { executableByExchange[x.exchange] = x.count; });
  const bestByExchange = {};
  (stats.best_by_exchange || []).forEach(x => { bestByExchange[x.exchange] = x; });
  const rows = byExchange.map(x => {
    const best = bestByExchange[x.exchange] || {};
    return `${x.exchange}: ${x.count}个 / 可执行${executableByExchange[x.exchange] || 0}个 / 最优${money(best.estimated_net_profit)} ${best.symbol || "-"}`;
  });
  el.textContent = `扫描机会：总${total}个，可执行${executable}个，展示${result?.display_limit ?? result?.opportunities?.length ?? 0}个。${rows.join("；")}`;
}

function renderPositions(state) {
  $("positionRows").innerHTML = (state.positions || []).map(p => {
    const lr = num(p.long_funding_settlement_count);
    const sr = num(p.short_funding_settlement_count);
    return `<tr>
      <td>${esc(p.symbol)}</td><td>${esc(p.long_exchange)}</td><td>${esc(p.short_exchange)}</td>
      <td>${money(p.notional_usdt || p.notional)}</td>
      <td>${esc(p.long_entry_price ?? "-")}</td><td>${esc(p.long_current_price ?? "-")}</td>
      <td>${esc(p.short_entry_price ?? "-")}</td><td>${esc(p.short_current_price ?? "-")}</td>
      <td class="${cls(p.unrealized_pnl)}">${money(p.unrealized_pnl)}</td>
      <td class="${cls(p.expected_net_profit)}">${money(p.expected_net_profit)}</td>
      <td class="${cls(p.next_funding_estimated_pnl)}">${money(p.next_funding_estimated_pnl)} (${esc(p.next_funding_scope || "-")})</td>
      <td>${esc(timeMs(p.next_funding_time))}</td>
      <td>${lr + sr} (${lr}/${sr})</td><td class="${cls(p.funding_pnl)}">${money(p.funding_pnl)}</td>
      <td>${esc(timeMs(p.opened_at))}</td>
    </tr>`;
  }).join("") || `<tr><td colspan="15">暂无持仓。</td></tr>`;
}

function renderLiveAccounts(live) {
  const accounts = live.accounts || [];
  renderLiveExchangeConfig(live);
  renderAccountSelects(accounts, live.account_id || "live-main");
  $("liveAccountRows").innerHTML = accounts.map(a => {
    const exchanges = a.exchanges || Object.keys(a.exchange_accounts || {});
    const configured = a.token_configured_exchanges || [];
    return `<tr>
      <td>${esc(a.id)}</td>
      <td>${esc(a.mode)}</td>
      <td class="${a.live_enabled ? "pos" : "warn"}">${esc(a.live_enabled)}</td>
      <td>${esc(exchanges.join(", ") || "-")}</td>
      <td>${esc(configured.join(", ") || "-")}</td>
      <td>${esc(a.open_executor || "-")}</td>
      <td>${esc(a.close_executor || "-")}</td>
    </tr>`;
  }).join("") || `<tr><td colspan="7">暂无实盘账户状态。</td></tr>`;
  $("live_enabled").value = String(Boolean(live.enabled));
}

function renderAccountSelects(accounts, selected) {
  const ids = [...new Set((accounts || []).map(a => a.id).filter(Boolean))];
  if (!ids.includes("live-main")) ids.unshift("live-main");
  const html = ids.map(id => `<option value="${esc(id)}"${id === selected ? " selected" : ""}>${esc(id)}</option>`).join("");
  ["debug_account_id", "test_account_id"].forEach(id => {
    const el = $(id);
    if (!el) return;
    const old = el.value || selected;
    el.innerHTML = html;
    if (ids.includes(old)) el.value = old;
  });
}

function renderLiveExchanges(live) {
  const account = (live.accounts || []).find(a => a.id === (live.account_id || "live-main")) || {};
  const exchangeAccounts = account.exchange_accounts || {};
  const configured = new Set(account.token_configured_exchanges || live.token_configured_exchanges || []);
  const states = {};
  (live.exchange_states || []).forEach(s => { states[s.exchange] = s; });
  const ids = [...new Set([
    ...Object.keys(exchangeAccounts),
    ...Array.from(configured),
    ...(live.exchange_accounts || [])
  ])].sort();
  $("liveExchangeRows").innerHTML = ids.map(id => {
    const info = exchangeAccounts[id] || {};
    const st = states[id] || {};
    return `<tr>
      <td>${esc(account.id || live.account_id || "live-main")}</td>
      <td>${esc(id)}</td>
      <td class="${configured.has(id) ? "pos" : "warn"}">${configured.has(id) ? "yes" : "no"}</td>
      <td>${esc(info.account_process || "-")}</td>
      <td>${esc(info.private_ws || "-")}</td>
      <td>${esc(st.last_sync_status || "-")}</td>
      <td>${esc(timeMs(st.last_sync_at))}</td>
      <td>${esc(st.last_sync_error || st.error || "-")}</td>
    </tr>`;
  }).join("") || `<tr><td colspan="8">这个总账户还没有添加交易所。先选择交易所并保存 API 配置。</td></tr>`;
}

function renderLiveExchangeConfig(live) {
  if (liveConfigDirty) return;
  const accountId = $("live_account_id")?.value || live.account_id || "live-main";
  const account = (live.accounts || []).find(a => a.id === accountId) || (live.accounts || []).find(a => a.id === (live.account_id || "live-main")) || {};
  const exchangeAccounts = account.exchange_accounts || {};
  const configured = new Set(account.token_configured_exchanges || live.token_configured_exchanges || []);
  const states = {};
  (live.exchange_states || []).forEach(s => { states[s.exchange] = s; });
  const ids = [...new Set([
    ...Object.keys(exchangeAccounts),
    ...Array.from(configured),
    ...(live.exchange_accounts || [])
  ])].sort();
  liveConfigDraft = ids.map(id => ({
    exchange: id,
    configured: configured.has(id),
    info: exchangeAccounts[id] || {},
    state: states[id] || {},
    deleted: false,
    isNew: false
  }));
  renderLiveConfigDraft();
}

function exchangeOptions(selected) {
  return knownExchanges.map(id => `<option value="${esc(id)}"${id === selected ? " selected" : ""}>${esc(id)}</option>`).join("");
}

function renderLiveConfigDraft() {
  const el = $("liveExchangeConfigRows");
  if (!el) return;
  el.innerHTML = liveConfigDraft.map((row, i) => {
    const st = row.state || {};
    const info = row.info || {};
    const disabled = row.deleted ? " disabled" : "";
    const trCls = row.deleted ? " class=\"muted\"" : "";
    return `<tr data-index="${i}" data-configured="${row.configured ? "true" : "false"}" data-deleted="${row.deleted ? "true" : "false"}"${trCls}>
      <td><select class="live-row-exchange" onchange="markLiveConfigDirty()"${disabled}>${exchangeOptions(row.exchange)}</select></td>
      <td><input class="live-row-api-key" autocomplete="off" placeholder="${row.configured ? "已保存，修改时重填" : ""}"${disabled}></td>
      <td><input class="live-row-api-secret" type="password" autocomplete="off" placeholder="${row.configured ? "已保存，修改时重填" : ""}"${disabled}></td>
      <td><input class="live-row-passphrase" type="password" autocomplete="off"${disabled}></td>
      <td><input class="live-row-access-token" type="password" autocomplete="off"${disabled}></td>
      <td><input class="live-row-note" value="${esc(row.note || "")}" onchange="markLiveConfigDirty()"${disabled}></td>
      <td class="${row.configured ? "pos" : "warn"}">${row.configured ? "yes" : "no"}</td>
      <td>${esc(info.account_process || "-")}</td>
      <td>${esc(info.private_ws || "-")}</td>
      <td>${esc(st.last_sync_status || "-")}</td>
      <td>${esc(timeMs(st.last_sync_at))}</td>
      <td>${esc(st.last_sync_error || st.error || "-")}</td>
      <td><button onclick="removeLiveExchangeRow(${i})">${row.deleted ? "撤销" : "删除"}</button></td>
    </tr>`;
  }).join("") || `<tr><td colspan="13">暂无交易所配置。</td></tr>`;
  el.querySelectorAll("input,select").forEach(x => x.addEventListener("input", markLiveConfigDirty));
}

function markLiveConfigDirty() {
  liveConfigDirty = true;
}

function addLiveExchangeRow() {
  liveConfigDirty = true;
  liveConfigDraft.push({exchange: "binance", configured: false, info: {}, state: {}, deleted: false, isNew: true});
  renderLiveConfigDraft();
}

function removeLiveExchangeRow(index) {
  liveConfigDirty = true;
  const row = liveConfigDraft[index];
  if (!row) return;
  if (row.isNew && !row.configured) {
    liveConfigDraft.splice(index, 1);
  } else {
    row.deleted = !row.deleted;
  }
  renderLiveConfigDraft();
}

function setLogTab(tab) {
  logTab = tab;
  $("tabLogs").className = tab === "trades" ? "active" : "";
  $("tabSkipped").className = tab === "skipped" ? "active" : "";
  $("tabLive").className = tab === "live" ? "active" : "";
  if (tab === "trades") loadTradeHistory(1);
  else renderLogs(lastState);
}

async function loadTradeHistory(page) {
  try {
    const mode = $("history_account_mode").value;
    const action = $("history_action").value;
    const ps = num($("history_page_size").value) || 50;
    const q = new URLSearchParams({account_mode: mode, page: String(Math.max(1, page || 1)), page_size: String(ps)});
    if (action) q.set("action", action);
    tradePage = await api(`/api/trades/history?${q.toString()}`);
    $("history_page").value = tradePage.page || 1;
    renderLogs(lastState);
  } catch (e) {
    $("historyPager").textContent = `历史记录查询失败：${e.message}`;
  }
}

function nextTradePage(delta) {
  const p = num(tradePage.page) || 1;
  const total = num(tradePage.total_pages) || 1;
  loadTradeHistory(Math.min(total, Math.max(1, p + delta)));
}

function renderLogs(state) {
  const logs = logTab === "live"
    ? (state.live_logs || [])
    : (logTab === "trades" ? (tradePage.trades || []) : (state.logs || []).filter(x => String(x.action || "").includes("skip")));
  $("historyPager").textContent = logTab === "trades"
    ? `历史记录：第 ${tradePage.page || 1}/${tradePage.total_pages || 0} 页，共 ${tradePage.total || 0} 条`
    : "未执行 / 实盘请求使用当前内存状态展示";
  $("logRows").innerHTML = logs.slice(0, 500).map(x => `<tr>
    <td>${esc(timeMs(x.time || x.ts_ms))}</td><td>${esc(x.action)}</td><td>${esc((x.account_mode || "-") + "/" + (x.account_id || "-"))}</td>
    <td>${esc(x.symbol)}</td><td>${esc(x.long_exchange)}</td><td>${esc(x.short_exchange)}</td>
    <td>${money(x.notional_usdt || x.notional)}</td><td class="${cls(x.net_pnl)}">${money(x.net_pnl)}</td><td>${esc(x.status || x.reason || "-")}</td>
  </tr>`).join("") || `<tr><td colspan="9">暂无记录。</td></tr>`;
}

function renderDebugState(live) {
  const b = live.debug_balances || {};
  $("debugBalanceRows").innerHTML = Object.keys(b).sort().map(k => `<tr><td>${esc(k)}</td><td>${money(b[k])}</td></tr>`).join("") || `<tr><td colspan="2">暂无调试余额。</td></tr>`;
  const ps = live.debug_positions || [];
  $("debugPositionRows").innerHTML = ps.map(p => `<tr>
    <td>${esc(p.id)}</td><td>${esc(p.exchange)}</td><td>${esc(p.symbol)}</td><td>${esc(p.side)}</td>
    <td>${money(p.notional)}</td><td>${esc(num(p.qty).toFixed(8))}</td><td>${esc(p.entry_price)}</td>
    <td>${money(p.margin)}</td><td>${money(p.open_fee)}</td><td>${esc(timeMs(p.opened_at))}</td>
  </tr>`).join("") || `<tr><td colspan="10">暂无调试持仓。</td></tr>`;
}

async function refreshAll() {
  try {
    setStatus("正在刷新...");
    const settled = await Promise.allSettled([
      api("/api/processes"),
      api("/api/funding/state"),
      api("/api/executor/state"),
      api("/api/live/state"),
      api("/api/config")
    ]);
    const value = (i, fallback) => settled[i].status === "fulfilled" ? settled[i].value : fallback;
    const proc = value(0, {});
    const state = value(1, lastState || {});
    const exec = value(2, {});
    const live = value(3, lastLive || {});
    const config = value(4, {});
    const failed = settled
      .map((x, i) => x.status === "rejected" ? ["进程", "模拟盘", "执行", "实盘", "配置"][i] : "")
      .filter(Boolean);
    proc.executor = exec;
    state.live_logs = live.logs || [];
    lastState = state;
    lastLive = live;
    renderCards(proc, state, live);
    renderExchangeFunds(state);
    renderExchangeFees(config);
    renderProcesses(proc);
    renderOrders(exec);
    renderOpenRejects(exec);
    renderOps(exec.last_opportunities || []);
    renderOpportunityStats(proc.scanner?.last_result || {});
    renderPositions(state);
    renderLiveAccounts(live);
    renderDebugState(live);
    if (logTab === "trades") await loadTradeHistory(num($("history_page").value) || 1);
    else renderLogs(state);
    setStatus(failed.length ? `部分刷新成功，失败接口：${failed.join("、")}` : "已刷新。");
  } catch (e) {
    setStatus(`刷新失败：${e.message}`);
  }
}

async function scanOnce() {
  try {
    setStatus("正在扫描 ETS...");
    const r = await api("/api/funding/scan", {method: "POST", body: JSON.stringify(payload())});
    lastState = r.paper_account || {};
    renderOps(r.opportunities || []);
    renderOpportunityStats(r);
    renderPositions(lastState);
    renderLogs(lastState);
    await refreshAll();
    setStatus(`扫描完成：命中 ${(r.opportunities || []).length} 个机会。`);
  } catch (e) {
    setStatus(`扫描失败：${e.message}`);
  }
}

async function applySettings() {
  try {
    setStatus("正在应用参数...");
    const r = await api("/api/funding/apply-settings", {method: "POST", body: JSON.stringify(payload())});
    await refreshAll();
    setStatus(`参数已应用，后续定时扫描会使用新参数，扫描周期 ${r.scanner_interval_ms || "-"} ms。`);
  } catch (e) {
    setStatus(`应用参数失败：${e.message}`);
  }
}

async function resetPaper() {
  try {
    setStatus("正在重置模拟盘...");
    await api("/api/funding/paper/reset", {method: "POST", body: JSON.stringify({capital_usdt: num($("capital_usdt").value)})});
    await refreshAll();
    setStatus("模拟盘已重置。");
  } catch (e) {
    setStatus(`重置失败：${e.message}`);
  }
}

async function saveLiveToken() {
  try {
    const account_id = $("live_account_id").value || "live-main";
    const enabled = $("live_enabled").value === "true";
    const exchange = $("live_exchange").value;
    await api("/api/live/enabled", {method: "POST", body: JSON.stringify({account_id, enabled})});
    const token = {
      account_id,
      exchange,
      api_key: $("live_api_key").value,
      api_secret: $("live_api_secret").value,
      passphrase: $("live_passphrase").value,
      access_token: $("live_access_token").value,
      note: $("live_note").value
    };
    await api("/api/live/token", {method: "POST", body: JSON.stringify(token)});
    $("liveConfigResult").textContent = `已添加/更新交易所配置：账户 ${account_id} / ${exchange} / enabled=${enabled}。其它已配置交易所不会被删除。`;
    $("live_api_key").value = "";
    $("live_api_secret").value = "";
    $("live_passphrase").value = "";
    $("live_access_token").value = "";
    await refreshAll();
  } catch (e) {
    $("liveConfigResult").textContent = `保存实盘配置失败：${e.message}`;
  }
}

async function saveLiveConfigRows() {
  try {
    const account_id = $("live_account_id").value || "live-main";
    const enabled = $("live_enabled").value === "true";
    await api("/api/live/enabled", {method: "POST", body: JSON.stringify({account_id, enabled})});
    const rows = Array.from(document.querySelectorAll("#liveExchangeConfigRows tr[data-index]"));
    let saved = 0;
    let deleted = 0;
    let skipped = 0;
    const seen = new Set();
    for (const row of rows) {
      const exchange = row.querySelector(".live-row-exchange")?.value || "";
      if (!exchange || seen.has(exchange)) {
        skipped += 1;
        continue;
      }
      seen.add(exchange);
      const configured = row.dataset.configured === "true";
      const isDeleted = row.dataset.deleted === "true";
      if (isDeleted) {
        if (configured) {
          const r = await api("/api/live/token/delete", {method: "POST", body: JSON.stringify({account_id, exchange})});
          if (r.ok === false) throw new Error(`删除 ${exchange} 被拒绝：${display(r.reason)} ${JSON.stringify(r.detail || {})}`);
          deleted += 1;
        }
        continue;
      }
      const token = {
        account_id,
        exchange,
        api_key: row.querySelector(".live-row-api-key")?.value || "",
        api_secret: row.querySelector(".live-row-api-secret")?.value || "",
        passphrase: row.querySelector(".live-row-passphrase")?.value || "",
        access_token: row.querySelector(".live-row-access-token")?.value || "",
        note: row.querySelector(".live-row-note")?.value || ""
      };
      const hasAuth = token.api_key.trim() && token.api_secret.trim();
      if (hasAuth) {
        const r = await api("/api/live/token", {method: "POST", body: JSON.stringify(token)});
        if (r.ok === false) throw new Error(`保存 ${exchange} 失败：${display(r.reason)}`);
        saved += 1;
      } else if (!configured) {
        skipped += 1;
      }
    }
    liveConfigDirty = false;
    $("liveConfigResult").textContent = `已保存：新增/修改 ${saved} 个，删除 ${deleted} 个，跳过 ${skipped} 个，实盘启用=${enabled}。`;
    await refreshAll();
  } catch (e) {
    $("liveConfigResult").textContent = `保存实盘配置失败：${e.message}`;
  }
}

saveLiveToken = saveLiveConfigRows;

async function syncLiveAccount() {
  try {
    const account_id = $("live_account_id").value || "live-main";
    const r = await api("/api/live/sync", {method: "POST", body: JSON.stringify({account_id})});
    $("liveConfigResult").textContent = `账户同步已提交：${r.account_id || account_id}`;
    await refreshAll();
  } catch (e) {
    $("liveConfigResult").textContent = `同步账户状态失败：${e.message}`;
  }
}

async function debugExchangeOrder() {
  try {
    const body = {
      account_id: $("debug_account_id").value || "live-main",
      exchange: $("debug_exchange").value,
      action: $("debug_action").value,
      symbol: $("debug_symbol").value,
      side: $("debug_side").value,
      notional: num($("debug_notional").value),
      price: num($("debug_price").value),
      leverage: num($("debug_leverage").value),
      order_id: $("debug_order_id").value,
      dry_run: true
    };
    const r = await api("/api/debug/exchange/order", {method: "POST", body: JSON.stringify(body)});
    $("debugResult").textContent = JSON.stringify(r);
    await refreshAll();
  } catch (e) {
    $("debugResult").textContent = `调试失败：${e.message}`;
  }
}

async function liveTestOrder() {
  try {
    const body = {
      account_id: $("test_account_id").value || "live-main",
      exchange: $("test_exchange").value,
      action: $("live_action").value,
      symbol: $("live_symbol").value,
      side: $("live_side").value,
      order_type: $("live_order_type").value,
      quantity: num($("live_quantity").value),
      price: num($("live_price").value),
      leverage: num($("live_leverage").value),
      reduce_only: $("live_reduce_only").value === "true",
      client_order_id: $("live_client_order_id").value,
      confirm: $("live_confirm").value
    };
    const r = await api("/api/live/test-order", {method: "POST", body: JSON.stringify(body)});
    $("liveTestResult").textContent = JSON.stringify(r);
    await refreshAll();
  } catch (e) {
    $("liveTestResult").textContent = `实盘测试失败：${e.message}`;
  }
}

refreshAll();
setInterval(refreshAll, 5000);
