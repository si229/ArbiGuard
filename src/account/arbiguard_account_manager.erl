-module(arbiguard_account_manager).
-behaviour(gen_server).

-export([start_link/0, snapshot/0, create_account/1, account/1,
         open_executor/1, close_executor/1,
         track_position/2, report_order_event/3,
         report_funding_settlement/3, set_live_enabled/2, set_exchange_token/3,
         get_exchange_token/2, exchange_snapshot_synced/3, sync_account/1, test_order/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {accounts = #{}}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

snapshot() ->
    gen_server:call(?MODULE, snapshot).

create_account(Config) ->
    gen_server:call(?MODULE, {create_account, Config}).

account(AccountID) ->
    gen_server:call(?MODULE, {account, AccountID}).

open_executor(AccountID) ->
    case account(AccountID) of
        {ok, Account} -> {ok, maps:get(open_executor, Account)};
        Error -> Error
    end.

close_executor(AccountID) ->
    case account(AccountID) of
        {ok, Account} -> {ok, maps:get(close_executor, Account)};
        Error -> Error
    end.

track_position(Req, Position) ->
    gen_server:cast(?MODULE, {track_position, Req, Position}),
    ok.

report_order_event(AccountID, ExchangeID, Event) ->
    gen_server:cast(?MODULE, {report_order_event, AccountID, ExchangeID, Event}).

report_funding_settlement(AccountID, PositionID, Settlement) ->
    gen_server:cast(?MODULE, {report_funding_settlement, AccountID, PositionID, Settlement}).

set_live_enabled(AccountID, Enabled) ->
    gen_server:call(?MODULE, {set_live_enabled, AccountID, Enabled}).

set_exchange_token(AccountID, ExchangeID, Token) ->
    gen_server:call(?MODULE, {set_exchange_token, AccountID, ExchangeID, Token}).

get_exchange_token(AccountID, ExchangeID) ->
    case account(AccountID) of
        {ok, _Account} -> arbiguard_exchange_account:get_token(AccountID, ExchangeID);
        _ -> undefined
    end.

exchange_snapshot_synced(AccountID, ExchangeID, Snapshot) ->
    gen_server:cast(?MODULE, {exchange_snapshot_synced, AccountID, ExchangeID, Snapshot}).

sync_account(AccountID) ->
    gen_server:cast(?MODULE, {sync_account, AccountID}).

test_order(Payload) ->
    gen_server:call(?MODULE, {test_order, Payload}, 30000).

init([]) ->
    Paper = default_paper_account(),
    {Live, _LiveLog} = start_account_workers(default_live_config()),
    {ok, #state{accounts = #{maps:get(id, Paper) => Paper, maps:get(id, Live) => Live}}}.

handle_call(snapshot, _From, State = #state{accounts = Accounts}) ->
    {reply, #{accounts => [public_account(A) || {_ID, A} <- maps:to_list(Accounts)]}, State};
handle_call({account, AccountID0}, _From, State = #state{accounts = Accounts}) ->
    AccountID = norm_account_id(AccountID0),
    {reply, case maps:find(AccountID, Accounts) of
                {ok, Account} -> {ok, Account};
                error -> {error, account_not_found}
            end, State};
handle_call({create_account, Config0}, _From, State = #state{accounts = Accounts}) ->
    Config = normalize_config(Config0),
    AccountID = maps:get(id, Config),
    case maps:is_key(AccountID, Accounts) of
        true ->
            {reply, #{ok => false, reason => <<"account_exists">>, account_id => AccountID}, State};
        false ->
            {Account, StartLog} = start_account_workers(Config),
            lager:info("account manager created account=~s mode=~s exchanges=~p",
                       [AccountID, maps:get(mode, Account), maps:get(exchanges, Account, [])]),
            {reply, #{ok => true, account => public_account(Account), start_log => StartLog},
             State#state{accounts = Accounts#{AccountID => Account}}}
    end;
handle_call({set_live_enabled, AccountID0, Enabled0}, _From, State = #state{accounts = Accounts}) ->
    AccountID = norm_account_id(AccountID0),
    Enabled = truthy(Enabled0),
    case maps:get(AccountID, Accounts, undefined) of
        undefined ->
            {reply, #{ok => false, reason => <<"account_not_found">>, account_id => AccountID}, State};
        Account ->
            Account1 = Account#{live_enabled => Enabled},
            broadcast_executor_meta(Account1),
            lager:warning("account manager live enabled account=~s enabled=~p", [AccountID, Enabled]),
            {reply, #{ok => true, account_id => AccountID, enabled => Enabled},
             State#state{accounts = Accounts#{AccountID => Account1}}}
    end;
handle_call({set_exchange_token, AccountID0, ExchangeID0, Token0}, _From, State = #state{accounts = Accounts}) ->
    AccountID = norm_account_id(AccountID0),
    ExchangeID = norm_exchange(ExchangeID0),
    Token = maps:without([account_id, <<"account_id">>, exchange, <<"exchange">>], ensure_map(Token0)),
    case ensure_account_exchange(AccountID, ExchangeID, Accounts) of
        {ok, Account, Accounts1} ->
            ok = arbiguard_exchange_account:set_token(AccountID, ExchangeID, Token),
            catch arbiguard_private_ws:refresh_auth(AccountID, ExchangeID),
            Account2 = add_exchange_to_account(Account, ExchangeID),
            Tokens = maps:get(exchange_tokens, Account2, #{}),
            Account1 = Account2#{exchange_tokens => Tokens#{ExchangeID => Token}},
            broadcast_executor_meta(Account1),
            {reply, #{ok => true, account_id => AccountID, exchange => ExchangeID},
             State#state{accounts = Accounts1#{AccountID => Account1}}};
        {error, Reason} ->
            {reply, #{ok => false, reason => Reason, account_id => AccountID, exchange => ExchangeID}, State}
    end;
handle_call({test_order, Payload0}, _From, State = #state{accounts = Accounts}) ->
    Payload = normalize_test_payload(Payload0),
    AccountID = maps:get(account_id, Payload),
    ExchangeID = maps:get(exchange, Payload),
    Token = arbiguard_exchange_account:get_token(AccountID, ExchangeID),
    Result = case {maps:get(AccountID, Accounts, undefined), Token, maps:get(confirm, Payload, <<"">>)} of
        {undefined, _, _} -> #{ok => false, status => <<"rejected">>, reason => <<"account_not_found">>};
        {_, undefined, _} -> #{ok => false, status => <<"rejected">>, reason => <<"live_token_not_configured">>};
        {_, _, <<"LIVE">>} -> arbiguard_live_adapter:test_order(Payload, Token);
        {_, _, _} -> #{ok => false, status => <<"rejected">>, reason => <<"confirm_live_required">>}
    end,
    {reply, Result#{account_id => AccountID, exchange => ExchangeID}, State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({report_order_event, AccountID0, _ExchangeID, Event}, State = #state{accounts = Accounts}) ->
    AccountID = norm_account_id(AccountID0),
    case maps:get(AccountID, Accounts, undefined) of
        undefined -> ok;
        Account ->
            Open = maps:get(open_executor, Account),
            Close = maps:get(close_executor, Account),
            Open ! {live_order_update, Event},
            Close ! {live_order_update, Event}
    end,
    {noreply, State};
handle_cast({exchange_snapshot_synced, AccountID0, ExchangeID0, Snapshot0}, State = #state{accounts = Accounts}) ->
    AccountID = norm_account_id(AccountID0),
    ExchangeID = norm_exchange(ExchangeID0),
    case maps:get(AccountID, Accounts, undefined) of
        undefined ->
            {noreply, State};
        Account ->
            Snapshots0 = maps:get(exchange_snapshots, Account, #{}),
            Snapshot = Snapshot0#{exchange => ExchangeID, synced_at => arbiguard_util:now_ms()},
            Account1 = Account#{exchange_snapshots => Snapshots0#{ExchangeID => Snapshot}},
            sync_live_pairs_to_close_executor(Account1),
            {noreply, State#state{accounts = Accounts#{AccountID => Account1}}}
    end;
handle_cast({sync_account, AccountID0}, State = #state{accounts = Accounts}) ->
    AccountID = norm_account_id(AccountID0),
    case maps:get(AccountID, Accounts, undefined) of
        undefined -> ok;
        Account ->
            [catch arbiguard_exchange_account:sync_now(AccountID, ExchangeID)
             || ExchangeID <- maps:get(exchanges, Account, [])]
    end,
    {noreply, State};
handle_cast({report_funding_settlement, AccountID0, PositionID, Settlement}, State = #state{accounts = Accounts}) ->
    AccountID = norm_account_id(AccountID0),
    case maps:get(AccountID, Accounts, undefined) of
        undefined -> ok;
        Account -> maps:get(close_executor, Account) ! {live_funding_settlement, PositionID, Settlement}
    end,
    {noreply, State};
handle_cast({track_position, Req0, Position0}, State = #state{accounts = Accounts}) ->
    Req = normalize_req_account(Req0, Position0),
    AccountID = maps:get(account_id, Req),
    Position = Position0#{account_id => AccountID, account_mode => maps:get(account_mode, Req)},
    case maps:get(AccountID, Accounts, undefined) of
        undefined ->
            lager:warning("account manager drop track_position account_not_found account=~s symbol=~s",
                          [AccountID, maps:get(symbol, Position, <<"">>)]);
        Account ->
            gen_server:cast(maps:get(close_executor, Account), {track_position, with_account_meta(Req, Account), Position})
    end,
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

default_paper_account() ->
    #{id => <<"paper-main">>,
      mode => <<"paper">>,
      name => <<"默认模拟总账户">>,
      open_executor => arbiguard_open_executor,
      close_executor => arbiguard_close_executor,
      exchange_accounts => #{},
      exchanges => [],
      created_at => arbiguard_util:now_ms()}.

default_live_config() ->
    #{id => <<"live-main">>,
      mode => <<"live">>,
      name => <<"默认实盘总账户">>,
      exchanges => [],
      live_enabled => false}.

normalize_config(Config0) when is_map(Config0) ->
    Mode = norm_mode(maps:get(mode, Config0, <<"live">>)),
    DefaultID = <<Mode/binary, "-", (integer_to_binary(arbiguard_util:now_ms()))/binary>>,
    Tokens = normalize_exchange_tokens(map_get_any(exchange_tokens, Config0, #{})),
    Exchanges0 = [norm_exchange(E) || E <- map_get_any(exchanges, Config0, [])],
    Exchanges = lists:usort(Exchanges0 ++ maps:keys(Tokens)),
    #{id => norm_account_id(maps:get(id, Config0, DefaultID)),
      mode => Mode,
      name => arbiguard_util:to_binary(maps:get(name, Config0, <<"">>)),
      exchanges => Exchanges,
      live_enabled => truthy(map_get_any(live_enabled, Config0, false)),
      exchange_tokens => Tokens,
      raw => Config0};
normalize_config(_) ->
    normalize_config(#{}).

start_account_workers(Config) ->
    AccountID = maps:get(id, Config),
    Mode = maps:get(mode, Config),
    OpenName = arbiguard_open_executor:account_name(AccountID),
    CloseName = arbiguard_close_executor:account_name(AccountID),
    {ExchangeAccounts, ExchangeLog} = start_exchange_accounts(AccountID, maps:get(exchanges, Config, [])),
    apply_initial_exchange_tokens(AccountID, maps:get(exchange_tokens, Config, #{})),
    Meta = executor_meta(Config, ExchangeAccounts),
    OpenLog = ensure_open_executor(OpenName, AccountID, Mode, Meta),
    CloseLog = ensure_close_executor(CloseName, AccountID, Mode, Meta),
    {Config#{open_executor => OpenName,
             close_executor => CloseName,
             exchange_accounts => ExchangeAccounts,
             live_enabled => maps:get(live_enabled, Config, false),
             exchange_tokens => maps:get(exchange_tokens, Config, #{}),
             created_at => arbiguard_util:now_ms()},
     #{open_executor => OpenLog, close_executor => CloseLog, exchange_accounts => ExchangeLog}}.

ensure_open_executor(Name, AccountID, Mode, Meta) ->
    case whereis(Name) of
        undefined -> arbiguard_open_executor:start_link(Meta#{name => Name, account_id => AccountID, account_mode => Mode});
        Pid -> gen_server:cast(Pid, {account_meta, Meta}), {ok, Pid, already_started}
    end.

ensure_close_executor(Name, AccountID, Mode, Meta) ->
    case whereis(Name) of
        undefined -> arbiguard_close_executor:start_link(Meta#{name => Name, account_id => AccountID, account_mode => Mode});
        Pid -> gen_server:cast(Pid, {account_meta, Meta}), {ok, Pid, already_started}
    end.

start_exchange_accounts(AccountID, Exchanges) ->
    lists:foldl(fun(ExchangeID, {Acc, Log}) ->
        Name = arbiguard_exchange_account:name(AccountID, ExchangeID),
        ExchangeConfig = exchange_config(ExchangeID),
        Start = case whereis(Name) of
            undefined -> arbiguard_exchange_account:start_link(AccountID, ExchangeID, ExchangeConfig);
            Pid -> {ok, Pid, already_started}
        end,
        PrivateName = arbiguard_private_ws:name(AccountID, ExchangeID),
        PrivateStart = case whereis(PrivateName) of
            undefined -> arbiguard_private_ws:start_link(AccountID, ExchangeConfig#{id => ExchangeID});
            PrivatePid -> {ok, PrivatePid, already_started}
        end,
        {Acc#{ExchangeID => #{account_process => Name, private_ws => PrivateName}},
         Log#{ExchangeID => #{account_process => Start, private_ws => PrivateStart}}}
    end, {#{}, #{}}, Exchanges).

apply_initial_exchange_tokens(AccountID, Tokens) when is_map(Tokens) ->
    maps:foreach(fun(ExchangeID, Token) ->
        catch arbiguard_exchange_account:set_token(AccountID, ExchangeID, Token)
    end, Tokens),
    ok;
apply_initial_exchange_tokens(_AccountID, _Tokens) ->
    ok.

exchange_config(ExchangeID) ->
    Exchanges = application:get_env(arbiguard, exchanges, arbiguard_calc:default_exchanges()),
    case [E || E <- Exchanges, norm_exchange(maps:get(id, E, <<"">>)) =:= norm_exchange(ExchangeID)] of
        [Hit | _] -> Hit;
        [] -> #{id => norm_exchange(ExchangeID)}
    end.

ensure_account_exchange(AccountID, ExchangeID, Accounts) ->
    case maps:get(AccountID, Accounts, undefined) of
        undefined ->
            {error, <<"account_not_found">>};
        Account ->
            ExchangeAccounts = maps:get(exchange_accounts, Account, #{}),
            case maps:is_key(ExchangeID, ExchangeAccounts) of
                true ->
                    {ok, Account, Accounts};
                false ->
                    {NewExchangeAccounts, _Log} = start_exchange_accounts(AccountID, [ExchangeID]),
                    Account1 = Account#{exchange_accounts => maps:merge(ExchangeAccounts, NewExchangeAccounts),
                                        exchanges => lists:usort([ExchangeID | maps:get(exchanges, Account, [])])},
                    {ok, Account1, Accounts#{AccountID => Account1}}
            end
    end.

add_exchange_to_account(Account, ExchangeID) ->
    Account#{exchanges => lists:usort([ExchangeID | maps:get(exchanges, Account, [])])}.

executor_meta(AccountOrConfig, ExchangeAccounts) ->
    #{account_id => maps:get(id, AccountOrConfig, <<"paper-main">>),
      account_mode => maps:get(mode, AccountOrConfig, <<"paper">>),
      live_enabled => maps:get(live_enabled, AccountOrConfig, false),
      exchange_accounts => ExchangeAccounts,
      exchange_tokens => maps:get(exchange_tokens, AccountOrConfig, #{})}.

with_account_meta(Req, Account) ->
    Meta = executor_meta(Account, maps:get(exchange_accounts, Account, #{})),
    maps:merge(Req, Meta).

broadcast_executor_meta(Account) ->
    Meta = executor_meta(Account, maps:get(exchange_accounts, Account, #{})),
    cast_executor_meta(maps:get(open_executor, Account, undefined), Meta),
    cast_executor_meta(maps:get(close_executor, Account, undefined), Meta),
    ok.

cast_executor_meta(Name, Meta) when is_atom(Name) ->
    case whereis(Name) of
        undefined -> ok;
        Pid -> gen_server:cast(Pid, {account_meta, Meta})
    end;
cast_executor_meta(_Name, _Meta) ->
    ok.

normalize_req_account(Req0, Position) ->
    Req = arbiguard_calc:normalize_request(Req0),
    Mode = norm_mode(maps:get(account_mode, Req, maps:get(account_mode, Position, <<"paper">>))),
    DefaultID = case Mode of <<"live">> -> <<"live-main">>; _ -> <<"paper-main">> end,
    Req#{account_mode => Mode,
         account_id => norm_account_id(maps:get(account_id, Req, maps:get(account_id, Position, DefaultID)))}.

public_account(Account) ->
    Public = Account#{open_executor_pid => pid_of(maps:get(open_executor, Account, undefined)),
                      close_executor_pid => pid_of(maps:get(close_executor, Account, undefined)),
                      token_configured_exchanges => maps:keys(maps:get(exchange_tokens, Account, #{})),
                      exchange_snapshot_count => maps:size(maps:get(exchange_snapshots, Account, #{}))},
    maps:without([raw, exchange_tokens], Public).

sync_live_pairs_to_close_executor(Account) ->
    case maps:get(mode, Account, <<"paper">>) of
        <<"live">> ->
            Req = live_track_req(Account),
            [gen_server:cast(maps:get(close_executor, Account), {track_position, Req, Position})
             || Position <- paired_live_positions(Account)];
        _ ->
            ok
    end.

live_track_req(Account) ->
    DefaultScan = application:get_env(arbiguard, default_scan, #{}),
    arbiguard_calc:normalize_request(DefaultScan#{
        account_mode => <<"live">>,
        account_id => maps:get(id, Account, <<"live-main">>),
        live_enabled => maps:get(live_enabled, Account, false),
        exchange_tokens => maps:get(exchange_tokens, Account, #{}),
        exchange_accounts => maps:get(exchange_accounts, Account, #{})
    }).

paired_live_positions(Account) ->
    Positions = live_positions(Account),
    BySymbol = group_by_symbol(Positions),
    lists:append([pair_symbol_positions(Symbol, Ps, Account) || {Symbol, Ps} <- maps:to_list(BySymbol)]).

live_positions(Account) ->
    Snapshots = maps:get(exchange_snapshots, Account, #{}),
    lists:append([maps:get(positions, Snapshot, []) || {_Exchange, Snapshot} <- maps:to_list(Snapshots)]).

group_by_symbol(Positions) ->
    lists:foldl(fun(Position, Acc) ->
        Symbol = maps:get(symbol, Position, <<"">>),
        case Symbol of
            <<"">> -> Acc;
            _ -> Acc#{Symbol => [Position | maps:get(Symbol, Acc, [])]}
        end
    end, #{}, Positions).

pair_symbol_positions(Symbol, Positions, Account) ->
    Longs = [P || P <- Positions, position_side(P) =:= <<"long">>, position_notional(P) > 0],
    Shorts = [P || P <- Positions, position_side(P) =:= <<"short">>, position_notional(P) > 0],
    pair_position_legs(Symbol, Longs, Shorts, Account).

pair_position_legs(_Symbol, [], _Shorts, _Account) -> [];
pair_position_legs(_Symbol, _Longs, [], _Account) -> [];
pair_position_legs(Symbol, [Long | RestLongs], Shorts, Account) ->
    case take_first_different_exchange(Long, Shorts) of
        {ok, Short, Shorts1} ->
            [position_pair(Symbol, Long, Short, Account) | pair_position_legs(Symbol, RestLongs, Shorts1, Account)];
        none ->
            pair_position_legs(Symbol, RestLongs, Shorts, Account)
    end.

take_first_different_exchange(Long, Shorts) ->
    LongEx = maps:get(exchange, Long, <<"">>),
    take_first_different_exchange(LongEx, Shorts, []).

take_first_different_exchange(_LongEx, [], _Seen) ->
    none;
take_first_different_exchange(LongEx, [Short | Rest], Seen) ->
    case maps:get(exchange, Short, <<"">>) =/= LongEx of
        true -> {ok, Short, lists:reverse(Seen) ++ Rest};
        false -> take_first_different_exchange(LongEx, Rest, [Short | Seen])
    end.

position_pair(Symbol, Long, Short, Account) ->
    LongEx = maps:get(exchange, Long, <<"">>),
    ShortEx = maps:get(exchange, Short, <<"">>),
    LongQty = position_qty(Long),
    ShortQty = position_qty(Short),
    LongEntry = entry_price(Long),
    ShortEntry = entry_price(Short),
    Notional = min(position_notional(Long), position_notional(Short)),
    ID = <<(maps:get(id, Account, <<"live-main">>))/binary, "|live|", Symbol/binary, "|", LongEx/binary, "|", ShortEx/binary>>,
    #{id => ID,
      position_id => ID,
      account_mode => <<"live">>,
      account_id => maps:get(id, Account, <<"live-main">>),
      symbol => Symbol,
      long_exchange => LongEx,
      short_exchange => ShortEx,
      long_entry_price => LongEntry,
      short_entry_price => ShortEntry,
      long_qty => LongQty,
      short_qty => ShortQty,
      notional => Notional,
      notional_usdt => Notional,
      leverage => max(position_leverage(Long), position_leverage(Short)),
      funding_pnl => f(maps:get(funding_pnl, Long, 0), 0) + f(maps:get(funding_pnl, Short, 0), 0),
      opened_at => min_positive_time(maps:get(opened_at, Long, 0), maps:get(opened_at, Short, 0)),
      source => <<"live_account_bootstrap">>}.

position_side(Position) ->
    Side0 = string:lowercase(arbiguard_util:to_binary(
        maps:get(side, Position, maps:get(position_side, Position, <<"">>)))),
    case Side0 of
        <<"short">> -> <<"short">>;
        <<"sell">> -> <<"short">>;
        _ ->
            case f(maps:get(qty, Position, maps:get(position_amt, Position, maps:get(size, Position, 0))), 0) < 0 of
                true -> <<"short">>;
                false -> <<"long">>
            end
    end.

position_qty(Position) ->
    abs(f(maps:get(qty, Position, maps:get(position_amt, Position, maps:get(size, Position, 0))), 0)).

entry_price(Position) ->
    f(maps:get(entry_price, Position, maps:get(avg_entry_price, Position, maps:get(avgPx, Position, 0))), 0).

position_notional(Position) ->
    Explicit = f(maps:get(notional, Position, maps:get(notional_usdt, Position, 0)), 0),
    case Explicit > 0 of
        true -> Explicit;
        false -> position_qty(Position) * entry_price(Position)
    end.

position_leverage(Position) ->
    max(1.0, f(maps:get(leverage, Position, 1), 1)).

min_positive_time(A0, B0) ->
    A = arbiguard_util:to_int(A0, 0),
    B = arbiguard_util:to_int(B0, 0),
    case {A > 0, B > 0} of
        {true, true} -> min(A, B);
        {true, false} -> A;
        {false, true} -> B;
        _ -> arbiguard_util:now_ms()
    end.

f(V, D) ->
    arbiguard_util:to_float(V, D).

pid_of(Name) when is_atom(Name) ->
    case whereis(Name) of undefined -> undefined; Pid -> list_to_binary(pid_to_list(Pid)) end;
pid_of(_) -> undefined.

norm_mode(live) -> <<"live">>;
norm_mode(paper) -> <<"paper">>;
norm_mode(V) ->
    case string:lowercase(arbiguard_util:to_binary(V)) of
        <<"live">> -> <<"live">>;
        _ -> <<"paper">>
    end.

norm_exchange(V) ->
    string:lowercase(arbiguard_util:to_binary(V)).

norm_account_id(V) ->
    case arbiguard_util:to_binary(V) of
        <<"">> -> <<"paper-main">>;
        B -> B
    end.

truthy(true) -> true;
truthy(<<"true">>) -> true;
truthy(1) -> true;
truthy(_) -> false.

ensure_map(Map) when is_map(Map) -> Map;
ensure_map(_) -> #{}.

normalize_exchange_tokens(Tokens0) when is_map(Tokens0) ->
    maps:fold(fun(ExchangeID0, Token0, Acc) ->
        ExchangeID = norm_exchange(ExchangeID0),
        Token = normalize_exchange_token(Token0),
        case Token of
            #{} -> Acc;
            _ -> Acc#{ExchangeID => Token}
        end
    end, #{}, Tokens0);
normalize_exchange_tokens(_) ->
    #{}.

normalize_exchange_token(Token0) ->
    maps:without([account_id, <<"account_id">>, exchange, <<"exchange">>], ensure_map(Token0)).

map_get_any(Key, Map, Default) when is_atom(Key), is_map(Map) ->
    maps:get(Key, Map, maps:get(atom_to_binary(Key, utf8), Map, Default));
map_get_any(Key, Map, Default) when is_binary(Key), is_map(Map) ->
    AtomValue = try maps:get(binary_to_existing_atom(Key, utf8), Map, Default)
                catch _:_ -> Default
                end,
    maps:get(Key, Map, AtomValue);
map_get_any(_Key, _Map, Default) ->
    Default.

normalize_test_payload(Payload0) ->
    Payload = ensure_map(Payload0),
    Payload#{
        account_id => norm_account_id(maps:get(account_id, Payload, <<"live-main">>)),
        action => string:lowercase(arbiguard_util:to_binary(maps:get(action, Payload, <<"open">>))),
        exchange => norm_exchange(maps:get(exchange, Payload, <<"binance">>)),
        symbol => string:uppercase(arbiguard_util:to_binary(maps:get(symbol, Payload, <<"BTCUSDT">>))),
        side => string:lowercase(arbiguard_util:to_binary(maps:get(side, Payload, <<"long">>))),
        order_type => string:uppercase(arbiguard_util:to_binary(maps:get(order_type, Payload, <<"LIMIT">>))),
        notional => arbiguard_util:to_float(maps:get(notional, Payload, 0), 0),
        quantity => arbiguard_util:to_float(maps:get(quantity, Payload, 0), 0),
        price => arbiguard_util:to_float(maps:get(price, Payload, 0), 0),
        leverage => max(1.0, arbiguard_util:to_float(maps:get(leverage, Payload, 1), 1)),
        reduce_only => truthy(maps:get(reduce_only, Payload, false)),
        client_order_id => arbiguard_util:to_binary(maps:get(client_order_id, Payload, <<"">>)),
        confirm => arbiguard_util:to_binary(maps:get(confirm, Payload, <<"">>))
    }.
