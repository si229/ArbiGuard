-module(arbiguard_account_manager).
-behaviour(gen_server).

-export([start_link/0, snapshot/0, create_account/1, account/1,
         open_executor/1, close_executor/1, notify_opportunities/2,
         submit_order/2, track_position/2, report_order_event/3,
         report_funding_settlement/3]).
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

notify_opportunities(Req, Result) ->
    gen_server:cast(?MODULE, {notify_opportunities, Req, Result}).

submit_order(Req, Opportunity) ->
    gen_server:call(?MODULE, {submit_order, Req, Opportunity}).

track_position(Req, Position) ->
    gen_server:call(?MODULE, {track_position, Req, Position}).

report_order_event(AccountID, ExchangeID, Event) ->
    gen_server:cast(?MODULE, {report_order_event, AccountID, ExchangeID, Event}).

report_funding_settlement(AccountID, PositionID, Settlement) ->
    gen_server:cast(?MODULE, {report_funding_settlement, AccountID, PositionID, Settlement}).

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
handle_call({submit_order, Req0, Opportunity}, _From, State = #state{accounts = Accounts}) ->
    Req = normalize_req_account(Req0),
    AccountID = maps:get(account_id, Req),
    case maps:get(AccountID, Accounts, undefined) of
        undefined ->
            {reply, #{ok => false, reason => <<"account_not_found">>, account_id => AccountID}, State};
        Account ->
            Reply = arbiguard_open_executor:submit_order(maps:get(open_executor, Account), Req, Opportunity),
            {reply, Reply, State}
    end;
handle_call({track_position, Req0, Position0}, _From, State = #state{accounts = Accounts}) ->
    Req = normalize_req_account(Req0, Position0),
    AccountID = maps:get(account_id, Req),
    Position = Position0#{account_id => AccountID, account_mode => maps:get(account_mode, Req)},
    case maps:get(AccountID, Accounts, undefined) of
        undefined ->
            {reply, #{ok => false, reason => <<"account_not_found">>, account_id => AccountID}, State};
        Account ->
            Reply = arbiguard_close_executor:track_position(maps:get(close_executor, Account), Req, Position),
            {reply, Reply, State}
    end;
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({notify_opportunities, Req0, Result}, State = #state{accounts = Accounts}) ->
    Req = normalize_req_account(Req0),
    AccountID = maps:get(account_id, Req),
    case maps:get(AccountID, Accounts, undefined) of
        undefined ->
            lager:warning("account manager drop opportunities account_not_found account=~s", [AccountID]);
        Account ->
            arbiguard_open_executor:notify_opportunities(maps:get(open_executor, Account), Req, Result)
    end,
    {noreply, State};
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
handle_cast({report_funding_settlement, AccountID0, PositionID, Settlement}, State = #state{accounts = Accounts}) ->
    AccountID = norm_account_id(AccountID0),
    case maps:get(AccountID, Accounts, undefined) of
        undefined -> ok;
        Account -> maps:get(close_executor, Account) ! {live_funding_settlement, PositionID, Settlement}
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
      exchanges => []}.

normalize_config(Config0) when is_map(Config0) ->
    Mode = norm_mode(maps:get(mode, Config0, <<"live">>)),
    DefaultID = <<Mode/binary, "-", (integer_to_binary(arbiguard_util:now_ms()))/binary>>,
    Exchanges = [norm_exchange(E) || E <- maps:get(exchanges, Config0, [])],
    #{id => norm_account_id(maps:get(id, Config0, DefaultID)),
      mode => Mode,
      name => arbiguard_util:to_binary(maps:get(name, Config0, <<"">>)),
      exchanges => Exchanges,
      raw => Config0};
normalize_config(_) ->
    normalize_config(#{}).

start_account_workers(Config) ->
    AccountID = maps:get(id, Config),
    Mode = maps:get(mode, Config),
    OpenName = arbiguard_open_executor:account_name(AccountID),
    CloseName = arbiguard_close_executor:account_name(AccountID),
    OpenLog = ensure_open_executor(OpenName, AccountID, Mode),
    CloseLog = ensure_close_executor(CloseName, AccountID, Mode),
    {ExchangeAccounts, ExchangeLog} = start_exchange_accounts(AccountID, maps:get(exchanges, Config, [])),
    {Config#{open_executor => OpenName,
             close_executor => CloseName,
             exchange_accounts => ExchangeAccounts,
             created_at => arbiguard_util:now_ms()},
     #{open_executor => OpenLog, close_executor => CloseLog, exchange_accounts => ExchangeLog}}.

ensure_open_executor(Name, AccountID, Mode) ->
    case whereis(Name) of
        undefined -> arbiguard_open_executor:start_link(#{name => Name, account_id => AccountID, account_mode => Mode});
        Pid -> {ok, Pid, already_started}
    end.

ensure_close_executor(Name, AccountID, Mode) ->
    case whereis(Name) of
        undefined -> arbiguard_close_executor:start_link(#{name => Name, account_id => AccountID, account_mode => Mode});
        Pid -> {ok, Pid, already_started}
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

exchange_config(ExchangeID) ->
    Exchanges = application:get_env(arbiguard, exchanges, arbiguard_calc:default_exchanges()),
    case [E || E <- Exchanges, norm_exchange(maps:get(id, E, <<"">>)) =:= norm_exchange(ExchangeID)] of
        [Hit | _] -> Hit;
        [] -> #{id => norm_exchange(ExchangeID)}
    end.

normalize_req_account(Req) ->
    normalize_req_account(Req, #{}).

normalize_req_account(Req0, Position) ->
    Req = arbiguard_calc:normalize_request(Req0),
    Mode = norm_mode(maps:get(account_mode, Req, maps:get(account_mode, Position, <<"paper">>))),
    DefaultID = case Mode of <<"live">> -> <<"live-main">>; _ -> <<"paper-main">> end,
    Req#{account_mode => Mode,
         account_id => norm_account_id(maps:get(account_id, Req, maps:get(account_id, Position, DefaultID)))}.

public_account(Account) ->
    maps:without([raw], Account#{open_executor_pid => pid_of(maps:get(open_executor, Account, undefined)),
                                 close_executor_pid => pid_of(maps:get(close_executor, Account, undefined))}).

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
