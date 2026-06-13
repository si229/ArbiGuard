-module(arbiguard_live_account).
-behaviour(gen_server).

-export([start_link/0, snapshot/0, set_exchange_token/2, get_exchange_token/1,
         submit_order/2, set_enabled/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {enabled = false, tokens = #{}, orders = #{}, logs = []}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

snapshot() ->
    gen_server:call(?MODULE, snapshot).

set_enabled(Enabled) ->
    gen_server:call(?MODULE, {set_enabled, Enabled}).

set_exchange_token(ExchangeID, TokenConfig) ->
    gen_server:call(?MODULE, {set_exchange_token, ExchangeID, TokenConfig}).

get_exchange_token(ExchangeID) ->
    gen_server:call(?MODULE, {get_exchange_token, ExchangeID}).

submit_order(Req, Order) ->
    gen_server:call(?MODULE, {submit_order, Req, Order}).

init([]) ->
    {ok, #state{}}.

handle_call(snapshot, _From, State = #state{enabled = Enabled, tokens = Tokens, orders = Orders, logs = Logs}) ->
    {reply, #{enabled => Enabled,
              token_exchanges => maps:keys(Tokens),
              orders => maps:values(Orders),
              logs => Logs}, State};
handle_call({set_enabled, Enabled0}, _From, State) ->
    Enabled = truthy(Enabled0),
    lager:warning("live account enabled=~p", [Enabled]),
    {reply, #{enabled => Enabled}, State#state{enabled = Enabled}};
handle_call({set_exchange_token, ExchangeID0, TokenConfig}, _From, State = #state{tokens = Tokens}) ->
    ExchangeID = norm_exchange(ExchangeID0),
    lager:info("live token configured exchange=~s", [ExchangeID]),
    {reply, ok, State#state{tokens = Tokens#{ExchangeID => TokenConfig}}};
handle_call({get_exchange_token, ExchangeID0}, _From, State = #state{tokens = Tokens}) ->
    ExchangeID = norm_exchange(ExchangeID0),
    {reply, maps:get(ExchangeID, Tokens, undefined), State};
handle_call({submit_order, Req, Order0}, _From, State = #state{enabled = Enabled, orders = Orders, logs = Logs}) ->
    Now = arbiguard_util:now_ms(),
    AccountID = arbiguard_util:to_binary(maps:get(account_id, Order0, maps:get(account_id, Req, <<"live-main">>))),
    Order = Order0#{submitted_at => Now, mode => live, account_mode => <<"live">>, account_id => AccountID},
    ID = maps:get(id, Order, order_id(Order)),
    Result =
        case Enabled of
            true ->
                %% Real exchange adapters must replace this branch with signed order requests.
                %% Until then, live orders are accepted into the live-account queue only.
                Order#{id => ID, status => <<"pending_adapter">>, reason => <<"live_exchange_adapter_not_implemented">>};
            false ->
                Order#{id => ID, status => <<"rejected">>, reason => <<"live_account_disabled">>}
        end,
    Log = #{time => Now,
            action => <<"live_order_request">>,
            account_id => AccountID,
            id => ID,
            status => maps:get(status, Result),
            reason => maps:get(reason, Result),
            req => Req},
    lager:warning("live order request id=~s status=~s reason=~s",
                  [ID, maps:get(status, Result), maps:get(reason, Result)]),
    {reply, Result, State#state{orders = Orders#{ID => Result}, logs = lists:sublist([Log | Logs], 500)}};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

norm_exchange(V) ->
    string:lowercase(arbiguard_util:to_binary(V)).

truthy(true) -> true;
truthy(<<"true">>) -> true;
truthy(1) -> true;
truthy(_) -> false.

order_id(Order) ->
    Symbol = maps:get(symbol, Order, <<"UNKNOWN">>),
    Long = maps:get(long_exchange, Order, <<"long">>),
    Short = maps:get(short_exchange, Order, <<"short">>),
    <<Symbol/binary, "|", Long/binary, "|", Short/binary, "|", (integer_to_binary(arbiguard_util:now_ms()))/binary>>.
