-module(arbiguard_exchange_account).
-behaviour(gen_server).

-export([start_link/3, name/2, snapshot/2, set_token/3, get_token/2,
         report_order_event/3, report_balance/3, report_position/3,
         report_liquidation/3, report_funding_settlement/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {account_id, exchange_id, config = #{}, token = undefined,
                balances = #{}, positions = #{}, orders = #{}, liquidations = [],
                logs = []}).

start_link(AccountID, ExchangeID, Config) ->
    gen_server:start_link({local, name(AccountID, ExchangeID)}, ?MODULE,
                          [AccountID, ExchangeID, Config], []).

name(AccountID, ExchangeID) ->
    list_to_atom("arbiguard_exchange_account_" ++ safe_atom_part(AccountID) ++ "_" ++ safe_atom_part(ExchangeID)).

snapshot(AccountID, ExchangeID) ->
    gen_server:call(name(AccountID, ExchangeID), snapshot).

set_token(AccountID, ExchangeID, Token) ->
    gen_server:call(name(AccountID, ExchangeID), {set_token, Token}).

get_token(AccountID, ExchangeID) ->
    case whereis(name(AccountID, ExchangeID)) of
        undefined -> undefined;
        _ -> gen_server:call(name(AccountID, ExchangeID), get_token)
    end.

report_order_event(AccountID, ExchangeID, Event) ->
    safe_call(AccountID, ExchangeID, {report_order_event, Event}).

report_balance(AccountID, ExchangeID, Balance) ->
    safe_call(AccountID, ExchangeID, {report_balance, Balance}).

report_position(AccountID, ExchangeID, Position) ->
    safe_call(AccountID, ExchangeID, {report_position, Position}).

report_liquidation(AccountID, ExchangeID, Event) ->
    safe_call(AccountID, ExchangeID, {report_liquidation, Event}).

report_funding_settlement(AccountID, ExchangeID, Event) ->
    safe_call(AccountID, ExchangeID, {report_funding_settlement, Event}).

init([AccountID0, ExchangeID0, Config]) ->
    {ok, #state{account_id = norm_account(AccountID0),
                exchange_id = norm_exchange(ExchangeID0),
                config = Config}}.

handle_call(snapshot, _From, State) ->
    {reply, public_state(State), State};
handle_call({set_token, Token}, _From, State) ->
    lager:info("exchange account token configured account=~s exchange=~s",
               [State#state.account_id, State#state.exchange_id]),
    {reply, ok, State#state{token = Token}};
handle_call(get_token, _From, State) ->
    {reply, State#state.token, State};
handle_call({report_order_event, Event0}, _From, State = #state{orders = Orders, logs = Logs}) ->
    Event = Event0#{account_id => State#state.account_id, exchange => State#state.exchange_id},
    OrderID = maps:get(order_id, Event, maps:get(id, Event, <<"">>)),
    Orders1 = case OrderID of <<"">> -> Orders; _ -> Orders#{OrderID => Event} end,
    arbiguard_account_manager:report_order_event(State#state.account_id, State#state.exchange_id, Event),
    {reply, #{ok => true, event => Event},
     State#state{orders = Orders1, logs = add_log(<<"order_event">>, Event, Logs)}};
handle_call({report_balance, Balance0}, _From, State = #state{balances = Balances, logs = Logs}) ->
    Balance = Balance0#{account_id => State#state.account_id, exchange => State#state.exchange_id,
                        updated_at => arbiguard_util:now_ms()},
    Asset = maps:get(asset, Balance, <<"USDT">>),
    {reply, #{ok => true, balance => Balance},
     State#state{balances = Balances#{Asset => Balance}, logs = add_log(<<"balance">>, Balance, Logs)}};
handle_call({report_position, Position0}, _From, State = #state{positions = Positions, logs = Logs}) ->
    Position = Position0#{account_id => State#state.account_id, exchange => State#state.exchange_id,
                          updated_at => arbiguard_util:now_ms()},
    Key = position_key(Position),
    {reply, #{ok => true, position => Position},
     State#state{positions = Positions#{Key => Position}, logs = add_log(<<"position">>, Position, Logs)}};
handle_call({report_liquidation, Event0}, _From, State = #state{liquidations = Liquidations, logs = Logs}) ->
    Event = Event0#{account_id => State#state.account_id, exchange => State#state.exchange_id,
                    time => arbiguard_util:now_ms()},
    arbiguard_account_manager:report_order_event(State#state.account_id, State#state.exchange_id,
                                                 Event#{event_type => liquidation}),
    {reply, #{ok => true, event => Event},
     State#state{liquidations = lists:sublist([Event | Liquidations], 100),
                 logs = add_log(<<"liquidation">>, Event, Logs)}};
handle_call({report_funding_settlement, Event0}, _From, State = #state{logs = Logs}) ->
    Event = Event0#{account_id => State#state.account_id, exchange => State#state.exchange_id},
    PositionID = maps:get(position_id, Event, <<"">>),
    arbiguard_account_manager:report_funding_settlement(State#state.account_id, PositionID, Event),
    {reply, #{ok => true, event => Event}, State#state{logs = add_log(<<"funding">>, Event, Logs)}};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

safe_call(AccountID, ExchangeID, Msg) ->
    Name = name(AccountID, ExchangeID),
    case whereis(Name) of
        undefined -> {error, exchange_account_not_found};
        _ -> gen_server:call(Name, Msg)
    end.

public_state(State) ->
    #{account_id => State#state.account_id,
      exchange => State#state.exchange_id,
      token_configured => State#state.token =/= undefined,
      balances => State#state.balances,
      positions => maps:values(State#state.positions),
      orders => maps:values(State#state.orders),
      liquidations => State#state.liquidations,
      logs => State#state.logs}.

position_key(Position) ->
    <<(maps:get(symbol, Position, <<"">>))/binary, "|",
      (maps:get(side, Position, <<"">>))/binary>>.

add_log(Action, Data, Logs) ->
    lists:sublist([#{time => arbiguard_util:now_ms(), action => Action,
                     data => maps:without([raw], Data)} | Logs], 300).

norm_account(V) -> arbiguard_util:to_binary(V).
norm_exchange(V) -> string:lowercase(arbiguard_util:to_binary(V)).

safe_atom_part(V) ->
    S = binary_to_list(string:lowercase(arbiguard_util:to_binary(V))),
    [case ((C >= $a andalso C =< $z) orelse (C >= $0 andalso C =< $9)) of true -> C; false -> $_ end || C <- S].
