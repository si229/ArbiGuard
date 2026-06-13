-module(arbiguard_close_executor).
-behaviour(gen_server).

-export([start_link/0, submit_close/2, snapshot/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {orders = #{}, last_submit = 0}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

submit_close(Req, Position) ->
    gen_server:call(?MODULE, {submit_close, Req, Position}).

snapshot() ->
    gen_server:call(?MODULE, snapshot).

init([]) ->
    {ok, #state{}}.

handle_call({submit_close, Req, Position}, _From, State = #state{orders = Orders}) ->
    Order = close_plan(Req, Position),
    subscribe_position_symbols(Position),
    NewOrder = dispatch_close(Req, Order),
    {reply, NewOrder, State#state{orders = Orders#{maps:get(id, NewOrder) => NewOrder},
                                  last_submit = arbiguard_util:now_ms()}};
handle_call(snapshot, _From, State) ->
    {reply, #{orders => maps:values(State#state.orders), last_submit => State#state.last_submit}, State};
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

close_plan(Req0, Position) ->
    Req = arbiguard_calc:normalize_request(Req0),
    Symbol = maps:get(symbol, Position, <<"">>),
    LongEx = maps:get(long_exchange, Position, <<"">>),
    ShortEx = maps:get(short_exchange, Position, <<"">>),
    #{id => <<Symbol/binary, "|", LongEx/binary, "|", ShortEx/binary, "|close">>,
      status => <<"planned_close">>,
      symbol => Symbol,
      long_exchange => LongEx,
      short_exchange => ShortEx,
      target_notional => maps:get(notional, Position, maps:get(notional_usdt, Position, 0.0)),
      mode => maps:get(close_order_mode, Req, <<"ioc">>),
      created_at => arbiguard_util:now_ms()}.

subscribe_position_symbols(Position) ->
    Symbol = maps:get(symbol, Position, <<"">>),
    LongEx = maps:get(long_exchange, Position, <<"">>),
    ShortEx = maps:get(short_exchange, Position, <<"">>),
    catch arbiguard_exchange_ticker:subscribe(LongEx, Symbol, close_execution_order),
    catch arbiguard_exchange_ticker:subscribe(ShortEx, Symbol, close_execution_order),
    ok.

dispatch_close(Req0, Order) ->
    Req = arbiguard_calc:normalize_request(Req0),
    case maps:get(account_mode, Req, <<"paper">>) of
        <<"live">> ->
            _ = catch arbiguard_live_account:submit_order(Req, Order),
            Order#{status => <<"submitted_live_close">>, submitted_at => arbiguard_util:now_ms()};
        live ->
            _ = catch arbiguard_live_account:submit_order(Req, Order),
            Order#{status => <<"submitted_live_close">>, submitted_at => arbiguard_util:now_ms()};
        _ ->
            Order#{status => <<"queued_paper_close">>, queued_at => arbiguard_util:now_ms()}
    end.
