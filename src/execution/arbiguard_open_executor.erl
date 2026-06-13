-module(arbiguard_open_executor).
-behaviour(gen_server).

-export([start_link/0, notify_opportunities/2, submit_order/2, snapshot/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {orders = #{}, last_opportunities = [], last_notify = 0}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

notify_opportunities(Req, Result) ->
    gen_server:cast(?MODULE, {opportunities, Req, Result}).

submit_order(Req, Opportunity) ->
    gen_server:call(?MODULE, {submit_order, Req, Opportunity}).

snapshot() ->
    gen_server:call(?MODULE, snapshot).

init([]) ->
    {ok, #state{}}.

handle_call({submit_order, Req, Opportunity}, _From, State) ->
    {Order, NewState} = create_order(Req, Opportunity, State),
    {reply, Order, NewState};
handle_call(snapshot, _From, State) ->
    {reply, #{orders => maps:values(State#state.orders),
              last_opportunities => State#state.last_opportunities,
              last_notify => State#state.last_notify}, State};
handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({opportunities, Req, Result}, State) ->
    Opportunities = maps:get(opportunities, Result, []),
    NewState = lists:foldl(fun(Op, Acc) -> maybe_create_execution_order(Req, Op, Acc) end, State, Opportunities),
    {noreply, NewState#state{last_opportunities = Opportunities, last_notify = arbiguard_util:now_ms()}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_create_execution_order(Req, Op, State = #state{orders = Orders}) ->
    MinProfit = maps:get(min_execution_profit_usdt, arbiguard_calc:normalize_request(Req), 5.0),
    ID = order_key(Op),
    case maps:get(estimated_net_profit, Op, 0) >= MinProfit andalso not maps:is_key(ID, Orders) of
        true ->
            Order = order_plan(Req, Op),
            subscribe_order_symbols(Op),
            FilledOrder = dispatch_order(Req, Order, Op),
            State#state{orders = Orders#{ID => FilledOrder}};
        false ->
            State
    end.

create_order(Req, Op, State = #state{orders = Orders}) ->
    Order = order_plan(Req, Op),
    subscribe_order_symbols(Op),
    {Order, State#state{orders = Orders#{maps:get(id, Order) => Order}}}.

order_plan(Req0, Op) ->
    Req = arbiguard_calc:normalize_request(Req0),
    Notional = maps:get(suggested_notional, Op, maps:get(execution_notional_usdt, Req, 200.0)),
    LongPrice = maps:get(long_price, Op, 0.0),
    ShortPrice = maps:get(short_price, Op, 0.0),
    #{id => order_key(Op),
      status => <<"planned_open">>,
      symbol => maps:get(symbol, Op),
      long_exchange => maps:get(long_exchange, Op),
      short_exchange => maps:get(short_exchange, Op),
      target_notional => Notional,
      long_target_qty => safe_div(Notional, LongPrice),
      short_target_qty => safe_div(Notional, ShortPrice),
      mode => maps:get(execution_order_mode, Req, <<"fok">>),
      expected_net_profit => maps:get(estimated_net_profit, Op, 0.0),
      created_at => arbiguard_util:now_ms()}.

subscribe_order_symbols(Op) ->
    Symbol = maps:get(symbol, Op),
    LongEx = maps:get(long_exchange, Op),
    ShortEx = maps:get(short_exchange, Op),
    catch arbiguard_exchange_ticker:subscribe(LongEx, Symbol, open_execution_order),
    catch arbiguard_exchange_ticker:subscribe(ShortEx, Symbol, open_execution_order),
    ok.

dispatch_order(Req0, Order, Op) ->
    Req = arbiguard_calc:normalize_request(Req0),
    case maps:get(account_mode, Req, <<"paper">>) of
        <<"live">> ->
            _ = catch arbiguard_live_account:submit_order(Req, Order),
            Order#{status => <<"submitted_live_open">>, submitted_at => arbiguard_util:now_ms()};
        live ->
            _ = catch arbiguard_live_account:submit_order(Req, Order),
            Order#{status => <<"submitted_live_open">>, submitted_at => arbiguard_util:now_ms()};
        _ ->
            _ = catch arbiguard_state:apply_open_order(Req, Order, Op),
            Order#{status => <<"filled_paper_open">>, filled_at => arbiguard_util:now_ms()}
    end.

order_key(Op) ->
    <<(maps:get(symbol, Op))/binary, "|", (maps:get(long_exchange, Op))/binary, "|", (maps:get(short_exchange, Op))/binary>>.

safe_div(_A, B) when B =< 0 -> 0.0;
safe_div(A, B) -> A / B.
