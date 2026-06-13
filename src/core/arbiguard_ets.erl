-module(arbiguard_ets).
-behaviour(gen_server).

-export([start_link/0]).
-export([put_ticker/1, get_ticker/2, all_tickers/0,
         put_funding/1, get_funding/2, all_funding/0,
         put_opportunities/1, all_opportunities/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(TICKER, arbiguard_ticker_ets).
-define(FUNDING, arbiguard_funding_ets).
-define(OPPORTUNITY, arbiguard_opportunity_ets).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    new_table(?TICKER),
    new_table(?FUNDING),
    new_table(?OPPORTUNITY),
    {ok, #{}}.

put_ticker(Row) ->
    Key = key(Row),
    true = ets:insert(?TICKER, {Key, Row}),
    ok.

get_ticker(Exchange, Symbol) ->
    lookup(?TICKER, {norm_exchange(Exchange), norm_symbol(Symbol)}).

all_tickers() ->
    [Row || {_Key, Row} <- ets:tab2list(?TICKER)].

put_funding(Row) ->
    Key = key(Row),
    true = ets:insert(?FUNDING, {Key, Row}),
    ok.

get_funding(Exchange, Symbol) ->
    lookup(?FUNDING, {norm_exchange(Exchange), norm_symbol(Symbol)}).

all_funding() ->
    [Row || {_Key, Row} <- ets:tab2list(?FUNDING)].

put_opportunities(Ops) ->
    ets:delete_all_objects(?OPPORTUNITY),
    [ets:insert(?OPPORTUNITY, {opportunity_key(Op), Op}) || Op <- Ops],
    ok.

all_opportunities() ->
    [Row || {_Key, Row} <- ets:tab2list(?OPPORTUNITY)].

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

new_table(Name) ->
    case ets:info(Name) of
        undefined -> ets:new(Name, [named_table, public, set, {read_concurrency, true}]);
        _ -> Name
    end.

lookup(Table, Key) ->
    case ets:lookup(Table, Key) of
        [{_, Row}] -> {ok, Row};
        [] -> not_found
    end.

key(Row) ->
    {norm_exchange(maps:get(exchange, Row, <<"">>)), norm_symbol(maps:get(symbol, Row, <<"">>))}.

opportunity_key(Op) ->
    {norm_symbol(maps:get(symbol, Op, <<"">>)),
     norm_exchange(maps:get(long_exchange, Op, <<"">>)),
     norm_exchange(maps:get(short_exchange, Op, <<"">>))}.

norm_exchange(V) ->
    string:lowercase(arbiguard_util:to_binary(V)).

norm_symbol(V) ->
    string:uppercase(arbiguard_util:to_binary(V)).
