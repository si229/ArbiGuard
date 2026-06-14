-module(arbiguard_ets).

-export([init/0]).
-export([put_ticker/1, delete_ticker/2, get_ticker/2, all_tickers/0,
         put_funding/1, get_funding/2, all_funding/0,
         put_opportunities/1, put_opportunities/2, opportunity_snapshot/0, all_opportunities/0,
         put_order_owner/3, find_order_owner/3,
         put_position_owner/4, find_position_owner/3]).

-define(TICKER, arbiguard_ticker_ets).
-define(FUNDING, arbiguard_funding_ets).
-define(OPPORTUNITY, arbiguard_opportunity_ets).
-define(OPPORTUNITY_META_KEY, '__meta__').
-define(ORDER_OWNER, arbiguard_order_owner_ets).
-define(POSITION_OWNER, arbiguard_position_owner_ets).

init() ->
    new_table(?TICKER),
    new_table(?FUNDING),
    new_table(?OPPORTUNITY),
    new_table(?ORDER_OWNER),
    new_table(?POSITION_OWNER),
    ok.

put_ticker(Row) ->
    Key = key(Row),
    true = ets:insert(?TICKER, {Key, Row}),
    ok.

delete_ticker(Exchange, Symbol) ->
    true = ets:delete(?TICKER, {norm_exchange(Exchange), norm_symbol(Symbol)}),
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
    put_opportunities(#{}, Ops).

put_opportunities(Req, Ops) ->
    ets:delete_all_objects(?OPPORTUNITY),
    [ets:insert(?OPPORTUNITY, {opportunity_key(Op), Op}) || Op <- Ops],
    ets:insert(?OPPORTUNITY, {?OPPORTUNITY_META_KEY, #{req => Req,
                                                       count => length(Ops),
                                                       updated_at => arbiguard_util:now_ms()}}),
    ok.

all_opportunities() ->
    [Row || {Key, Row} <- ets:tab2list(?OPPORTUNITY), Key =/= ?OPPORTUNITY_META_KEY].

opportunity_snapshot() ->
    Meta = case ets:lookup(?OPPORTUNITY, ?OPPORTUNITY_META_KEY) of
        [{?OPPORTUNITY_META_KEY, M}] -> M;
        [] -> #{req => #{}, count => 0, updated_at => 0}
    end,
    Meta#{opportunities => all_opportunities()}.

put_order_owner(AccountID, ExchangeID, Order) ->
    Owner = maps:get(owner_pid, Order, undefined),
    IDs = unique_non_empty([maps:get(id, Order, <<"">>),
                            maps:get(order_id, Order, <<"">>),
                            maps:get(client_order_id, Order, <<"">>),
                            maps:get(parent_id, Order, <<"">>)]),
    Row = #{account_id => norm_account(AccountID),
            exchange => norm_exchange(ExchangeID),
            owner_pid => Owner,
            order => maps:without([req, opportunity, position], Order),
            updated_at => arbiguard_util:now_ms()},
    [ets:insert(?ORDER_OWNER, {{norm_account(AccountID), norm_exchange(ExchangeID), ID}, Row}) || ID <- IDs, is_pid(Owner)],
    ok.

find_order_owner(AccountID, ExchangeID, Event) ->
    IDs = unique_non_empty([maps:get(order_id, Event, <<"">>),
                            maps:get(client_order_id, Event, <<"">>),
                            maps:get(id, Event, <<"">>),
                            maps:get(parent_id, Event, <<"">>)]),
    find_owner(?ORDER_OWNER, norm_account(AccountID), norm_exchange(ExchangeID), IDs).

put_position_owner(AccountID, ExchangeID, Position, OwnerPid) when is_pid(OwnerPid) ->
    IDs = unique_non_empty([maps:get(position_id, Position, <<"">>),
                            maps:get(id, Position, <<"">>),
                            position_symbol_side_key(Position)]),
    Row = #{account_id => norm_account(AccountID),
            exchange => norm_exchange(ExchangeID),
            owner_pid => OwnerPid,
            position => maps:without([req, opportunity], Position),
            updated_at => arbiguard_util:now_ms()},
    [ets:insert(?POSITION_OWNER, {{norm_account(AccountID), norm_exchange(ExchangeID), ID}, Row}) || ID <- IDs],
    ok;
put_position_owner(_AccountID, _ExchangeID, _Position, _OwnerPid) ->
    ok.

find_position_owner(AccountID, ExchangeID, Event) ->
    IDs = unique_non_empty([maps:get(position_id, Event, <<"">>),
                            maps:get(id, Event, <<"">>),
                            position_symbol_side_key(Event)]),
    find_owner(?POSITION_OWNER, norm_account(AccountID), norm_exchange(ExchangeID), IDs).

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

norm_account(V) ->
    arbiguard_util:to_binary(V).

unique_non_empty(Values) ->
    lists:usort([arbiguard_util:to_binary(V) || V <- Values, arbiguard_util:to_binary(V) =/= <<"">>]).

find_owner(_Table, _AccountID, _ExchangeID, []) ->
    not_found;
find_owner(Table, AccountID, ExchangeID, [ID | Rest]) ->
    case ets:lookup(Table, {AccountID, ExchangeID, ID}) of
        [{_, #{owner_pid := OwnerPid} = Row}] when is_pid(OwnerPid) -> {ok, OwnerPid, Row};
        _ -> find_owner(Table, AccountID, ExchangeID, Rest)
    end.

position_symbol_side_key(Row) ->
    Symbol = norm_symbol(maps:get(symbol, Row, <<"">>)),
    Side = string:lowercase(arbiguard_util:to_binary(maps:get(side, Row, maps:get(position_side, Row, <<"">>)))),
    case Symbol =/= <<"">> andalso Side =/= <<"">> of
        true -> <<Symbol/binary, "|", Side/binary>>;
        false -> <<"">>
    end.
