-module(arbiguard_runtime_config).

-export([set_exchange_ws_endpoint/4, set_exchange_limits/3, set_exchange_fees/4]).

set_exchange_ws_endpoint(ExchangeID0, Host0, Port0, Path0) ->
    ExchangeID = string:lowercase(arbiguard_util:to_binary(ExchangeID0)),
    Host = arbiguard_util:to_binary(Host0),
    Port = arbiguard_util:to_int(Port0, 443),
    Path = ensure_path(arbiguard_util:to_binary(Path0)),
    Exchanges0 = application:get_env(arbiguard, exchanges, []),
    {Found, Exchanges} = update_exchange(ExchangeID, Host, Port, Path, Exchanges0),
    case Found of
        true ->
            ok = application:set_env(arbiguard, exchanges, Exchanges),
            Result = catch arbiguard_exchange_ticker:set_ws_endpoint(ExchangeID, Host, Port, Path),
            #{ok => true, exchange => ExchangeID, ws_host => Host, ws_port => Port,
              ws_path => Path, ticker_result => format_result(Result)};
        false ->
            #{ok => false, error => <<"exchange_not_found">>, exchange => ExchangeID}
    end.

update_exchange(_ID, _Host, _Port, _Path, []) ->
    {false, []};
update_exchange(ID, Host, Port, Path, [E | Rest]) ->
    case string:lowercase(arbiguard_util:to_binary(maps:get(id, E, <<"">>))) =:= ID of
        true ->
            {true, [E#{ws_host => Host, ws_port => Port, ws_path => Path} | Rest]};
        false ->
            {Found, Rows} = update_exchange(ID, Host, Port, Path, Rest),
            {Found, [E | Rows]}
    end.

ensure_path(<<"/", _/binary>> = Path) -> Path;
ensure_path(Path) -> <<"/", Path/binary>>.

format_result({'EXIT', Reason}) ->
    unicode:characters_to_binary(io_lib:format("~p", [Reason]));
format_result(Result) ->
    Result.

set_exchange_limits(ExchangeID0, MaxSingle0, MaxTotal0) ->
    ExchangeID = string:lowercase(arbiguard_util:to_binary(ExchangeID0)),
    MaxSingle = arbiguard_util:to_float(MaxSingle0, 0),
    MaxTotal = arbiguard_util:to_float(MaxTotal0, 0),
    Exchanges0 = application:get_env(arbiguard, exchanges, []),
    {Found, Exchanges} = update_exchange_limits(ExchangeID, MaxSingle, MaxTotal, Exchanges0),
    case Found of
        true ->
            ok = application:set_env(arbiguard, exchanges, Exchanges),
            #{ok => true, exchange => ExchangeID,
              max_single_order_usdt => MaxSingle,
              max_total_position_usdt => MaxTotal};
        false ->
            #{ok => false, error => <<"exchange_not_found">>, exchange => ExchangeID}
    end.

update_exchange_limits(_ID, _MaxSingle, _MaxTotal, []) ->
    {false, []};
update_exchange_limits(ID, MaxSingle, MaxTotal, [E | Rest]) ->
    case string:lowercase(arbiguard_util:to_binary(maps:get(id, E, <<"">>))) =:= ID of
        true ->
            {true, [E#{max_single_order_usdt => MaxSingle,
                       max_total_position_usdt => MaxTotal} | Rest]};
        false ->
            {Found, Rows} = update_exchange_limits(ID, MaxSingle, MaxTotal, Rest),
            {Found, [E | Rows]}
    end.

set_exchange_fees(ExchangeID0, Maker0, Taker0, Rebate0) ->
    ExchangeID = string:lowercase(arbiguard_util:to_binary(ExchangeID0)),
    Maker = arbiguard_util:to_float(Maker0, 0.0002),
    Taker = arbiguard_util:to_float(Taker0, 0.0005),
    Rebate = clamp01(arbiguard_util:to_float(Rebate0, 0.0)),
    Exchanges0 = application:get_env(arbiguard, exchanges, []),
    {Found, Exchanges} = update_exchange_fees(ExchangeID, Maker, Taker, Rebate, Exchanges0),
    case Found of
        true ->
            ok = application:set_env(arbiguard, exchanges, Exchanges),
            #{ok => true, exchange => ExchangeID,
              maker_fee_rate => Maker,
              taker_fee_rate => Taker,
              fee_rebate_rate => Rebate,
              effective_taker_fee_rate => Taker * (1.0 - Rebate)};
        false ->
            #{ok => false, error => <<"exchange_not_found">>, exchange => ExchangeID}
    end.

update_exchange_fees(_ID, _Maker, _Taker, _Rebate, []) ->
    {false, []};
update_exchange_fees(ID, Maker, Taker, Rebate, [E | Rest]) ->
    case string:lowercase(arbiguard_util:to_binary(maps:get(id, E, <<"">>))) =:= ID of
        true ->
            {true, [E#{maker_fee_rate => Maker,
                       taker_fee_rate => Taker,
                       fee_rebate_rate => Rebate} | Rest]};
        false ->
            {Found, Rows} = update_exchange_fees(ID, Maker, Taker, Rebate, Rest),
            {Found, [E | Rows]}
    end.

clamp01(V) when V < 0 -> 0.0;
clamp01(V) when V > 1 -> 1.0;
clamp01(V) -> V.
