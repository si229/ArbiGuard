-module(arbiguard_market).

-export([fetch/1]).

fetch(Exchange) ->
    ID = string:lowercase(arbiguard_util:to_binary(maps:get(id, Exchange, <<"">>))),
    case ID of
        <<"binance">> -> fetch_binance(Exchange);
        <<"gate">> -> fetch_gate(Exchange);
        <<"okx">> -> {ok, []};
        <<"htx">> -> {ok, []};
        <<"weex">> -> {ok, []};
        _ -> {ok, []}
    end.

fetch_binance(Exchange) ->
    Base = trim_right(maps:get(base_url, Exchange, <<"https://fapi.binance.com">>)),
    case http_json(<<Base/binary, "/fapi/v1/premiumIndex">>) of
        {ok, Items} when is_list(Items) ->
            Rows = [binance_row(Exchange, I) || I <- Items],
            {ok, [R || R <- Rows, maps:get(symbol, R, <<"">>) =/= <<"">>, maps:get(mark_price, R, 0) > 0]};
        Error -> Error
    end.

binance_row(Exchange, I) ->
    Symbol = normalize_symbol(maps:get(symbol, I, <<"">>)),
    #{exchange => <<"binance">>,
      exchange_name => maps:get(name, Exchange, <<"Binance">>),
      symbol => Symbol,
      raw_symbol => maps:get(symbol, I, <<"">>),
      mark_price => f(maps:get(markPrice, I, 0), 0),
      index_price => f(maps:get(indexPrice, I, 0), 0),
      funding_rate => f(maps:get(lastFundingRate, I, 0), 0),
      next_funding_time => arbiguard_util:to_int(maps:get(nextFundingTime, I, 0), 0),
      funding_interval_hours => f(maps:get(funding_interval_hours, Exchange, 8), 8),
      maker_fee_rate => f(maps:get(maker_fee_rate, Exchange, 0.0002), 0.0002),
      taker_fee_rate => f(maps:get(taker_fee_rate, Exchange, 0.0005), 0.0005),
      quote_volume => 0,
      updated_at => arbiguard_util:now_ms()}.

fetch_gate(Exchange) ->
    Base = trim_right(maps:get(base_url, Exchange, <<"https://api.gateio.ws/api/v4">>)),
    case http_json(<<Base/binary, "/futures/usdt/contracts">>) of
        {ok, Items} when is_list(Items) ->
            Rows = [gate_row(Exchange, I) || I <- Items],
            {ok, [R || R <- Rows, maps:get(symbol, R, <<"">>) =/= <<"">>, maps:get(mark_price, R, 0) > 0]};
        Error -> Error
    end.

gate_row(Exchange, I) ->
    Raw = maps:get(name, I, maps:get(contract, I, <<"">>)),
    #{exchange => <<"gate">>,
      exchange_name => maps:get(name, Exchange, <<"Gate.io">>),
      symbol => normalize_symbol(Raw),
      raw_symbol => Raw,
      mark_price => f(maps:get(mark_price, I, maps:get(last_price, I, 0)), 0),
      index_price => f(maps:get(index_price, I, 0), 0),
      funding_rate => f(maps:get(funding_rate, I, 0), 0),
      next_funding_time => arbiguard_util:to_int(maps:get(funding_next_apply, I, 0), 0) * 1000,
      funding_interval_hours => f(maps:get(funding_interval, I, maps:get(funding_interval_hours, Exchange, 8)), 8),
      maker_fee_rate => f(maps:get(maker_fee_rate, Exchange, 0.0002), 0.0002),
      taker_fee_rate => f(maps:get(taker_fee_rate, Exchange, 0.0005), 0.0005),
      quote_volume => f(maps:get(volume_24h_quote, I, maps:get(volume_24h_settle, I, 0)), 0),
      delist_time => arbiguard_util:to_int(maps:get(delisting_time, I, 0), 0) * 1000,
      updated_at => arbiguard_util:now_ms()}.

http_json(UrlBin) ->
    Url = binary_to_list(UrlBin),
    Headers = [{"accept", "application/json"}, {"user-agent", "ArbiGuard/0.1"}],
    Opts = [{timeout, 10000}],
    case httpc:request(get, {Url, Headers}, Opts, [{body_format, binary}]) of
        {ok, {{_, Code, _}, _RespHeaders, Body}} when Code >= 200, Code < 300 ->
            try {ok, arbiguard_json:decode(Body)}
            catch _:Reason -> {error, {json_decode_failed, Reason}}
            end;
        {ok, {{_, Code, _}, _RespHeaders, Body}} ->
            {error, {http_status, Code, Body}};
        {error, Reason} ->
            {error, Reason}
    end.

normalize_symbol(V) ->
    Upper = string:uppercase(arbiguard_util:to_binary(V)),
    binary:replace(Upper, <<"_">>, <<"">>, [global]).

trim_right(Bin) ->
    list_to_binary(string:trim(binary_to_list(arbiguard_util:to_binary(Bin)), trailing, "/")).

f(V, D) ->
    arbiguard_util:to_float(V, D).
