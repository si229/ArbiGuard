-module(arbiguard_market).

-export([fetch/1]).

fetch(Exchange) ->
    ID = string:lowercase(arbiguard_util:to_binary(maps:get(id, Exchange, <<"">>))),
    case ID of
        <<"binance">> -> fetch_binance(Exchange);
        <<"gate">> -> fetch_gate(Exchange);
        <<"okx">> -> fetch_okx(Exchange);
        <<"htx">> -> fetch_htx(Exchange);
        <<"weex">> -> fetch_weex(Exchange);
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
    with_exchange_limits(#{exchange => <<"binance">>,
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
      updated_at => arbiguard_util:now_ms()}, Exchange).

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
    with_exchange_limits(#{exchange => <<"gate">>,
      exchange_name => maps:get(name, Exchange, <<"Gate.io">>),
      symbol => normalize_symbol(Raw),
      raw_symbol => Raw,
      mark_price => f(maps:get(mark_price, I, maps:get(last_price, I, 0)), 0),
      index_price => f(maps:get(index_price, I, 0), 0),
      funding_rate => f(maps:get(funding_rate, I, 0), 0),
      next_funding_time => arbiguard_util:to_int(maps:get(funding_next_apply, I, 0), 0) * 1000,
      funding_interval_hours => normalize_interval(f(maps:get(funding_interval, I, maps:get(funding_interval_hours, Exchange, 8)), 8)),
      maker_fee_rate => f(maps:get(maker_fee_rate, Exchange, 0.0002), 0.0002),
      taker_fee_rate => f(maps:get(taker_fee_rate, Exchange, 0.0005), 0.0005),
      quote_volume => f(maps:get(volume_24h_quote, I, maps:get(volume_24h_settle, I, 0)), 0),
      delist_time => arbiguard_util:to_int(maps:get(delisting_time, I, 0), 0) * 1000,
      updated_at => arbiguard_util:now_ms()}, Exchange).

fetch_okx(Exchange) ->
    Base = trim_right(maps:get(base_url, Exchange, <<"https://www.okx.com">>)),
    LiveInst = okx_live_instruments(Base),
    case http_json(<<Base/binary, "/api/v5/market/tickers?instType=SWAP">>) of
        {ok, #{data := Items}} when is_list(Items) ->
            USDT = [I || I <- Items, is_okx_usdt_swap(maps:get(instId, I, <<"">>)),
                         okx_live_allowed(maps:get(instId, I, <<"">>), LiveInst)],
            Rows = [okx_row(Exchange, Base, I) || I <- lists:sublist(sort_by_volume(USDT), 120)],
            {ok, [R || R <- Rows, maps:get(symbol, R, <<"">>) =/= <<"">>, maps:get(mark_price, R, 0) > 0]};
        {ok, _} -> {ok, []};
        Error -> Error
    end.

okx_live_instruments(Base) ->
    case http_json(<<Base/binary, "/api/v5/public/instruments?instType=SWAP">>) of
        {ok, #{data := Items}} when is_list(Items) ->
            maps:from_list([{maps:get(instId, I, <<"">>), I} || I <- Items,
                             is_okx_usdt_swap(maps:get(instId, I, <<"">>)),
                             string:lowercase(arbiguard_util:to_binary(maps:get(state, I, <<"">>))) =:= <<"live">>]);
        _ -> #{}
    end.

okx_row(Exchange, Base, I) ->
    Raw = maps:get(instId, I, <<"">>),
    Funding = okx_funding(Base, Raw),
    FundingTime = arbiguard_util:to_int(maps:get(fundingTime, Funding, 0), 0),
    NextFunding = arbiguard_util:to_int(maps:get(nextFundingTime, Funding, FundingTime), 0),
    PrevFunding = arbiguard_util:to_int(maps:get(prevFundingTime, Funding, 0), 0),
    Interval = interval_hours(FundingTime, NextFunding, interval_hours(PrevFunding, FundingTime, f(maps:get(funding_interval_hours, Exchange, 8), 8))),
    with_exchange_limits(#{exchange => <<"okx">>,
      exchange_name => maps:get(name, Exchange, <<"OKX">>),
      symbol => normalize_symbol(Raw),
      raw_symbol => Raw,
      mark_price => f(maps:get(last, I, 0), 0),
      index_price => 0,
      funding_rate => f(maps:get(fundingRate, Funding, 0), 0),
      next_funding_time => next_funding_time(FundingTime, NextFunding, Interval),
      funding_interval_hours => Interval,
      maker_fee_rate => f(maps:get(maker_fee_rate, Exchange, 0.0002), 0.0002),
      taker_fee_rate => f(maps:get(taker_fee_rate, Exchange, 0.0005), 0.0005),
      quote_volume => f(maps:get(volCcy24h, I, maps:get(vol24h, I, 0)), 0),
      updated_at => arbiguard_util:now_ms()}, Exchange).

okx_funding(Base, Raw) ->
    case http_json(<<Base/binary, "/api/v5/public/funding-rate?instId=", Raw/binary>>) of
        {ok, #{data := [Funding | _]}} -> Funding;
        _ -> #{}
    end.

fetch_htx(Exchange) ->
    Base = trim_right(maps:get(base_url, Exchange, <<"https://api.hbdm.com">>)),
    Contracts = htx_contract_info(Base),
    Prices = htx_prices(Base),
    case http_json(<<Base/binary, "/linear-swap-api/v1/swap_batch_funding_rate">>) of
        {ok, #{data := Items}} when is_list(Items) ->
            Rows = [htx_row(Exchange, I, Contracts, Prices) || I <- Items],
            {ok, [R || R <- Rows, maps:get(symbol, R, <<"">>) =/= <<"">>, maps:get(mark_price, R, 0) > 0]};
        {ok, _} -> {ok, []};
        Error -> Error
    end.

htx_contract_info(Base) ->
    case http_json(<<Base/binary, "/linear-swap-api/v1/swap_contract_info">>) of
        {ok, #{data := Items}} when is_list(Items) ->
            maps:from_list([{normalize_symbol(maps:get(contract_code, I, maps:get(symbol, I, <<"">>))), I} || I <- Items]);
        _ -> #{}
    end.

htx_prices(Base) ->
    case http_json(<<Base/binary, "/linear-swap-ex/market/detail/batch_merged">>) of
        {ok, #{ticks := Items}} when is_list(Items) ->
            maps:from_list([{normalize_symbol(maps:get(contract_code, I, maps:get(symbol, I, <<"">>))), I} || I <- Items]);
        _ -> #{}
    end.

htx_row(Exchange, I, Contracts, Prices) ->
    Raw = maps:get(contract_code, I, maps:get(symbol, I, <<"">>)),
    Symbol = normalize_symbol(Raw),
    Contract = maps:get(Symbol, Contracts, #{}),
    Price = maps:get(Symbol, Prices, #{}),
    CurrentFunding = arbiguard_util:to_int(maps:get(funding_time, I, maps:get(settlement_time, I, 0)), 0),
    NextFunding = arbiguard_util:to_int(maps:get(next_funding_time, I, 0), 0),
    Interval0 = f(maps:get(funding_interval, I, maps:get(funding_interval_hours, I, maps:get(funding_interval_hours, Exchange, 8))), 8),
    Interval = interval_hours(CurrentFunding, NextFunding, Interval0),
    Tradable = htx_tradable(Contract),
    with_exchange_limits(#{exchange => <<"htx">>,
      exchange_name => maps:get(name, Exchange, <<"HTX">>),
      symbol => case Tradable of true -> Symbol; false -> <<"">> end,
      raw_symbol => Raw,
      mark_price => positive(f(maps:get(mark_price, I, 0), 0), f(maps:get(close, Price, maps:get(last_price, Price, 0)), 0)),
      index_price => f(maps:get(index_price, I, 0), 0),
      funding_rate => f(maps:get(funding_rate, I, 0), 0),
      next_funding_time => next_funding_time(CurrentFunding, NextFunding, Interval),
      funding_interval_hours => Interval,
      maker_fee_rate => f(maps:get(maker_fee_rate, Exchange, 0.0002), 0.0002),
      taker_fee_rate => f(maps:get(taker_fee_rate, Exchange, 0.0005), 0.0005),
      quote_volume => f(maps:get(amount, Price, maps:get(vol, Price, 0)), 0),
      delist_time => delist_time(Contract),
      updated_at => arbiguard_util:now_ms()}, Exchange).

fetch_weex(Exchange) ->
    Base = trim_right(maps:get(base_url, Exchange, <<"https://api-contract.weex.com">>)),
    case {http_json(<<Base/binary, "/capi/v3/market/ticker/24hr">>),
          http_json(<<Base/binary, "/capi/v3/market/premiumIndex">>)} of
        {{ok, Tickers0}, {ok, Premiums0}} ->
            Tickers = unwrap_list(Tickers0),
            Premiums = maps:from_list([{normalize_symbol(maps:get(symbol, P, <<"">>)), P} || P <- unwrap_list(Premiums0)]),
            Rows = [weex_row(Exchange, T, Premiums) || T <- Tickers],
            {ok, [R || R <- Rows, maps:get(symbol, R, <<"">>) =/= <<"">>, maps:get(mark_price, R, 0) > 0]};
        {{error, Reason}, _} -> {error, {ticker_failed, Reason}};
        {_, {error, Reason}} -> {error, {premium_failed, Reason}}
    end.

weex_row(Exchange, T, Premiums) ->
    Raw = maps:get(symbol, T, maps:get(instId, T, <<"">>)),
    Symbol = normalize_symbol(Raw),
    Premium = maps:get(Symbol, Premiums, #{}),
    CurrentFunding = arbiguard_util:to_int(maps:get(time, Premium, 0), 0),
    NextFunding = arbiguard_util:to_int(maps:get(nextFundingTime, Premium, 0), 0),
    CycleMinutes = f(maps:get(collectCycle, Premium, 0), 0),
    Interval0 = case CycleMinutes > 0 of true -> CycleMinutes / 60; false -> f(maps:get(funding_interval_hours, Exchange, 8), 8) end,
    Interval = interval_hours(CurrentFunding, NextFunding, Interval0),
    with_exchange_limits(#{exchange => <<"weex">>,
      exchange_name => maps:get(name, Exchange, <<"WEEX">>),
      symbol => Symbol,
      raw_symbol => Raw,
      mark_price => positive(f(maps:get(markPrice, Premium, 0), 0), positive(f(maps:get(markPrice, T, 0), 0), f(maps:get(lastPrice, T, 0), 0))),
      index_price => positive(f(maps:get(indexPrice, Premium, 0), 0), f(maps:get(indexPrice, T, 0), 0)),
      funding_rate => f(maps:get(lastFundingRate, Premium, maps:get(forecastFundingRate, Premium, 0)), 0),
      next_funding_time => next_funding_time(CurrentFunding, NextFunding, Interval),
      funding_interval_hours => Interval,
      maker_fee_rate => f(maps:get(maker_fee_rate, Exchange, 0.0002), 0.0002),
      taker_fee_rate => f(maps:get(taker_fee_rate, Exchange, 0.0006), 0.0006),
      quote_volume => f(maps:get(quoteVolume, T, maps:get(volume, T, 0)), 0),
      delist_time => positive_int(delist_time(T), delist_time(Premium)),
      updated_at => arbiguard_util:now_ms()}, Exchange).

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
    NoDash = binary:replace(Upper, <<"-">>, <<"">>, [global]),
    NoSwap = binary:replace(NoDash, <<"SWAP">>, <<"">>, [global]),
    binary:replace(NoSwap, <<"_">>, <<"">>, [global]).

trim_right(Bin) ->
    list_to_binary(string:trim(binary_to_list(arbiguard_util:to_binary(Bin)), trailing, "/")).

f(V, D) ->
    arbiguard_util:to_float(V, D).

with_exchange_limits(Row, Exchange) ->
    Row#{max_single_order_usdt => f(maps:get(max_single_order_usdt, Exchange, 0), 0),
         max_total_position_usdt => f(maps:get(max_total_position_usdt, Exchange, 0), 0)}.

is_okx_usdt_swap(Raw) ->
    B = string:uppercase(arbiguard_util:to_binary(Raw)),
    binary:match(B, <<"USDT-SWAP">>) =/= nomatch.

okx_live_allowed(_Raw, LiveInst) when map_size(LiveInst) =:= 0 ->
    true;
okx_live_allowed(Raw, LiveInst) ->
    maps:is_key(Raw, LiveInst).

sort_by_volume(Items) ->
    lists:sort(fun(A, B) ->
        f(maps:get(volCcy24h, A, maps:get(vol24h, A, 0)), 0) >=
            f(maps:get(volCcy24h, B, maps:get(vol24h, B, 0)), 0)
    end, Items).

interval_hours(CurrentMs, NextMs, Fallback) ->
    case CurrentMs > 0 andalso NextMs > CurrentMs of
        true -> (NextMs - CurrentMs) / 3600000;
        false -> normalize_interval(Fallback)
    end.

normalize_interval(Value) when Value =< 0 ->
    8;
normalize_interval(Value) when Value > 24 ->
    Value / 3600;
normalize_interval(Value) ->
    Value.

next_funding_time(CurrentMs, NextMs, IntervalHours) ->
    Now = arbiguard_util:now_ms(),
    Base0 = case NextMs > Now of
        true -> NextMs;
        false ->
            case CurrentMs > Now of
                true -> CurrentMs;
                false -> positive_int(NextMs, CurrentMs)
            end
    end,
    case Base0 > 0 of
        false -> 0;
        true ->
            Step = trunc(max(1, IntervalHours) * 3600000),
            next_funding_time_loop(Base0, Step, Now)
    end.

next_funding_time_loop(Base, _Step, Now) when Base > Now ->
    Base;
next_funding_time_loop(Base, Step, Now) ->
    next_funding_time_loop(Base + Step, Step, Now).

positive(Value, _Fallback) when Value > 0 ->
    Value;
positive(_, Fallback) ->
    Fallback.

positive_int(Value, _Fallback) when is_integer(Value), Value > 0 ->
    Value;
positive_int(Value, _Fallback) when is_float(Value), Value > 0 ->
    trunc(Value);
positive_int(_, Fallback) ->
    Fallback.

delist_time(Row) ->
    positive_int(arbiguard_util:to_int(maps:get(delistTime, Row, 0), 0),
                 positive_int(arbiguard_util:to_int(maps:get(delisting_time, Row, 0), 0) * 1000,
                              arbiguard_util:to_int(maps:get(delivery_time, Row, 0), 0))).

htx_tradable(Row) when map_size(Row) =:= 0 ->
    true;
htx_tradable(Row) ->
    Status = string:lowercase(arbiguard_util:to_binary(maps:get(contract_status, Row, maps:get(status, Row, <<"">>)))),
    lists:member(Status, [<<"">>, <<"1">>, <<"trading">>, <<"online">>, <<"normal">>, <<"enable">>, <<"enabled">>]).

unwrap_list(Value) when is_list(Value) ->
    Value;
unwrap_list(#{data := List}) when is_list(List) ->
    List;
unwrap_list(#{result := List}) when is_list(List) ->
    List;
unwrap_list(_) ->
    [].
