-module(arbiguard_calc).

-export([build_opportunity/4, normalize_request/1, default_exchanges/0]).

-define(DELIST_RISK_MS, 12 * 3600 * 1000).

normalize_request(Req0) ->
    Defaults = default_scan(),
    Req1 = maps:merge(Defaults, Req0),
    Capital = f(maps:get(capital_usdt, Req1, 10000), 10000),
    Req1#{
        capital_usdt => Capital,
        max_position_pct => f(maps:get(max_position_pct, Req1, 0.1), 0.1),
        execution_notional_usdt => f(maps:get(execution_notional_usdt, Req1, 200), 200),
        min_funding_rate => f(maps:get(min_funding_rate, Req1, 0.0003), 0.0003),
        min_price_gap_rate => f(maps:get(min_price_gap_rate, Req1, 0.002), 0.002),
        max_basis_rate => f(maps:get(max_basis_rate, Req1, 0.02), 0.02),
        min_quote_volume => f(maps:get(min_quote_volume, Req1, 0), 0),
        execution_window_sec => f(maps:get(execution_window_sec, Req1, 60), 60),
        fast_refresh_sec => f(maps:get(fast_refresh_sec, Req1, 1), 1),
        limit => arbiguard_util:to_int(maps:get(limit, Req1, 30), 30),
        paper_leverage => f(maps:get(paper_leverage, Req1, 10), 10),
        min_execution_profit_usdt => f(maps:get(min_execution_profit_usdt, Req1, 5), 5),
        price_gap_close_profit_usdt => f(maps:get(price_gap_close_profit_usdt, Req1, 10), 10),
        execution_order_mode => maps:get(execution_order_mode, Req1, <<"fok">>),
        exchanges => normalize_exchanges(maps:get(exchanges, Req1, default_exchanges()))
    }.

default_scan() ->
    Config = application:get_env(arbiguard, default_scan, #{}),
    Exchanges = application:get_env(arbiguard, exchanges, default_exchanges()),
    Config#{exchanges => Exchanges}.

default_exchanges() ->
    [#{id => <<"binance">>, name => <<"Binance">>, enabled => true, base_url => <<"https://fapi.binance.com">>, maker_fee_rate => 0.0002, taker_fee_rate => 0.0005, funding_interval_hours => 8},
     #{id => <<"okx">>, name => <<"OKX">>, enabled => true, base_url => <<"https://www.okx.com">>, maker_fee_rate => 0.0002, taker_fee_rate => 0.0005, funding_interval_hours => 8},
     #{id => <<"gate">>, name => <<"Gate.io">>, enabled => true, base_url => <<"https://api.gateio.ws/api/v4">>, maker_fee_rate => 0.0002, taker_fee_rate => 0.0005, funding_interval_hours => 8},
     #{id => <<"weex">>, name => <<"WEEX">>, enabled => true, base_url => <<"https://api-contract.weex.com">>, maker_fee_rate => 0.0002, taker_fee_rate => 0.0006, funding_interval_hours => 8},
     #{id => <<"htx">>, name => <<"HTX">>, enabled => true, base_url => <<"https://api.hbdm.com">>, maker_fee_rate => 0.0002, taker_fee_rate => 0.0005, funding_interval_hours => 8}].

normalize_exchanges(Rows) ->
    [normalize_exchange(E) || E <- Rows, maps:get(enabled, E, true) =:= true].

normalize_exchange(E) ->
    ID = lower_bin(maps:get(id, E, <<"">>)),
    E#{
        id => ID,
        name => maps:get(name, E, string:uppercase(ID)),
        maker_fee_rate => f(maps:get(maker_fee_rate, E, 0.0002), 0.0002),
        taker_fee_rate => f(maps:get(taker_fee_rate, E, 0.0005), 0.0005),
        funding_interval_hours => f(maps:get(funding_interval_hours, E, 8), 8),
        max_single_order_usdt => f(maps:get(max_single_order_usdt, E, 0), 0),
        max_total_position_usdt => f(maps:get(max_total_position_usdt, E, 0), 0)
    }.

build_opportunity(Req, Symbol, LongLeg, ShortLeg) ->
    case valid_legs(Req, LongLeg, ShortLeg) of
        false -> false;
        true ->
            Notional = max_position_usdt(Req),
            LongPrice = f(maps:get(mark_price, LongLeg, 0), 0),
            ShortPrice = f(maps:get(mark_price, ShortLeg, 0), 0),
            Mid = (LongPrice + ShortPrice) / 2,
            PriceGap = (ShortPrice - LongPrice) / Mid,
            FundingEdge0 = funding_window_return(LongLeg, ShortLeg),
            {FundingEdge, Method0} =
                case can_collect_short_before_long(LongLeg, ShortLeg) of
                    true -> {f(maps:get(funding_rate, ShortLeg, 0), 0), <<"pre_settlement_short_funding">>};
                    false -> {FundingEdge0, <<"funding_rate_spread">>}
                end,
            MinFunding = f(maps:get(min_funding_rate, Req, 0.0003), 0.0003),
            MinGap = f(maps:get(min_price_gap_rate, Req, 0.002), 0.002),
            case (FundingEdge >= MinFunding) orelse (abs(PriceGap) >= MinGap) of
                false -> false;
                true ->
                    OpenFeeRate = f(maps:get(taker_fee_rate, LongLeg, 0.0005), 0.0005) + f(maps:get(taker_fee_rate, ShortLeg, 0.0005), 0.0005),
                    RoundTripFeeRate = OpenFeeRate * 2,
                    ExpectedNetReturn = FundingEdge + PriceGap - RoundTripFeeRate,
                    case ExpectedNetReturn > 0 of
                        false -> false;
                        true ->
                            Type = classify(FundingEdge, PriceGap, MinFunding, MinGap),
                            Method = method(Type, Method0),
                            EstimatedFunding = Notional * FundingEdge,
                            RoundTripFee = Notional * RoundTripFeeRate,
                            BreakEven = case EstimatedFunding > 0 of true -> RoundTripFee / EstimatedFunding; false -> 999.0 end,
                            SecondsToFunding = seconds_until(arbiguard_util:min_positive(maps:get(next_funding_time, ShortLeg, 0), maps:get(next_funding_time, LongLeg, 0))),
                            #{symbol => Symbol,
                              opportunity_type => Type,
                              arbitrage_method => Method,
                              long_exchange => maps:get(exchange, LongLeg),
                              short_exchange => maps:get(exchange, ShortLeg),
                              long_price => LongPrice,
                              short_price => ShortPrice,
                              long_mark_price => LongPrice,
                              short_mark_price => ShortPrice,
                              long_funding_rate => f(maps:get(funding_rate, LongLeg, 0), 0),
                              short_funding_rate => f(maps:get(funding_rate, ShortLeg, 0), 0),
                              long_funding_interval_hours => f(maps:get(funding_interval_hours, LongLeg, 8), 8),
                              short_funding_interval_hours => f(maps:get(funding_interval_hours, ShortLeg, 8), 8),
                              long_next_funding_time => maps:get(next_funding_time, LongLeg, 0),
                              short_next_funding_time => maps:get(next_funding_time, ShortLeg, 0),
                              long_fee_rate => f(maps:get(taker_fee_rate, LongLeg, 0.0005), 0.0005),
                              short_fee_rate => f(maps:get(taker_fee_rate, ShortLeg, 0.0005), 0.0005),
                              basis_rate => PriceGap,
                              price_gap_return => PriceGap,
                              funding_edge_return => FundingEdge,
                              expected_gross_return => FundingEdge + PriceGap,
                              expected_net_return => ExpectedNetReturn,
                              estimated_net_profit => Notional * ExpectedNetReturn,
                              estimated_funding => EstimatedFunding,
                              round_trip_fee => RoundTripFee,
                              open_fee => Notional * OpenFeeRate,
                              break_even_fundings => BreakEven,
                              suggested_notional => Notional,
                              seconds_to_funding => SecondsToFunding,
                              in_execution_window => SecondsToFunding > 0 andalso SecondsToFunding =< f(maps:get(execution_window_sec, Req, 60), 60),
                              recommended_timing => timing(SecondsToFunding, f(maps:get(execution_window_sec, Req, 60), 60)),
                              risk_level => risk(ExpectedNetReturn, PriceGap, BreakEven)}
                    end
            end
    end.

valid_legs(Req, LongLeg, ShortLeg) ->
    LongEx = maps:get(exchange, LongLeg, <<"">>),
    ShortEx = maps:get(exchange, ShortLeg, <<"">>),
    LongPrice = f(maps:get(mark_price, LongLeg, 0), 0),
    ShortPrice = f(maps:get(mark_price, ShortLeg, 0), 0),
    MinVol = f(maps:get(min_quote_volume, Req, 0), 0),
    MaxBasis = f(maps:get(max_basis_rate, Req, 0.02), 0.02),
    Same = LongEx =:= ShortEx,
    VolOK = (f(maps:get(quote_volume, LongLeg, 0), 0) =:= 0 orelse f(maps:get(quote_volume, LongLeg, 0), 0) >= MinVol) andalso
            (f(maps:get(quote_volume, ShortLeg, 0), 0) =:= 0 orelse f(maps:get(quote_volume, ShortLeg, 0), 0) >= MinVol),
    Mid = case LongPrice > 0 andalso ShortPrice > 0 of true -> (LongPrice + ShortPrice) / 2; false -> 0 end,
    BasisOK = Mid > 0 andalso abs((ShortPrice - LongPrice) / Mid) =< MaxBasis,
    (not Same) andalso LongPrice > 0 andalso ShortPrice > 0 andalso VolOK andalso BasisOK andalso
        short_cycle_positive(LongLeg, ShortLeg) andalso (not delist_risk(LongLeg)) andalso (not delist_risk(ShortLeg)).

short_cycle_positive(LongLeg, ShortLeg) ->
    LI = normalize_window(f(maps:get(funding_interval_hours, LongLeg, 8), 8)),
    SI = normalize_window(f(maps:get(funding_interval_hours, ShortLeg, 8), 8)),
    case abs(LI - SI) < 0.001 of
        true -> true;
        false when LI < SI -> f(maps:get(funding_rate, LongLeg, 0), 0) < 0;
        false -> f(maps:get(funding_rate, ShortLeg, 0), 0) > 0
    end.

funding_window_return(LongLeg, ShortLeg) ->
    LI = normalize_window(f(maps:get(funding_interval_hours, LongLeg, 8), 8)),
    SI = normalize_window(f(maps:get(funding_interval_hours, ShortLeg, 8), 8)),
    W = max(LI, SI),
    ShortIncome = f(maps:get(funding_rate, ShortLeg, 0), 0) * (W / SI),
    LongCost = f(maps:get(funding_rate, LongLeg, 0), 0) * (W / LI),
    ShortIncome - LongCost.

can_collect_short_before_long(LongLeg, ShortLeg) ->
    S = maps:get(next_funding_time, ShortLeg, 0),
    L = maps:get(next_funding_time, LongLeg, 0),
    S > 0 andalso L > 0 andalso S + 60000 < L.

classify(FundingEdge, PriceGap, MinFunding, MinGap) ->
    HasFunding = FundingEdge >= MinFunding,
    HasGap = abs(PriceGap) >= MinGap,
    case {HasFunding, HasGap} of
        {true, true} -> <<"funding_and_price_gap">>;
        {false, true} -> <<"price_gap">>;
        _ -> <<"funding">>
    end.

method(<<"price_gap">>, _) -> <<"cross_exchange_price_gap">>;
method(<<"funding_and_price_gap">>, <<"funding_rate_spread">>) -> <<"funding_and_price_gap">>;
method(_, Method) -> Method.

risk(Net, Basis, BreakEven) when Net >= 0.005, abs(Basis) =< 0.01, BreakEven =< 3 -> <<"low">>;
risk(Net, Basis, _BreakEven) when Net >= 0.002, abs(Basis) =< 0.02 -> <<"medium">>;
risk(_, _, _) -> <<"high">>.

timing(Sec, Window) when Sec > 0, Sec =< Window -> <<"execute_now">>;
timing(Sec, Window) when Sec > 0, Sec =< Window * 5 -> <<"pre_execution_watch">>;
timing(Sec, _Window) when Sec > 0 -> <<"wait">>;
timing(_, _) -> <<"missing_funding_time">>.

seconds_until(0) -> 0;
seconds_until(Ms) -> max(0, (Ms - arbiguard_util:now_ms()) div 1000).

max_position_usdt(Req) ->
    Capital = f(maps:get(capital_usdt, Req, 10000), 10000),
    Pct = f(maps:get(max_position_pct, Req, 0.1), 0.1),
    Exec = f(maps:get(execution_notional_usdt, Req, 200), 200),
    max(Exec, Capital * Pct).

normalize_window(V) when V =< 0 -> 8;
normalize_window(V) -> max(1, V).

delist_risk(Leg) ->
    T = maps:get(delist_time, Leg, 0),
    T > 0 andalso T - arbiguard_util:now_ms() =< ?DELIST_RISK_MS.

f(V, D) ->
    arbiguard_util:to_float(V, D).

lower_bin(V) ->
    string:lowercase(arbiguard_util:to_binary(V)).
