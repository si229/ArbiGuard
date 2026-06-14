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
        max_open_positions => max(0, arbiguard_util:to_int(maps:get(max_open_positions, Req1, 5), 5)),
        execution_notional_usdt => f(maps:get(execution_notional_usdt, Req1, 200), 200),
        min_quote_volume => f(maps:get(min_quote_volume, Req1, 0), 0),
        execution_window_sec => f(maps:get(execution_window_sec, Req1, 60), 60),
        fast_refresh_sec => f(maps:get(fast_refresh_sec, Req1, 1), 1),
        limit => arbiguard_util:to_int(maps:get(limit, Req1, 30), 30),
        paper_leverage => f(maps:get(paper_leverage, Req1, 10), 10),
        min_execution_profit_usdt => f(maps:get(min_execution_profit_usdt, Req1, 10), 10),
        price_gap_close_profit_usdt => f(maps:get(price_gap_close_profit_usdt, Req1, 10), 10),
        execution_order_mode => maps:get(execution_order_mode, Req1, <<"fok">>),
        exchanges => normalize_exchanges(maps:get(exchanges, Req1, default_exchanges()))
    }.

default_scan() ->
    Config = application:get_env(arbiguard, default_scan, #{}),
    Exchanges = application:get_env(arbiguard, exchanges, default_exchanges()),
    Config#{exchanges => Exchanges}.

default_exchanges() ->
    [#{id => <<"binance">>, name => <<"Binance">>, enabled => true,
       base_url => <<"https://fapi.binance.com">>,
       ws_host => <<"fstream.binance.com">>, ws_port => 443, ws_path => <<"/public/ws">>,
       maker_fee_rate => 0.0002, taker_fee_rate => 0.0005, fee_rebate_rate => 0.0, funding_interval_hours => 8},
     #{id => <<"okx">>, name => <<"OKX">>, enabled => true,
       base_url => <<"https://www.okx.com">>,
       ws_host => <<"ws.okx.com">>, ws_port => 8443, ws_path => <<"/ws/v5/public">>,
       maker_fee_rate => 0.0002, taker_fee_rate => 0.0005, fee_rebate_rate => 0.0, funding_interval_hours => 8},
     #{id => <<"gate">>, name => <<"Gate.io">>, enabled => true,
       base_url => <<"https://api.gateio.ws/api/v4">>,
       ws_host => <<"fx-ws.gateio.ws">>, ws_port => 443, ws_path => <<"/v4/ws/usdt">>,
       maker_fee_rate => 0.0002, taker_fee_rate => 0.0005, fee_rebate_rate => 0.0, funding_interval_hours => 8},
     #{id => <<"weex">>, name => <<"WEEX">>, enabled => true,
       base_url => <<"https://api-contract.weex.com">>,
       ws_host => <<"ws-contract.weex.com">>, ws_port => 443, ws_path => <<"/v3/ws/public">>,
       maker_fee_rate => 0.0002, taker_fee_rate => 0.0006, fee_rebate_rate => 0.8, funding_interval_hours => 8},
     #{id => <<"htx">>, name => <<"HTX">>, enabled => true,
       base_url => <<"https://api.hbdm.com">>,
       ws_host => <<"api.hbdm.com">>, ws_port => 443, ws_path => <<"/linear-swap-ws">>,
       maker_fee_rate => 0.0002, taker_fee_rate => 0.0005, fee_rebate_rate => 0.0, funding_interval_hours => 8}].

normalize_exchanges(Rows) ->
    [normalize_exchange(E) || E <- Rows, maps:get(enabled, E, true) =:= true].

normalize_exchange(E) ->
    ID = lower_bin(maps:get(id, E, <<"">>)),
    E#{
        id => ID,
        name => maps:get(name, E, string:uppercase(ID)),
        maker_fee_rate => f(maps:get(maker_fee_rate, E, 0.0002), 0.0002),
        taker_fee_rate => f(maps:get(taker_fee_rate, E, 0.0005), 0.0005),
        fee_rebate_rate => clamp01(f(maps:get(fee_rebate_rate, E, 0.0), 0.0)),
        funding_interval_hours => f(maps:get(funding_interval_hours, E, 8), 8),
        max_single_order_usdt => f(maps:get(max_single_order_usdt, E, 0), 0),
        max_total_position_usdt => f(maps:get(max_total_position_usdt, E, 0), 0)
    }.

build_opportunity(Req, Symbol, LongLeg, ShortLeg) ->
    case valid_legs(Req, LongLeg, ShortLeg) of
        false -> false;
        true ->
            Notional = max_position_usdt(Req, LongLeg, ShortLeg),
            LongMarkPrice = f(maps:get(mark_price, LongLeg, 0), 0),
            ShortMarkPrice = f(maps:get(mark_price, ShortLeg, 0), 0),
            %% Opening execution uses taker prices: buy long at ask, sell short at bid.
            %% Fall back to mark only when the exchange snapshot has no executable quote yet.
            LongPrice = executable_open_price(long, LongLeg),
            ShortPrice = executable_open_price(short, ShortLeg),
            Mid = (LongPrice + ShortPrice) / 2,
            PriceGap = executable_price_gap(LongLeg, ShortLeg, LongPrice, ShortPrice, Mid),
            FundingEdge0 = next_settlement_return(LongLeg, ShortLeg),
            {FundingEdge, Method0} =
                case can_collect_short_before_long(LongLeg, ShortLeg) of
                    true -> {f(maps:get(funding_rate, ShortLeg, 0), 0), <<"pre_settlement_short_funding">>};
                    false -> {FundingEdge0, <<"funding_rate_spread">>}
                end,
            LongGrossFeeRate = f(maps:get(taker_fee_rate, LongLeg, 0.0005), 0.0005),
            ShortGrossFeeRate = f(maps:get(taker_fee_rate, ShortLeg, 0.0005), 0.0005),
            LongFeeRate = effective_fee_rate(LongLeg),
            ShortFeeRate = effective_fee_rate(ShortLeg),
            OpenFeeRate = LongFeeRate + ShortFeeRate,
            RoundTripFeeRate = OpenFeeRate * 2,
            ExpectedNetReturn = FundingEdge + PriceGap - RoundTripFeeRate,
            case ExpectedNetReturn > 0 of
                false -> false;
                true ->
                            Type = classify(FundingEdge, PriceGap, 0.0, 0.0),
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
                              long_bid => f(maps:get(bid, LongLeg, 0), 0),
                              long_ask => f(maps:get(ask, LongLeg, LongPrice), LongPrice),
                              short_bid => f(maps:get(bid, ShortLeg, ShortPrice), ShortPrice),
                              short_ask => f(maps:get(ask, ShortLeg, 0), 0),
                              long_updated_at => maps:get(updated_at, LongLeg, 0),
                              short_updated_at => maps:get(updated_at, ShortLeg, 0),
                              execution_price_basis => executable_price_basis(LongLeg, ShortLeg),
                              long_mark_price => LongMarkPrice,
                              short_mark_price => ShortMarkPrice,
                              long_funding_rate => f(maps:get(funding_rate, LongLeg, 0), 0),
                              short_funding_rate => f(maps:get(funding_rate, ShortLeg, 0), 0),
                              long_funding_interval_hours => f(maps:get(funding_interval_hours, LongLeg, 8), 8),
                              short_funding_interval_hours => f(maps:get(funding_interval_hours, ShortLeg, 8), 8),
                              long_next_funding_time => maps:get(next_funding_time, LongLeg, 0),
                              short_next_funding_time => maps:get(next_funding_time, ShortLeg, 0),
                              long_fee_rate => LongFeeRate,
                              short_fee_rate => ShortFeeRate,
                              long_gross_fee_rate => LongGrossFeeRate,
                              short_gross_fee_rate => ShortGrossFeeRate,
                              long_fee_rebate_rate => clamp01(f(maps:get(fee_rebate_rate, LongLeg, 0), 0)),
                              short_fee_rebate_rate => clamp01(f(maps:get(fee_rebate_rate, ShortLeg, 0), 0)),
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
    end.

valid_legs(Req, LongLeg, ShortLeg) ->
    LongEx = maps:get(exchange, LongLeg, <<"">>),
    ShortEx = maps:get(exchange, ShortLeg, <<"">>),
    LongPrice = f(maps:get(mark_price, LongLeg, 0), 0),
    ShortPrice = f(maps:get(mark_price, ShortLeg, 0), 0),
    MinVol = f(maps:get(min_quote_volume, Req, 0), 0),
    Same = LongEx =:= ShortEx,
    VolOK = (f(maps:get(quote_volume, LongLeg, 0), 0) =:= 0 orelse f(maps:get(quote_volume, LongLeg, 0), 0) >= MinVol) andalso
            (f(maps:get(quote_volume, ShortLeg, 0), 0) =:= 0 orelse f(maps:get(quote_volume, ShortLeg, 0), 0) >= MinVol),
    Mid = case LongPrice > 0 andalso ShortPrice > 0 of true -> (LongPrice + ShortPrice) / 2; false -> 0 end,
    (not Same) andalso LongPrice > 0 andalso ShortPrice > 0 andalso VolOK andalso Mid > 0 andalso
        short_cycle_positive(LongLeg, ShortLeg) andalso (not delist_risk(LongLeg)) andalso (not delist_risk(ShortLeg)).

executable_open_price(long, Leg) ->
    choose_positive(f(maps:get(ask, Leg, 0), 0), f(maps:get(mark_price, Leg, 0), 0));
executable_open_price(short, Leg) ->
    choose_positive(f(maps:get(bid, Leg, 0), 0), f(maps:get(mark_price, Leg, 0), 0)).

executable_price_basis(LongLeg, ShortLeg) ->
    LongAsk = f(maps:get(ask, LongLeg, 0), 0),
    ShortBid = f(maps:get(bid, ShortLeg, 0), 0),
    case LongAsk > 0 andalso ShortBid > 0 of
        true -> <<"taker_bid_ask">>;
        false -> <<"mark_price_fallback">>
    end.

executable_price_gap(LongLeg, ShortLeg, LongPrice, ShortPrice, Mid) ->
    LongAsk = f(maps:get(ask, LongLeg, 0), 0),
    ShortBid = f(maps:get(bid, ShortLeg, 0), 0),
    case LongAsk > 0 andalso ShortBid > 0 andalso Mid > 0 andalso LongPrice > 0 andalso ShortPrice > 0 of
        true -> (ShortPrice - LongPrice) / Mid;
        false -> 0.0
    end.

choose_positive(Value, _Fallback) when Value > 0 -> Value;
choose_positive(_Value, Fallback) -> Fallback.

short_cycle_positive(LongLeg, ShortLeg) ->
    LI = normalize_window(f(maps:get(funding_interval_hours, LongLeg, 8), 8)),
    SI = normalize_window(f(maps:get(funding_interval_hours, ShortLeg, 8), 8)),
    case abs(LI - SI) < 0.001 of
        true -> true;
        false when LI < SI -> f(maps:get(funding_rate, LongLeg, 0), 0) < 0;
        false -> f(maps:get(funding_rate, ShortLeg, 0), 0) > 0
    end.

next_settlement_return(LongLeg, ShortLeg) ->
    LongPNL = -f(maps:get(funding_rate, LongLeg, 0), 0),
    ShortPNL = f(maps:get(funding_rate, ShortLeg, 0), 0),
    LongNext = maps:get(next_funding_time, LongLeg, 0),
    ShortNext = maps:get(next_funding_time, ShortLeg, 0),
    SameWindow = LongNext > 0 andalso ShortNext > 0 andalso abs(LongNext - ShortNext) =< 60000,
    case SameWindow of
        true ->
            LongPNL + ShortPNL;
        false when LongNext > 0, ShortNext > 0, LongNext < ShortNext ->
            max(0.0, LongPNL);
        false when LongNext > 0, ShortNext > 0, ShortNext < LongNext ->
            max(0.0, ShortPNL);
        false ->
            LongPNL + ShortPNL
    end.

can_collect_short_before_long(LongLeg, ShortLeg) ->
    S = maps:get(next_funding_time, ShortLeg, 0),
    L = maps:get(next_funding_time, LongLeg, 0),
    S > 0 andalso L > 0 andalso S + 60000 < L.

classify(FundingEdge, PriceGap, _MinFunding, _MinGap) ->
    HasFunding = FundingEdge > 0,
    HasGap = PriceGap > 0,
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

max_position_usdt(Req, LongLeg, ShortLeg) ->
    Capital = f(maps:get(capital_usdt, Req, 10000), 10000),
    Pct = f(maps:get(max_position_pct, Req, 0.1), 0.1),
    Margin = f(maps:get(execution_notional_usdt, Req, 200), 200),
    Leverage = max(1.0, f(maps:get(paper_leverage, Req, 10), 10)),
    MarginCap = case Pct > 0 of
        true -> Capital * Pct;
        false -> Margin
    end,
    Target = max(0.0, min(Margin, MarginCap) * Leverage),
    apply_leg_notional_limits(Target, LongLeg, ShortLeg).

apply_leg_notional_limits(Target, LongLeg, ShortLeg) ->
    Limits = [f(maps:get(max_single_order_usdt, Leg, 0), 0) || Leg <- [LongLeg, ShortLeg],
                                                            f(maps:get(max_single_order_usdt, Leg, 0), 0) > 0],
    case Limits of
        [] -> Target;
        _ -> min(Target, lists:min(Limits))
    end.

normalize_window(V) when V =< 0 -> 8;
normalize_window(V) when V > 24 -> max(1, V / 3600);
normalize_window(V) -> max(1, V).

delist_risk(Leg) ->
    T = maps:get(delist_time, Leg, 0),
    T > 0 andalso T - arbiguard_util:now_ms() =< ?DELIST_RISK_MS.

f(V, D) ->
    arbiguard_util:to_float(V, D).

effective_fee_rate(Leg) ->
    Gross = f(maps:get(taker_fee_rate, Leg, 0.0005), 0.0005),
    Rebate = clamp01(f(maps:get(fee_rebate_rate, Leg, 0.0), 0.0)),
    Gross * (1.0 - Rebate).

clamp01(V) when V < 0 -> 0.0;
clamp01(V) when V > 1 -> 1.0;
clamp01(V) -> V.

lower_bin(V) ->
    string:lowercase(arbiguard_util:to_binary(V)).
