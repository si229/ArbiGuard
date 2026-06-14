-module(arbiguard_live_order).

-export([submit/2, submit_open/2, submit_close/2, cancel/2]).

submit(Req, Order) ->
    Action = maps:get(action, Order, maps:get(order_action, Order, <<"submit">>)),
    submit_with_action(Action, Req, Order).

submit_open(Req, Order) ->
    submit_with_action(<<"open">>, Req, Order).

submit_close(Req, Order) ->
    submit_with_action(<<"close">>, Req, Order).

cancel(Req, Order) ->
    submit_with_action(<<"cancel">>, Req, Order).

submit_with_action(Action0, Req0, Order0) ->
    Req = normalize_req_account(Req0, Order0),
    AccountID = maps:get(account_id, Req),
    Order = Order0#{id => maps:get(id, Order0, order_id(Order0)),
                    action => normalize_action(Action0),
                    submitted_at => arbiguard_util:now_ms(),
                    account_id => AccountID,
                    account_mode => <<"live">>,
                    requested_notional => maps:get(target_notional, Order0, maps:get(requested_notional, Order0, 0.0)),
                    filled_notional => maps:get(filled_notional, Order0, 0.0)},
    case live_enabled(Req) of
        false ->
            rejected(Order, <<"live_account_disabled">>);
        true ->
            case required_tokens(Req, AccountID, Order) of
                {ok, Exchanges} ->
                    accept_pending_adapter(Order, Exchanges);
                {error, Reason, ExchangeID} ->
                    rejected(Order#{exchange => ExchangeID}, Reason)
            end
    end.

accept_pending_adapter(Order, Exchanges) ->
    maps:without([owner_pid, req, opportunity, position],
                 Order#{status => <<"awaiting_fill">>,
                        adapter_status => <<"pending_adapter">>,
                        exchange_submit => <<"pending_real_adapter">>,
                        token_configured_exchanges => Exchanges,
                        reason => <<"waiting_exchange_fill_report">>}).

rejected(Order, Reason) ->
    maps:without([owner_pid, req, opportunity, position],
                 Order#{status => <<"rejected">>,
                        reason => Reason,
                        submitted_at => maps:get(submitted_at, Order, arbiguard_util:now_ms())}).

required_tokens(Req, AccountID, Order) ->
    Exchanges = required_exchanges(Order),
    required_tokens(Req, AccountID, Exchanges, []).

required_tokens(_Req, _AccountID, [], Acc) ->
    {ok, lists:reverse(Acc)};
required_tokens(Req, AccountID, [ExchangeID | Rest], Acc) ->
    case token_for_exchange(Req, AccountID, ExchangeID) of
        undefined -> {error, <<"live_token_not_configured">>, ExchangeID};
        Token when is_map(Token) ->
            case has_token(Token) of
                true -> required_tokens(Req, AccountID, Rest, [ExchangeID | Acc]);
                false -> {error, <<"live_token_not_configured">>, ExchangeID}
            end;
        _ -> {error, <<"live_token_not_configured">>, ExchangeID}
    end.

token_for_exchange(Req, AccountID, ExchangeID) ->
    Tokens = maps:get(exchange_tokens, Req, #{}),
    case maps:get(ExchangeID, Tokens, undefined) of
        undefined -> arbiguard_exchange_account:get_token(AccountID, ExchangeID);
        Token -> Token
    end.

required_exchanges(Order) ->
    lists:usort([E || E <- [norm_exchange(maps:get(exchange, Order, <<"">>)),
                            norm_exchange(maps:get(long_exchange, Order, <<"">>)),
                            norm_exchange(maps:get(short_exchange, Order, <<"">>))],
                      E =/= <<"">>]).

has_token(Token) ->
    ApiKey = maps:get(api_key, Token, maps:get(<<"api_key">>, Token, <<"">>)),
    Secret = maps:get(api_secret, Token, maps:get(<<"api_secret">>, Token, <<"">>)),
    ApiKey =/= <<"">> andalso Secret =/= <<"">>.

live_enabled(Req) ->
    case maps:get(live_enabled, Req, true) of
        false -> false;
        <<"false">> -> false;
        0 -> false;
        _ -> true
    end.

normalize_req_account(Req0, Order) ->
    Req = arbiguard_calc:normalize_request(Req0),
    Mode = norm_mode(maps:get(account_mode, Req, maps:get(account_mode, Order, <<"live">>))),
    DefaultID = case Mode of <<"live">> -> <<"live-main">>; _ -> <<"paper-main">> end,
    Req#{account_mode => Mode,
         account_id => norm_account_id(maps:get(account_id, Req, maps:get(account_id, Order, DefaultID)))}.

order_id(Order) ->
    Symbol = maps:get(symbol, Order, <<"UNKNOWN">>),
    Long = maps:get(long_exchange, Order, <<"long">>),
    Short = maps:get(short_exchange, Order, <<"short">>),
    <<Symbol/binary, "|", Long/binary, "|", Short/binary, "|", (integer_to_binary(arbiguard_util:now_ms()))/binary>>.

normalize_action(V) ->
    case string:lowercase(arbiguard_util:to_binary(V)) of
        <<"open">> -> <<"open">>;
        <<"close">> -> <<"close">>;
        <<"cancel">> -> <<"cancel">>;
        _ -> <<"submit">>
    end.

norm_mode(live) -> <<"live">>;
norm_mode(paper) -> <<"paper">>;
norm_mode(V) ->
    case string:lowercase(arbiguard_util:to_binary(V)) of
        <<"live">> -> <<"live">>;
        _ -> <<"paper">>
    end.

norm_exchange(V) ->
    string:lowercase(arbiguard_util:to_binary(V)).

norm_account_id(V) ->
    case arbiguard_util:to_binary(V) of
        <<"">> -> <<"live-main">>;
        B -> B
    end.
