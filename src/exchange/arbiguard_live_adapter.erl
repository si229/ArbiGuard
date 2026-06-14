-module(arbiguard_live_adapter).

-export([test_order/2]).

test_order(Payload, TokenConfig) ->
    Exchange = norm_exchange(maps:get(exchange, Payload, <<"">>)),
    case validate_live_payload(Payload, TokenConfig) of
        ok ->
            dispatch_test_order(Exchange, Payload, TokenConfig);
        Error ->
            Error
    end.

dispatch_test_order(Exchange, _Payload, _TokenConfig) ->
    #{ok => false,
      status => <<"rejected">>,
      reason => <<"live_adapter_not_implemented">>,
      exchange => Exchange,
      detail => <<"Signed real-order adapter is not implemented yet; no request was sent to the exchange.">>}.

validate_live_payload(Payload, TokenConfig) ->
    case {has_token(TokenConfig), positive(maps:get(quantity, Payload, 0)), valid_action(maps:get(action, Payload, <<"">>))} of
        {false, _, _} ->
            #{ok => false, status => <<"rejected">>, reason => <<"live_token_not_configured">>};
        {_, false, _} ->
            #{ok => false, status => <<"rejected">>, reason => <<"quantity_required_for_live_order">>};
        {_, _, false} ->
            #{ok => false, status => <<"rejected">>, reason => <<"unsupported_live_action">>};
        _ ->
            ok
    end.

has_token(TokenConfig) when is_map(TokenConfig) ->
    ApiKey = maps:get(api_key, TokenConfig, maps:get(<<"api_key">>, TokenConfig, <<"">>)),
    Secret = maps:get(api_secret, TokenConfig, maps:get(<<"api_secret">>, TokenConfig, <<"">>)),
    ApiKey =/= <<"">> andalso Secret =/= <<"">>;
has_token(_) ->
    false.

positive(V) when is_number(V) ->
    V > 0;
positive(_) ->
    false.

valid_action(<<"open">>) -> true;
valid_action(<<"close">>) -> true;
valid_action(<<"cancel">>) -> true;
valid_action(_) -> false.

norm_exchange(V) ->
    string:lowercase(arbiguard_util:to_binary(V)).
