-module(arbiguard_json).

-export([decode/1, encode/1]).

decode(<<>>) ->
    #{};
decode(Bin) when is_binary(Bin) ->
    normalize(json:decode(Bin));
decode(List) when is_list(List) ->
    decode(unicode:characters_to_binary(List)).

encode(Term) ->
    iolist_to_binary(json:encode(denormalize(Term))).

normalize(Map) when is_map(Map) ->
    maps:from_list([{key_to_atom(K), normalize(V)} || {K, V} <- maps:to_list(Map)]);
normalize(List) when is_list(List) ->
    [normalize(V) || V <- List];
normalize(Value) ->
    Value.

denormalize(Map) when is_map(Map) ->
    maps:from_list([{key_to_binary(K), denormalize(V)} || {K, V} <- maps:to_list(Map)]);
denormalize(List) when is_list(List) ->
    [denormalize(V) || V <- List];
denormalize(Value) when is_binary(Value) ->
    safe_utf8_binary(Value);
denormalize(Value) ->
    Value.

key_to_atom(K) when is_atom(K) ->
    K;
key_to_atom(K) when is_binary(K) ->
    try binary_to_existing_atom(K, utf8)
    catch
        error:badarg -> binary_to_atom(K, utf8)
    end;
key_to_atom(K) when is_list(K) ->
    key_to_atom(unicode:characters_to_binary(K)).

key_to_binary(K) when is_atom(K) ->
    atom_to_binary(K, utf8);
key_to_binary(K) when is_binary(K) ->
    safe_utf8_binary(K);
key_to_binary(K) when is_list(K) ->
    safe_utf8_binary(unicode:characters_to_binary(K)).

safe_utf8_binary(Bin) when is_binary(Bin) ->
    case unicode:characters_to_binary(Bin, utf8, utf8) of
        Clean when is_binary(Clean) ->
            Clean;
        _ ->
            unicode:characters_to_binary(io_lib:format("~p", [Bin]))
    end.
