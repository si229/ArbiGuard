-module(arbiguard_util).

-export([to_float/2, to_int/2, to_binary/1, now_ms/0, clamp_positive/2, min_positive/2]).

to_float(V, _Default) when is_float(V) -> V;
to_float(V, _Default) when is_integer(V) -> V * 1.0;
to_float(V, Default) when is_binary(V) ->
    try binary_to_float(V)
    catch
        _:_ ->
            try binary_to_integer(V) * 1.0
            catch _:_ -> Default
            end
    end;
to_float(V, Default) when is_list(V) ->
    to_float(unicode:characters_to_binary(V), Default);
to_float(_, Default) -> Default.

to_int(V, _Default) when is_integer(V) -> V;
to_int(V, _Default) when is_float(V) -> trunc(V);
to_int(V, Default) when is_binary(V) ->
    try binary_to_integer(V)
    catch
        _:_ ->
            try trunc(binary_to_float(V))
            catch _:_ -> Default
            end
    end;
to_int(V, Default) when is_list(V) ->
    to_int(unicode:characters_to_binary(V), Default);
to_int(_, Default) -> Default.

to_binary(V) when is_binary(V) -> V;
to_binary(V) when is_atom(V) -> atom_to_binary(V, utf8);
to_binary(V) when is_list(V) -> unicode:characters_to_binary(V);
to_binary(V) when is_integer(V) -> integer_to_binary(V);
to_binary(V) when is_float(V) -> float_to_binary(V, [{decimals, 12}, compact]).

now_ms() ->
    erlang:system_time(millisecond).

clamp_positive(Value, _Fallback) when Value > 0 -> Value;
clamp_positive(_, Fallback) -> Fallback.

min_positive(A, B) when A > 0, B > 0 -> min(A, B);
min_positive(A, _B) when A > 0 -> A;
min_positive(_A, B) when B > 0 -> B;
min_positive(_, _) -> 0.
