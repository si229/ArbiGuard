-module(wz_trace).

%% 运维人员通过远程 Shell 临时跟踪函数调用；默认上限 100 条，避免无界 trace。
-export([
    trace/1,
    trace/2,
    trace/3,
    trace/4,
    trace_pid/1,
    trace_pid/2,
    trace_pid/3,
    trace_pid/4,
    trace_pid/5,
    do_trace/2,
    get_tspec/1,
    clear_all/0
]).

-define(DEFAULT_TRACE_COUNT, 100).
-define(TRACE_FILE_KEY, {?MODULE, trace_file}).
-define(TRACE_SETUP_LOCK, {?MODULE, setup}).

-type tspecs() :: [recon_trace:tspec()].
-type options() :: list().
-type fun_name() :: atom().
-type args() :: list() | '_' | arity().

-spec trace(module() | tspecs()) -> recon_trace:num_matches().
trace(Mod) when is_atom(Mod) ->
    trace(Mod, '_');
trace(TSpecs) when is_list(TSpecs) ->
    trace(TSpecs, []).

-spec trace(tspecs(), options()) -> recon_trace:num_matches();
    (module(), options()) -> recon_trace:num_matches();
    (module(), fun_name()) -> recon_trace:num_matches().
trace(TSpecs0, Options) when is_list(TSpecs0) andalso is_list(Options) ->
    do_trace([get_tspec(TSpec) || TSpec <- TSpecs0], Options);
trace(Mod, Options) when is_atom(Mod) andalso is_list(Options) ->
    trace(Mod, '_', '_', Options);
trace(Mod, Fun) when is_atom(Mod) andalso is_atom(Fun) ->
    trace(Mod, Fun, '_', []).

-spec trace(module(), fun_name(), args()) -> recon_trace:num_matches().
trace(Mod, Fun, Args) when is_atom(Mod) andalso is_atom(Fun) ->
    trace(Mod, Fun, Args, []).

-spec trace(module(), fun_name(), args(), options()) -> recon_trace:num_matches().
trace(Mod, Fun, Arity, Options)
        when is_atom(Mod) andalso is_atom(Fun) andalso is_integer(Arity) ->
    trace(Mod, Fun, lists:duplicate(Arity, '_'), Options);
trace(Mod, Fun, Args, Options) when is_atom(Mod) andalso is_atom(Fun) ->
    do_trace([{Mod, Fun, [{Args, [], [{return_trace}]}]}], Options).

-spec trace_pid(term()) -> recon_trace:num_matches().
trace_pid(PidSpec) ->
    trace_pid(PidSpec, '_').

-spec trace_pid(term(), module() | tspecs()) -> recon_trace:num_matches().
trace_pid(PidSpec, Mod) when is_atom(Mod) ->
    trace_pid(PidSpec, Mod, '_');
trace_pid(PidSpec, TSpecs) when is_list(TSpecs) ->
    trace_pid(PidSpec, TSpecs, []).

-spec trace_pid(term(), tspecs(), options()) -> recon_trace:num_matches();
    (term(), module(), options()) -> recon_trace:num_matches();
    (term(), module(), fun_name()) -> recon_trace:num_matches().
trace_pid(PidSpec, TSpecs0, Options)
        when is_list(TSpecs0) andalso is_list(Options) ->
    do_trace_pid(PidSpec, [get_tspec(TSpec) || TSpec <- TSpecs0], Options);
trace_pid(PidSpec, Mod, Options) when is_atom(Mod) andalso is_list(Options) ->
    trace_pid(PidSpec, Mod, '_', '_', Options);
trace_pid(PidSpec, Mod, Fun) when is_atom(Mod) andalso is_atom(Fun) ->
    trace_pid(PidSpec, Mod, Fun, '_', []).

-spec trace_pid(term(), module(), fun_name(), args()) -> recon_trace:num_matches().
trace_pid(PidSpec, Mod, Fun, Args) when is_atom(Mod) andalso is_atom(Fun) ->
    trace_pid(PidSpec, Mod, Fun, Args, []).

-spec trace_pid(term(), module(), fun_name(), args(), options()) ->
    recon_trace:num_matches().
trace_pid(PidSpec, Mod, Fun, Arity, Options)
        when is_atom(Mod) andalso is_atom(Fun) andalso is_integer(Arity) ->
    trace_pid(PidSpec, Mod, Fun, lists:duplicate(Arity, '_'), Options);
trace_pid(PidSpec, Mod, Fun, Args, Options)
        when is_atom(Mod) andalso is_atom(Fun) ->
    do_trace_pid(PidSpec,
        [{Mod, Fun, [{Args, [], [{return_trace}]}]}], Options).

-spec do_trace_pid(term(), tspecs(), options()) -> recon_trace:num_matches().
do_trace_pid(PidSpec, TSpecs, Options) ->
    do_trace(TSpecs, [{pid, PidSpec} | Options]).

-spec do_trace(tspecs(), options()) -> recon_trace:num_matches().
do_trace(TSpecs, Options0) when is_list(TSpecs) andalso is_list(Options0) ->
    with_trace_lock(fun() -> do_trace_locked(TSpecs, Options0) end).

do_trace_locked(TSpecs, Options0) ->
    [code:ensure_loaded(Mod) || {Mod, _, _} <- TSpecs,
        Mod =/= '_' andalso is_atom(Mod)],
    ok = clear_all_locked(),
    {TraceCount, Options, TraceDevice} = prepare_options(Options0),
    try
        Matches = recon_trace:calls(
            TSpecs, TraceCount, [{scope, local} | Options]),
        ok = watch_trace_file(TraceDevice),
        Matches
    catch
        Class:Reason:Stacktrace ->
            close_trace_file(),
            erlang:raise(Class, Reason, Stacktrace)
    end.

-spec get_tspec(module() | tuple()) -> recon_trace:tspec().
get_tspec(Mod) when is_atom(Mod) ->
    {Mod, '_', [{'_', [], [{return_trace}]}]};
get_tspec({Mod, Fun}) when is_atom(Mod) andalso is_atom(Fun) ->
    {Mod, Fun, [{'_', [], [{return_trace}]}]};
get_tspec({Mod, Fun, Arity})
        when is_atom(Mod) andalso is_atom(Fun) andalso is_integer(Arity) ->
    {Mod, Fun, [{lists:duplicate(Arity, '_'), [], [{return_trace}]}]};
get_tspec({_Mod, _Fun, [{_, _, [{return_trace}]} | _]} = TSpec) ->
    TSpec;
get_tspec({Mod, Fun, Args})
        when is_atom(Mod) andalso is_atom(Fun) andalso is_list(Args) ->
    {Mod, Fun, [{Args, [], [{return_trace}]}]}.

-spec clear_all() -> ok.
clear_all() ->
    with_trace_lock(fun clear_all_locked/0).

clear_all_locked() ->
    ok = recon_trace:clear(),
    close_trace_file().

with_trace_lock(Fun) ->
    %% recon 使用全局注册名；串行 setup/clear，确保文件 watcher 绑定本次 formatter。
    global:trans({?TRACE_SETUP_LOCK, self()}, Fun, [node()]).

prepare_options(Options0) ->
    TraceCount0 = proplists:get_value(
        trace_count, Options0, ?DEFAULT_TRACE_COUNT),
    TraceCount = validate_trace_count(TraceCount0),
    Options1 = proplists:delete(trace_count, Options0),
    case proplists:get_value(to_file, Options1) of
        undefined ->
            {TraceCount, Options1, undefined};
        true ->
            {TraceOptions, TraceDevice} =
                open_trace_file("trace_msg.log", Options1),
            {TraceCount, TraceOptions, TraceDevice};
        Filename ->
            {TraceOptions, TraceDevice} = open_trace_file(Filename, Options1),
            {TraceCount, TraceOptions, TraceDevice}
    end.

validate_trace_count(Count) when is_integer(Count) andalso Count > 0 ->
    Count;
validate_trace_count({Count, WindowMs})
        when is_integer(Count) andalso Count > 0 andalso
             is_integer(WindowMs) andalso WindowMs > 0 ->
    {Count, WindowMs};
validate_trace_count(Count) ->
    error({invalid_trace_count, Count}).

open_trace_file(Filename, Options0) ->
    case file:open(Filename, [write]) of
        {ok, Device} ->
            put(?TRACE_FILE_KEY, Device),
            {[{io_server, Device} | proplists:delete(to_file, Options0)],
                Device};
        {error, Reason} ->
            error({trace_file_open_failed, Filename, Reason})
    end.

watch_trace_file(undefined) ->
    ok;
watch_trace_file(Device) ->
    case whereis(recon_trace_formatter) of
        Formatter when is_pid(Formatter) ->
            _ = spawn(fun() -> close_file_after_trace(Formatter, Device) end),
            ok;
        undefined ->
            %% 极热函数可能在 calls/3 返回前已达到上限；此时直接收口文件即可。
            close_trace_file()
    end.

close_file_after_trace(Formatter, Device) ->
    Monitor = erlang:monitor(process, Formatter),
    receive
        {'DOWN', Monitor, process, Formatter, _Reason} ->
            _ = file:close(Device),
            ok
    end.

close_trace_file() ->
    case erase(?TRACE_FILE_KEY) of
        undefined ->
            ok;
        Device ->
            _ = file:close(Device),
            ok
    end.
