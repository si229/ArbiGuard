-module(arbiguard_executor).

-export([notify_opportunities/2, submit_order/2, reset/0, snapshot/0]).

notify_opportunities(Req, Result) ->
    arbiguard_ets:put_opportunities(arbiguard_calc:normalize_request(Req),
                                    maps:get(opportunities, Result, [])).

submit_order(Req, Opportunity) ->
    Req1 = normalize_req_account(Req),
    Executor = open_executor_for_req(Req1),
    case whereis(Executor) of
        undefined ->
            #{ok => false,
              reason => <<"open_executor_not_running">>,
              account_id => maps:get(account_id, Req1),
              account_mode => maps:get(account_mode, Req1),
              executor => atom_to_binary(Executor, utf8)};
        _ ->
            arbiguard_open_executor:submit_order(Executor, Req1, Opportunity)
    end.

reset() ->
    Open = safe(fun arbiguard_open_executor:reset/0),
    Close = safe(fun arbiguard_close_executor:reset/0),
    #{ok => true, open_executor => Open, close_executor => Close}.

snapshot() ->
    Open = safe(fun arbiguard_open_executor:snapshot/0),
    Close = safe(fun arbiguard_close_executor:snapshot/0),
    Accounts = safe(fun arbiguard_account_manager:snapshot/0),
    #{open_executor => Open,
      close_executor => Close,
      accounts => Accounts,
      orders => maps:get(orders, Open, []),
      close_orders => maps:get(orders, Close, []),
      last_opportunities => maps:get(last_opportunities, Open, []),
      last_notify => maps:get(last_notify, Open, 0)}.

safe(Fun) ->
    try Fun()
    catch Class:Reason ->
        #{error => unicode:characters_to_binary(io_lib:format("~p:~p", [Class, Reason]))}
    end.

normalize_req_account(Req0) ->
    Req = arbiguard_calc:normalize_request(Req0),
    Mode = norm_mode(maps:get(account_mode, Req, <<"paper">>)),
    DefaultID = case Mode of <<"live">> -> <<"live-main">>; _ -> <<"paper-main">> end,
    Req#{account_mode => Mode,
         account_id => norm_account_id(maps:get(account_id, Req, DefaultID))}.

open_executor_for_req(Req) ->
    AccountID = maps:get(account_id, Req, <<"paper-main">>),
    case {maps:get(account_mode, Req, <<"paper">>), AccountID} of
        {<<"paper">>, <<"paper-main">>} -> arbiguard_open_executor;
        _ -> arbiguard_open_executor:account_name(AccountID)
    end.

norm_mode(live) -> <<"live">>;
norm_mode(paper) -> <<"paper">>;
norm_mode(V) ->
    case string:lowercase(arbiguard_util:to_binary(V)) of
        <<"live">> -> <<"live">>;
        _ -> <<"paper">>
    end.

norm_account_id(V) ->
    case arbiguard_util:to_binary(V) of
        <<"">> -> <<"paper-main">>;
        B -> B
    end.
