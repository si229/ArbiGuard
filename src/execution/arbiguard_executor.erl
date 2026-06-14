-module(arbiguard_executor).

-export([notify_opportunities/2, submit_order/2, reset/0, snapshot/0]).

notify_opportunities(Req, Result) ->
    case catch arbiguard_account_manager:notify_opportunities(Req, Result) of
        {'EXIT', _} -> arbiguard_open_executor:notify_opportunities(Req, Result);
        Other -> Other
    end.

submit_order(Req, Opportunity) ->
    case catch arbiguard_account_manager:submit_order(Req, Opportunity) of
        {'EXIT', _} -> arbiguard_open_executor:submit_order(Req, Opportunity);
        Other -> Other
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
