%%%
%%% Genlib

-module(genlib_retry).

-export([linear/2]).
-export([exponential/3]).
-export([exponential/4]).
-export([intervals/1]).

-export([timecap/2]).

-export([next_step/1]).

-export([new_strategy/1]).
-export([new_strategy/3]).

-export_type([strategy/0]).
-export_type([policy/0]).

-type retries_num() :: pos_integer() | infinity.
-type max_retries() :: retries_num() | {max_total_timeout, pos_integer()}.
-type wait_time() :: pos_integer().
-type jitter_epsilon() :: pos_integer().
-type step_timeout() :: wait_time() | {jitter, wait_time(), jitter_epsilon()}.

-type policy() ::
    {linear, Retries :: max_retries(), Timeout :: step_timeout()}
    | {exponential, Retries :: max_retries(), Factor :: number(), Timeout :: step_timeout()}
    | {exponential, Retries :: max_retries(), Factor :: number(), Timeout :: step_timeout(), MaxTimeout :: timeout()}
    | {intervals, Array :: list(step_timeout())}
    | {timecap, timeout(), policy()}.

-opaque strategy() ::
    {linear, Retries :: retries_num(), Timeout :: step_timeout()}
    | {exponential, Retries :: retries_num(), Factor :: number(), Timeout :: step_timeout(), MaxTimeout :: timeout()}
    | {array, Array :: list(step_timeout())}
    | {timecap, Start :: pos_integer(), Finish :: pos_integer(), strategy()}
    | finish.

%%

-define(IS_POSINT(V), (is_integer(V) andalso V > 0)).
-define(IS_RETRIES(V), (V =:= infinity orelse ?IS_POSINT(V))).
-define(IS_MAX_TOTAL_TIMEOUT(V), (is_integer(V) andalso V >= 0)).

-spec linear(retries_num() | {max_total_timeout, pos_integer()}, step_timeout()) -> strategy().
linear(Retries, Timeout) when
    ?IS_RETRIES(Retries) andalso
        ?IS_POSINT(Timeout)
->
    {linear, Retries, Timeout};
linear({max_total_timeout, MaxTotalTimeout} = Retries, Timeout) when
    ?IS_MAX_TOTAL_TIMEOUT(MaxTotalTimeout) andalso
        ?IS_POSINT(Timeout)
->
    {linear, compute_retries(linear, Retries, Timeout), Timeout};
linear({max_total_timeout, MaxTotalTimeout} = Retries, {jitter, Timeout, Epsilon}) when
    ?IS_MAX_TOTAL_TIMEOUT(MaxTotalTimeout) andalso
        ?IS_POSINT(Timeout) andalso
        ?IS_POSINT(Epsilon)
->
    {linear, compute_retries(linear, Retries, Timeout), {jitter, Timeout, Epsilon}};
linear(Retries, {jitter, Timeout, Epsilon}) when
    ?IS_POSINT(Timeout) andalso
        ?IS_POSINT(Epsilon)
->
    {linear, Retries, {jitter, Timeout, Epsilon}}.

-spec exponential(retries_num() | {max_total_timeout, pos_integer()}, number(), step_timeout()) -> strategy().
exponential(Retries, Factor, Timeout) when
    ?IS_POSINT(Timeout) andalso
        Factor > 0
->
    exponential(Retries, Factor, Timeout, infinity);
exponential(Retries, Factor, {jitter, Timeout, Epsilon}) when
    ?IS_POSINT(Timeout) andalso
        ?IS_POSINT(Epsilon) andalso
        Factor > 0
->
    exponential(Retries, Factor, {jitter, Timeout, Epsilon}, infinity).

-spec exponential(retries_num() | {max_total_timeout, pos_integer()}, number(), step_timeout(), timeout()) ->
    strategy().
exponential(Retries, Factor, Timeout, MaxTimeout) when
    ?IS_RETRIES(Retries) andalso
        ?IS_POSINT(Timeout) andalso
        Factor > 0 andalso
        (MaxTimeout =:= infinity orelse ?IS_POSINT(MaxTimeout))
->
    {exponential, Retries, Factor, Timeout, MaxTimeout};
exponential(Retries, Factor, {jitter, Timeout, Epsilon}, MaxTimeout) when
    ?IS_RETRIES(Retries) andalso
        ?IS_POSINT(Timeout) andalso
        ?IS_POSINT(Epsilon) andalso
        Factor > 0 andalso
        (MaxTimeout =:= infinity orelse ?IS_POSINT(MaxTimeout))
->
    {exponential, Retries, Factor, {jitter, Timeout, Epsilon}, MaxTimeout};
exponential({max_total_timeout, MaxTotalTimeout} = Retries, Factor, Timeout, MaxTimeout) when
    ?IS_MAX_TOTAL_TIMEOUT(MaxTotalTimeout) andalso
        ?IS_POSINT(Timeout) andalso
        Factor > 0 andalso
        (MaxTimeout =:= infinity orelse ?IS_POSINT(MaxTimeout))
->
    {exponential, compute_retries(exponential, Retries, {Factor, Timeout, MaxTimeout}), Factor, Timeout, MaxTimeout};
exponential({max_total_timeout, MaxTotalTimeout} = Retries, Factor, {jitter, Timeout, Epsilon}, MaxTimeout) when
    ?IS_MAX_TOTAL_TIMEOUT(MaxTotalTimeout) andalso
        ?IS_POSINT(Timeout) andalso
        ?IS_POSINT(Epsilon) andalso
        Factor > 0 andalso
        (MaxTimeout =:= infinity orelse ?IS_POSINT(MaxTimeout))
->
    {exponential, compute_retries(exponential, Retries, {Factor, Timeout, MaxTimeout}), Factor,
        {jitter, Timeout, Epsilon}, MaxTimeout}.

-spec intervals([pos_integer(), ...]) -> strategy().
intervals([{jitter, Timeout, Epsilon} | _] = Array) when
    ?IS_POSINT(Timeout) andalso
        ?IS_POSINT(Epsilon)
->
    {array, Array};
intervals([Timeout | _] = Array) when ?IS_POSINT(Timeout) ->
    {array, Array}.

-spec timecap(MaxTimeToSpend :: timeout(), strategy()) -> strategy().
timecap(infinity, Strategy) ->
    Strategy;
timecap(MaxTimeToSpend, Strategy) when ?IS_POSINT(MaxTimeToSpend) ->
    Now = now_ms(),
    {timecap, Now, Now + MaxTimeToSpend, Strategy};
timecap(_, _Strategy) ->
    finish.

%%

-spec next_step(strategy()) -> {wait, Timeout :: pos_integer(), strategy()} | finish.
next_step({linear, Retries, {jitter, Timeout, Epsilon}}) when Retries > 0 ->
    Jitter = calc_jitter(Epsilon),
    {wait, Timeout + Jitter, {linear, release_retry(Retries), {jitter, Timeout, Epsilon}}};
next_step({linear, Retries, Timeout}) when Retries > 0 ->
    {wait, Timeout, {linear, release_retry(Retries), Timeout}};
next_step({linear, _, _}) ->
    finish;
next_step({exponential, Retries, Factor, {jitter, Timeout, Epsilon}, MaxTimeout}) when Retries > 0 ->
    Jitter = calc_jitter(Epsilon),
    NewTimeout = min(round(Timeout * Factor), MaxTimeout),
    {wait, Timeout + Jitter, {exponential, release_retry(Retries), Factor, {jitter, NewTimeout, Epsilon}, MaxTimeout}};
next_step({exponential, Retries, Factor, Timeout, MaxTimeout}) when Retries > 0 ->
    NewTimeout = min(round(Timeout * Factor), MaxTimeout),
    {wait, Timeout, {exponential, release_retry(Retries), Factor, NewTimeout, MaxTimeout}};
next_step({exponential, _, _, _, _}) ->
    finish;
next_step({array, []}) ->
    finish;
next_step({array, [{jitter, Timeout, Epsilon} | Remain]}) ->
    Jitter = calc_jitter(Epsilon),
    {wait, Timeout + Jitter, {array, Remain}};
next_step({array, [Timeout | Remain]}) ->
    {wait, Timeout, {array, Remain}};
next_step({timecap, Last, Deadline, Strategy}) ->
    Now = now_ms(),
    case next_step(Strategy) of
        {wait, Cooldown, NextStrategy} ->
            case max(0, Cooldown - (Now - Last)) of
                Timeout when Now + Timeout > Deadline ->
                    finish;
                Timeout ->
                    {wait, Timeout, {timecap, Now + Timeout, Deadline, NextStrategy}}
            end;
        finish ->
            finish
    end;
next_step(finish) ->
    finish;
next_step(Strategy) ->
    error(badarg, [Strategy]).

%

-spec new_strategy(policy()) -> strategy().
new_strategy({linear, Retries, Timeout}) ->
    linear(Retries, Timeout);
new_strategy({exponential, Retries, Factor, Timeout}) ->
    exponential(Retries, Factor, Timeout);
new_strategy({exponential, Retries, Factor, Timeout, MaxTimeout}) ->
    exponential(Retries, Factor, Timeout, MaxTimeout);
new_strategy({intervals, Array}) ->
    intervals(Array);
new_strategy({timecap, Timeout, Policy}) ->
    timecap(Timeout, new_strategy(Policy));
new_strategy(BadPolicy) ->
    erlang:error(badarg, [BadPolicy]).

-spec new_strategy
    (policy(), genlib_time:ts(), non_neg_integer()) -> strategy();
    (policy(), undefined, undefined) -> strategy().
new_strategy(PolicySpec, undefined, undefined) ->
    new_strategy(PolicySpec);
new_strategy(PolicySpec, _InitialTimestamp, Attempt) ->
    %% TODO: Use InitialTimestamp
    Strategy = new_strategy(PolicySpec),
    skip_steps(Strategy, Attempt).

%

-spec compute_retries
    (
        linear,
        {max_total_timeout, non_neg_integer()},
        Timeout :: pos_integer()
    ) -> non_neg_integer();
    (
        exponential,
        {max_total_timeout, non_neg_integer()},
        {Factor :: number(), Timeout :: pos_integer(), MaxTimeout :: timeout()}
    ) -> non_neg_integer().
compute_retries(linear, {max_total_timeout, MaxTotalTimeout}, Timeout) ->
    trunc(MaxTotalTimeout / Timeout);
compute_retries(exponential, {max_total_timeout, MaxTotalTimeout}, {Factor, Timeout, MaxTimeout}) when
    MaxTimeout =< Timeout; Factor =:= 1
->
    trunc(MaxTotalTimeout / min(Timeout, MaxTimeout));
compute_retries(exponential, {max_total_timeout, MaxTotalTimeout}, {Factor, Timeout, MaxTimeout}) ->
    %First element
    B1 = Timeout,
    %Common ratio
    Q = Factor,
    M =
        case MaxTimeout of
            infinity ->
                % Bi can't be bigger than MaxTimeout
                infinity;
            _ ->
                % A threshold after which Bi changes to MaxTimeout
                trunc(math:log(MaxTimeout / B1) / math:log(Q) + 1)
        end,
    % A number of iteration we would need to achieve MaxTotalTimeout
    N = trunc(math:log(MaxTotalTimeout * (Q - 1) / B1 + 1) / math:log(Q)),
    case N < M of
        true ->
            N;
        false ->
            trunc((MaxTotalTimeout - B1 * (math:pow(Q, M - 1) - 1) / (Q - 1) + (M - 1) * MaxTimeout) / MaxTimeout)
    end.

release_retry(infinity) ->
    infinity;
release_retry(N) ->
    N - 1.

now_ms() ->
    genlib_time:ticks() div 1000.

calc_jitter(Epsilon) ->
    rand:uniform(2 * Epsilon + 1) - Epsilon - 1.

-spec skip_steps(strategy(), non_neg_integer()) -> strategy().
skip_steps(Strategy, 0) ->
    Strategy;
skip_steps(Strategy, N) when N > 0 ->
    NewStrategy =
        case next_step(Strategy) of
            {wait, _Timeout, NextStrategy} ->
                NextStrategy;
            finish = NextStrategy ->
                NextStrategy
        end,
    skip_steps(NewStrategy, N - 1).
