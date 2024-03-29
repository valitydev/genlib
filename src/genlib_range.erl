-module(genlib_range).

%% @doc Module for working with number sequences (like lists:seq/2,3),
%% but more efficiently (i.e. without generating a list of numbers)
%%
%% Supports both forward- and backward-ranges (increasing and decreasing respectively)

-export([map/2]).
-export([foldl/3]).
-export([to_list/1]).

-type bound() :: integer().
-type step() :: neg_integer() | pos_integer().
-type t() :: {bound(), bound()} | {bound(), bound(), step()}.

-define(IS_RANGE(R),
    ((is_integer(element(1, R))) andalso
        (is_integer(element(2, R))) andalso
        (?IS_SIMPLE_RANGE(R) orelse ?IS_RANGE_WITH_STEP(R)))
).

-define(IS_SIMPLE_RANGE(R),
    (tuple_size(R) == 2)
).

-define(IS_RANGE_WITH_STEP(R),
    (tuple_size(R) == 3 andalso
        is_integer(element(3, R)) andalso
        element(3, R) /= 0)
).

%% @doc Map over range
-spec map(fun((integer()) -> T), t()) -> [T].
map(Fun0, Range) when is_function(Fun0, 1) ->
    Fun1 = fun(Idx, Acc) ->
        [Fun0(Idx) | Acc]
    end,
    lists:reverse(foldl(Fun1, [], Range));
map(_, _) ->
    error(badarg).

%% @doc Fold over range from starting from the first boundary
-spec foldl(fun((integer(), T) -> T), T, t()) -> T.
foldl(Fun, Acc, Range) when is_function(Fun, 2), ?IS_RANGE(Range) ->
    {From, To, Step} = to_extended_range(Range),
    do_foldl(Fun, Acc, From, To, Step);
foldl(_, _, _) ->
    error(badarg).

%% @doc Convert range to list
%% Somewhat similar to lists:seq/2,3, but covers all possible valid variations of arguments
-spec to_list(t()) -> [integer()].
to_list(Range) ->
    {From, To, Step} = to_extended_range(Range),
    if
        From < To, Step < 0 -> [];
        From > To, Step > 0 -> [];
        true -> lists:seq(From, To, Step)
    end.

%%
%% Internals
%%

do_foldl(_Fun, Acc, From, To, Step) when (From > To andalso Step > 0) -> Acc;
do_foldl(_Fun, Acc, From, To, Step) when (From < To andalso Step < 0) -> Acc;
do_foldl(Fun, Acc, From, To, Step) -> do_foldl(Fun, Fun(From, Acc), From + Step, To, Step).

to_extended_range({From, To}) ->
    {From, To, 1};
to_extended_range({_From, _To, _Step} = Range) ->
    Range.
