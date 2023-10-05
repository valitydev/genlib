%%%
%%% Genlib

-module(genlib_map).

%%

-export([get/2]).
-export([get/3]).
-export([deepput/3]).
-export([mget/2]).
-export([foreach/2]).
-export([truemap/2]).
-export([compact/1]).
-export([atomize/1]).
-export([atomize/2]).
-export([binarize/1]).
-export([binarize/2]).
-export([diff/2]).
-export([fold_while/3]).
-export([search/2]).
-export([zipfold/4]).
-export([flatten_join/2]).

%%

-spec get(any(), map()) -> undefined | term().
get(Key, Map) ->
    get(Key, Map, undefined).

-spec get(any(), map(), Default) -> Default | term() when Default :: term().
get(Key, Map = #{}, Default) ->
    case maps:find(Key, Map) of
        {ok, Value} ->
            Value;
        _ ->
            Default
    end.

-spec mget([Key | {Key, Default}], map()) -> [Default | term()] when
    Key :: atom() | binary() | number(),
    Default :: term().
mget([{Key, Default} | Rest], Map) ->
    [get(Key, Map, Default) | mget(Rest, Map)];
mget([Key | Rest], Map) ->
    [get(Key, Map) | mget(Rest, Map)];
mget([], _Map) ->
    [].

-spec deepput(KeyPath :: [term()], Value :: term(), map()) -> map().
deepput([Key], Value, Map) ->
    maps:put(Key, Value, Map);
deepput([Key | Rest], Value, Map) ->
    maps:put(Key, deepput(Rest, Value, get(Key, Map, #{})), Map).

-spec truemap(Function, #{K1 => V1}) -> #{K2 => V2} when
    Function :: fun((K1, V1) -> {K2, V2}),
    K1 :: any(),
    K2 :: any(),
    V1 :: any(),
    V2 :: any().
truemap(F, Map = #{}) ->
    maps:fold(
        fun(K, V, M) ->
            {Kn, Vn} = F(K, V),
            maps:put(Kn, Vn, M)
        end,
        #{},
        Map
    ).

-spec foreach(Function, map()) -> ok when Function :: fun((Key :: any(), Value :: any()) -> any()).
foreach(F, Map = #{}) ->
    maps:fold(
        fun(K, V, _) ->
            F(K, V),
            ok
        end,
        ok,
        Map
    ).

-spec compact(map()) -> map().
compact(Map = #{}) ->
    maps:fold(
        fun
            (K, undefined, M) -> maps:remove(K, M);
            (_, _, M) -> M
        end,
        Map,
        Map
    ).

-spec atomize(#{binary() => any()}) -> #{atom() => any()}.
atomize(Map) ->
    truemap(fun(K, V) -> {binary_to_atom(K, utf8), V} end, Map).

-spec binarize(#{atom() => any()}) -> #{binary() => any()}.
binarize(Map) ->
    truemap(fun(K, V) -> {atom_to_binary(K, utf8), V} end, Map).

-spec atomize(Map, pos_integer() | infinity) -> AtomicMap when
    Map :: #{binary() => Map | term()},
    AtomicMap :: #{atom() | binary() => AtomicMap | term()}.
atomize(Map, 1) ->
    atomize(Map);
atomize(Map, N) ->
    atomize(
        maps:map(
            fun
                (_, V) when is_map(V) -> atomize(V, decrement(N));
                (_, V) -> V
            end,
            Map
        )
    ).

-spec binarize(AtomicMap, pos_integer() | infinity) -> Map when
    Map :: #{binary() => Map | term()},
    AtomicMap :: #{atom() | binary() => AtomicMap | term()}.
binarize(Map, 1) ->
    binarize(Map);
binarize(Map, N) ->
    binarize(
        maps:map(
            fun
                (_, V) when is_map(V) -> binarize(V, decrement(N));
                (_, V) -> V
            end,
            Map
        )
    ).

decrement(infinity) -> infinity;
decrement(N) -> N - 1.

-spec diff(#{}, Since :: #{}) -> Diff :: #{}.
diff(Map, Since) ->
    maps:fold(
        fun(K, V, M) ->
            case get(K, M, make_ref()) of
                V -> maps:remove(K, M);
                _ -> M
            end
        end,
        Map,
        Since
    ).

%% @doc Like maps:fold, but can be stopped amid the traversal of a map.
%% Function must return {cont, NewAcc} to continue folding the list, or {halt, FinalAcc} to stop immediately.
-spec fold_while(fun((K, V, Acc) -> {cont, Acc} | {halt, Acc}), Acc, #{K => V}) -> Acc when
    K :: term(), V :: term(), Acc :: term().
fold_while(Fun, Acc, Map) when is_function(Fun, 3) and is_map(Map) ->
    do_fold_while(Fun, Acc, maps:iterator(Map)).

do_fold_while(Fun, Acc, Iter) ->
    case maps:next(Iter) of
        none ->
            Acc;
        {K, V, NextIter} ->
            case Fun(K, V, Acc) of
                {halt, FinalAcc} -> FinalAcc;
                {cont, NextAcc} -> do_fold_while(Fun, NextAcc, NextIter)
            end
    end.

%% @doc Like lists:search, but for maps to reduce memory pressure
%% (comparing to naive implementation with conversion to list)
-spec search(fun((K, V) -> boolean()), #{K => V}) -> false | {K, V}.
search(Fun, Map) when is_function(Fun, 2), is_map(Map) ->
    do_search(Fun, maps:next(maps:iterator(Map))).

do_search(_Fun, none) ->
    false;
do_search(Fun, {Key, Value, Iter}) ->
    case Fun(Key, Value) of
        true -> {Key, Value};
        false -> do_search(Fun, maps:next(Iter))
    end.

%% @doc Fold two maps "joining" them by key.
%% NOTE: If a key-value exists only in one map, this pair is ignored altogether
-spec zipfold(
    fun((K, V1, V2, A) -> A),
    InitAcc :: A,
    #{K => V1},
    #{K => V2}
) -> A.
zipfold(Fun, Acc, M1, M2) ->
    maps:fold(
        fun(Key, V1, AccIn) ->
            case maps:find(Key, M2) of
                {ok, V2} ->
                    Fun(Key, V1, V2, AccIn);
                error ->
                    AccIn
            end
        end,
        Acc,
        M1
    ).

%% @doc Flattens nested map and joins its keys as binaries with given separator
-spec flatten_join(char(), map()) -> map().
flatten_join(Separator, Map) when is_map(Map) ->
    {Result, _} = flatten_join(Separator, Map, #{}, []),
    Result.

flatten_join(Separator, Map, IntoMap, Prefixes) when is_map(Map) ->
    Folder = fun
        (K, V, {Acc, Prefix}) when is_map(V) ->
            {Acc1, _} = flatten_join(Separator, V, Acc, [K | Prefix]),
            {Acc1, Prefix};
        (K, V, {Acc, Prefix}) ->
            {maps:put(join_key(Separator, [K | Prefix]), V, Acc), Prefix}
    end,
    maps:fold(Folder, {IntoMap, Prefixes}, Map).

join_key(Separator, Parts) ->
    Transformer = fun
        (V) when is_atom(V) -> atom_to_binary(V);
        (V) when is_list(V) -> list_to_binary(V);
        (V) when is_binary(V) -> V;
        (V) -> list_to_binary(io_lib:format("~p", [V]))
    end,
    iolist_to_binary(lists:join(Separator, lists:map(Transformer, lists:reverse(Parts)))).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-spec test() -> _.

-spec flatten_join_test() -> _.
flatten_join_test() ->
    ?assertEqual(
        #{
            <<"a.b.c">> => "test",
            <<"a.d">> => <<"test">>,
            <<"a.b.e">> => 42,
            <<"a.b.{arbitrary,[#{term => term}]}">> => "arbitrary term",
            <<"f">> => 42
        },
        flatten_join($., #{
            a => #{
                b => #{
                    "c" => "test",
                    "e" => 42,
                    {arbitrary, [#{term => term}]} => "arbitrary term"
                },
                "d" => <<"test">>
            },
            f => 42
        })
    ).

-endif.
