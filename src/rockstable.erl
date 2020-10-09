-module(rockstable).
-author("cleverfox <devel@viruzzz.org>").
-create_date("2020-10-04").

-export([table/1,tables/0]).
-export([etsget1/2]).
-export([put/2]).

-record (test1, {
           field1,
           field2,
           field3
          }).

-record (test2, {
           fieldA,
           fieldB,
           fieldC
          }).

table(test1) ->
  {
   test1,
   record_info(fields,test1),
   field1,
   [field2, field3],
   fun() -> ok end
  };

table(test2) ->
  {
   test2,
   record_info(fields,test2),
   [fieldA, fieldC],
   [fieldB, [fieldB, fieldC]],
   undefined
  };

table(justkv) ->
  {
   justkv,
   [key,value],
   [key],
   [],
   undefined
  }.

tables() ->
  [ table(T) || T<- [test1, test2, justkv] ].


-define (encode(X), term_to_binary(X)).
-define (decode(X), binary_to_term(X)).
-define (idxenc(X), sext:encode(X)).
-define (idxdec(X), sext:decode(X)).
-define (idxmatcher(X), sext:prefix(X)).

put(Alias, Record) ->
  Table=element(1,Record),
  PriKey=mk_pri_key(Alias, Table, Record),
  Key=?encode(PriKey),
  {Db,CF}=rockstable:etsget1({Alias,{Table,{handler,table}}},no_table_handler),
  case rocksdb:get(Db,CF, Key, []) of
    not_found -> ok;
    {ok, _} -> io:format("FIX ME, check old record before replacement~n")
  end,
  ok=rocksdb:put(Db, CF, Key, ?encode(Record), []),
  Indexes=mk_idx(Alias, Table, Record),
  put_index(Alias, Table, Indexes, PriKey),
%  { PriKey, Indexes }.
  Indexes.

%%%
%%% --- [ internal functions ] ---
%%% 

put_index(Alias, Table, IdxData, PriK) ->
  lists:map(
    fun({IdxID, Data}) ->
        {Db,CF}=etsget1({Alias,{Table,{handler, {index,IdxID}}}},
                           'no_index_handler'),
        Key={Data, PriK},
        io:format("Idx ~p~n",[Key]),
        ok=rocksdb:put(Db, CF, ?idxenc(Key), <<>>, [])
    end, IdxData).

mk_pri_key(Alias, Table, Record) ->
  PriKey=etsget1({Alias,{Table,prikey_nums}}, 'no_such_table'),
  lists:foldl(
    fun(N,Acc) ->
        Num=element(N,PriKey),
        setelement(N,Acc,element(Num, Record))
    end, PriKey, 
    lists:seq(1,size(PriKey))
   ).

etsget1(Key, Error) ->
  case ets:select(rockstable,[{{Key,'$1'},[],['$1']}]) of
    [] -> throw(Error);
    [N] -> N;
    [_|_] -> throw(multiple_matched)
  end.

mk_idx(Alias, Table, Record) ->
  Indexes=ets:match_object(rockstable,{{Alias,{Table,{index,'_'}}},'_'}),
  lists:map(
    fun({{_,{_,{index,Name}}}, Fields}) ->
        {
         Name,
         mk_idx_key(Fields, Record)
        }
    end, Indexes).

mk_idx_key(Num, Record) when is_integer(Num)->
  element(Num, Record);

mk_idx_key(Index0, Record) when is_list(Index0) ->
  Index=list_to_tuple(Index0),
  lists:foldl(
    fun(N,Acc) ->
        Num=element(N,Index),
        setelement(N,Acc,element(Num, Record))
    end, Index, 
    lists:seq(1,size(Index))
   ).


