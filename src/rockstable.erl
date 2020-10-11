-module(rockstable).
-author("cleverfox <devel@viruzzz.org>").
-create_date("2020-10-04").

-export([table/1,tables/0]).
-export([etsget1/2]).
-export([open_db/3, close_db/1]).
-export([put/2,get/2,get/3,del/2]).

-compile({no_auto_import,[get/2]}).

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

open_db(Alias, Path, Tablespec) ->
  gen_server:call(rockstable_srv,{open_db, Alias, Path, Tablespec}).

close_db(Alias) ->
  gen_server:call(rockstable_srv,{close_db, Alias}).

put(Alias, Record) ->
  Table=element(1,Record),
  PriKey=mk_pri_key(Alias, Table, Record),
  Key=?encode(PriKey),
  NewIndexes=mk_idx(Alias, Table, Record),
  {Db,CF}=rockstable:etsget1({Alias,{Table,{handler,table}}},no_table_handler),
  {IdxDel,IdxAdd}=case rocksdb:get(Db,CF, Key, []) of
                    not_found ->
                      {[],NewIndexes};

                    {ok, OldBinRec} ->
                      OldObject=?decode(OldBinRec),
                      OldIndexes=mk_idx(Alias, Table, OldObject),
                      {
                       OldIndexes--NewIndexes,
                       NewIndexes--OldIndexes,
                      }
                  end,
  ok=rocksdb:put(Db, CF, Key, ?encode(Record), []),
  del_index(Alias, Table, IdxDel, PriKey),
  put_index(Alias, Table, IdxAdd, PriKey),
  { IdxDel, IdxAdd }.

get(Alias, Pattern) ->
  get(Alias, Pattern, []).

get(Alias, Pattern, Opts) ->
  Table=element(1,Pattern),
  {Db,CF}=rockstable:etsget1({Alias,{Table,{handler,table}}},no_table_handler),
  PriKey=mk_pri_key(Alias, Table, Pattern),
  case rockstable_internal:is_tuple_usable_idx(PriKey) of
    true -> get_pk({Db, CF}, PriKey);
    false ->
      case lists:keyfind(use_index,1,Opts) of
        {use_index, Index} ->
          IndexKey=mk_n_idx(Alias, Table, Index, Pattern),
          io:format("Force idx ~p: ~p~n",[Index, mk_n_idx(Alias, Table, Index, Pattern)]),
          get_by_index(Alias, Table, Index, IndexKey, Pattern);
        false ->
          Indexes=rockstable_internal:usable_indices(mk_idx(Alias, Table, Pattern)),
          io:format("Indexes ~p~n",[Indexes]),
          case Indexes of
            [] ->
              get_ss({Db, CF}, Pattern);
            [{Fields,IndexKey}|_] ->
              io:format("Idx ~p: ~p~n",[Fields, mk_n_idx(Alias, Table, Fields, Pattern)]),
              get_by_index(Alias, Table, Fields, IndexKey, Pattern)
          end
      end
  end.

del(Alias, Pattern) ->
  Table=element(1,Pattern),
  {Db,CF}=rockstable:etsget1({Alias,{Table,{handler,table}}},no_table_handler),
  PriKey=mk_pri_key(Alias, Table, Pattern),
  case rockstable_internal:is_tuple_usable_idx(PriKey) of
    false ->
      throw('no_primary_key_in_pattern');
    true ->
      Key=?encode(PriKey),
      case rocksdb:get(Db,CF, Key, []) of
        not_found ->
          not_found;
        {ok, BinRecord} ->
          Record=?decode(BinRecord),
          Indexes=mk_idx(Alias, Table, Record),
          del_index(Alias, Table, Indexes, PriKey),
          rocksdb:delete(Db,CF,Key,[])
      end
  end.

%%%
%%% --- [ internal functions ] ---
%%% 

del_index(Alias, Table, IdxData, PriK) ->
  lists:map(
    fun({IdxID, Data}) ->
        {Db,ICF}=etsget1({Alias,{Table,{handler, {index,IdxID}}}},
                   'no_index_handler'),
        Key={Data, PriK},
        io:format("del Idx ~p~n",[Key]),
        ok=rocksdb:delete(Db, ICF, ?idxenc(Key), [])
    end, IdxData).

mk_n_idx(Alias, Table, Index, Pattern) ->
  Fields=etsget1({Alias,{Table,{index,Index}}}, 'no_index'),
  mk_idxN(Fields, Pattern).

get_by_index(Alias, Table, Fields, IndexKey, Pattern) ->
  {Db,ICF}=etsget1({Alias,{Table,{handler, {index,Fields}}}},
                   'no_index_handler'),
  {ok, Itr} = rocksdb:iterator(Db, ICF, []),
  try
    get_idx_itr({seek, ?idxmatcher({IndexKey,'_'})},
                Itr,
                IndexKey,
                fun(E,A) ->
                    {Db,CF}=rockstable:etsget1({Alias,{Table,{handler,table}}},no_table_handler),
                    {ok, BinRecord}=rocksdb:get(Db,CF, ?encode(E), []),
                    GotRecord=?decode(BinRecord),
                    case is_record_match(Pattern, GotRecord) of
                      true ->
                        [GotRecord|A];
                      false ->
                        A
                    end
                end, [])
  after
    rocksdb:iterator_close(Itr)
  end.

get_pk({Db, CF}, PriKey) ->
  case rocksdb:get(Db,CF, ?encode(PriKey), []) of
    not_found -> not_found;
    {ok, BinRecord} ->
      GotRecord=?decode(BinRecord),
      [GotRecord]
  end.

get_ss({Db, TableCF}, Pattern) ->
  io:format("Sequence scan~n"),
  {ok, Itr} = rocksdb:iterator(Db, TableCF, []),
  try
    get_tab_itr(first,
                Itr,
                fun(GotRecord,A) ->
                    case is_record_match(Pattern, GotRecord) of
                      true ->
                        [GotRecord|A];
                      false ->
                        A
                    end
                end, [])
  after
    rocksdb:iterator_close(Itr)
  end.

is_record_match(Pattern, Record) ->
  R=match_record(Pattern, Record, 1, size(Pattern)),
  io:format("Match ~p with ~p ~p~n",[Pattern, Record, R]),
  R.

match_record(_Pattern, _Record, Field, End) when Field>End ->
  true;

match_record(Pattern, Record, Field, End) ->
  PE=element(Field, Pattern),
  if(PE == '_') ->
      match_record(Pattern, Record, Field+1, End);
    element(Field, Record) =/= PE ->
      false;
    true ->
      match_record(Pattern, Record, Field+1, End)
  end.



get_tab_itr(Action, Itr, Fun, ExtraData) ->
  case rocksdb:iterator_move(Itr,Action) of
    {error, invalid_iterator} ->
      ExtraData;
    {ok, _BinKey, BinVal} ->
      Val=?decode(BinVal),
      ED1=Fun(Val, ExtraData),
      get_tab_itr(next, Itr, Fun, ED1);
    _Any ->
      ExtraData
  end.

get_idx_itr(Action, Itr, IndexKey, Fun, ExtraData) ->
  case rocksdb:iterator_move(Itr,Action) of
    {error, invalid_iterator} ->
      ExtraData;
    {ok, BinKey, <<>>} ->
      case ?idxdec(BinKey) of
        {IndexKey,IndexVal} ->
          ED1=Fun(IndexVal, ExtraData),
          get_idx_itr(next, Itr, IndexKey, Fun, ED1);
        _Any ->
          ExtraData
      end
  end.

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
        {Name, mk_idxN(Fields, Record)}
    end, Indexes).

mk_idxN(Fields, Record) ->
  mk_idx_key(Fields, Record).

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


