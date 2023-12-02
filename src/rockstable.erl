-module(rockstable).
-author("cleverfox <devel@viruzzz.org>").
-create_date("2020-10-04").

-export([open_db/3, close_db/1]).
-export([put/3,get/3,get/4,del/3]).
-export([tx_new/1, tx_commit/1]).
-export([backup/2,restore/2]).
-export([transaction/2]).

-define (encode(X), term_to_binary(X)).
-define (decode(X), binary_to_term(X)).
-define (idxenc(X), sext:encode(X)).
-define (idxdec(X), sext:decode(X)).
-define (idxmatcher(X), sext:prefix(X)).

open_db(Alias, Path, Tablespec) ->
  gen_server:call(rockstable_srv,{open_db, Alias, Path, Tablespec}).

close_db(Alias) ->
  gen_server:call(rockstable_srv,{close_db, Alias}).

transaction(Alias, Fun) ->
  {ok, Ref} = tx_new(Alias),
  try
    put(rockstable_txn,Ref),
    Res=Fun(),
    tx_commit(Ref),
    put(rockstable_txn,undefined),
    {atomic, Res}
  catch Ec:Ee ->
          put(rockstable_txn,undefined),
          {aborted,{Ec,Ee}}
  end.



backup(Alias, BackupDB) ->
  Matched=ets:match_object(rockstable,{{Alias,dbh},'_'}),
  case Matched of
    [{{Alias,dbh}, Handler}] ->
      {ok, Bck1} = rocksdb:open_backup_engine(BackupDB),
      ok=rocksdb:create_new_backup(Bck1, Handler),
      ok=rocksdb:close_backup_engine(Bck1);
    [] ->
      throw('no_alias')
  end.

restore(BackupDB, DBPath) ->
  {ok, Bck1} = rocksdb:open_backup_engine(BackupDB),
  rocksdb:restore_db_from_latest_backup(Bck1, DBPath),
  ok=rocksdb:close_backup_engine(Bck1).

tx_new(Alias) ->
  Matched=ets:match_object(rockstable,{{Alias,dbh},'_'}),
  case Matched of
    [{{Alias,dbh}, Handler}] ->
      rocksdb:transaction(Handler,[]);
    [] ->
      throw('no_alias')
  end.

tx_commit(Txn) ->
  ok = rocksdb:transaction_commit(Txn).

put(Alias, Txn, Record) ->
  Table=element(1,Record),
  PriKey=mk_pri_key(Alias, Table, Record),
  Key=?encode(PriKey),
  NewIndexes=mk_idx(Alias, Table, Record),
  {Db,CF}=etsget1({Alias,{Table,{handler,table}}},no_table_handler),
  {IdxDel,IdxAdd}=case iget(Db, Txn,CF, Key) of
                    not_found ->
                      {[],NewIndexes};

                    {ok, OldBinRec} ->
                      OldObject=?decode(OldBinRec),
                      OldIndexes=mk_idx(Alias, Table, OldObject),
                      {
                       OldIndexes--NewIndexes,
                       NewIndexes--OldIndexes
                      }
                  end,
  ok=iput(Db, Txn, CF, Key, ?encode(Record)),
  del_index(Alias, Txn, Table, IdxDel, PriKey),
  put_index(Alias, Txn, Table, IdxAdd, PriKey),
  ok.

get(Alias, Txn, Pattern) ->
  get(Alias, Txn, Pattern, []).

get(Alias, Txn, Pattern, Opts) ->
  Table=element(1,Pattern),
  {Db,CF}=etsget1({Alias,{Table,{handler,table}}},no_table_handler),
  PriKey=mk_pri_key(Alias, Table, Pattern),
  case rockstable_internal:is_tuple_usable_idx(PriKey) of
    true -> get_pk(Db, Txn, CF, PriKey);
    false ->
      case lists:keyfind(use_index,1,Opts) of
        {use_index, Index} ->
          IndexKey=mk_n_idx(Alias, Table, Index, Pattern),
%          io:format("Force idx ~p: ~p~n",[Index, mk_n_idx(Alias, Table, Index, Pattern)]),
          case lists:keyfind(pred,1,Opts) of
            {pred, Pred} ->
              get_by_index(Alias, Txn, Table, Index, IndexKey, {Pattern,Pred});
            false ->
              get_by_index(Alias, Txn, Table, Index, IndexKey, Pattern)
          end;
        false ->
          Indexes=rockstable_internal:usable_indices(mk_idx(Alias, Table, Pattern)),
%          io:format("Indexes ~p~n",[Indexes]),
          case Indexes of
            [] ->
              case lists:keyfind(pred,1,Opts) of
                {pred, Pred} ->
                  get_ss(Db, Txn, CF, {Pattern,Pred});
                false ->
                  get_ss(Db, Txn, CF, Pattern)
              end;
            [{Fields,IndexKey}|_] ->
%              io:format("Idx ~p: ~p~n",[Fields, mk_n_idx(Alias, Table, Fields, Pattern)]),
              case lists:keyfind(pred,1,Opts) of
                {pred, Pred} ->
                  get_by_index(Alias, Txn, Table, Fields, IndexKey, {Pattern,Pred} );
                false ->
                  get_by_index(Alias, Txn, Table, Fields, IndexKey, Pattern)
              end
          end
      end
  end.

del(Alias, Txn, Pattern) ->
  Table=element(1,Pattern),
  {Db,CF}=etsget1({Alias,{Table,{handler,table}}},no_table_handler),
  PriKey=mk_pri_key(Alias, Table, Pattern),
  case rockstable_internal:is_tuple_usable_idx(PriKey) of
    false ->
      throw('no_primary_key_in_pattern');
    true ->
      Key=?encode(PriKey),
      case iget(Db, Txn,CF, Key) of
        not_found ->
          not_found;
        {ok, BinRecord} ->
          Record=?decode(BinRecord),
          Indexes=mk_idx(Alias, Table, Record),
          del_index(Alias, Txn, Table, Indexes, PriKey),
          idelete(Db, Txn,CF,Key)
      end
  end.

%%%
%%% --- [ internal functions ] ---
%%% 

del_index(Alias, Txn, Table, IdxData, PriK) ->
  lists:map(
    fun({IdxID, Data}) ->
        {Db,ICF}=etsget1({Alias,{Table,{handler, {index,IdxID}}}},
                   'no_index_handler'),
        Key={Data, PriK},
%        io:format("del Idx ~p~n",[Key]),
        ok=idelete(Db, Txn, ICF, ?idxenc(Key))
    end, IdxData).

mk_n_idx(Alias, Table, Index, Pattern) ->
  Fields=etsget1({Alias,{Table,{index,Index}}}, 'no_index'),
  mk_idxN(Fields, Pattern).

get_by_index(Alias, Txn, Table, Fields, IndexKey, Pattern) ->
  {Db,ICF}=etsget1({Alias,{Table,{handler, {index,Fields}}}},
                   'no_index_handler'),
  {ok, Itr} = iitr(Db, Txn, ICF),
  try
    get_idx_itr({seek, ?idxmatcher({IndexKey,'_'})},
                Itr,
                IndexKey,
                fun(E,A) ->
                    {Db,CF}=etsget1({Alias,{Table,{handler,table}}},no_table_handler),
                    {ok, BinRecord}=iget(Db, Txn,CF, ?encode(E)),
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

get_pk(Db, Txn, CF, PriKey) ->
  case iget(Db, Txn,CF, ?encode(PriKey)) of
    not_found -> not_found;
    {ok, BinRecord} ->
      GotRecord=?decode(BinRecord),
      [GotRecord]
  end.

get_ss(Db, Txn, TableCF, Pattern) ->
  {ok, Itr} = iitr(Db, Txn, TableCF),
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

is_record_match({Pattern,Fun}, Record) when is_function(Fun,1) ->
  R=match_record(Pattern, Record, 1, size(Pattern)),
  if R ->
       Fun(Record);
     true ->
       false
  end;

is_record_match(Pattern, Record) ->
  R=match_record(Pattern, Record, 1, size(Pattern)),
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

put_index(Alias, Txn, Table, IdxData, PriK) ->
  lists:map(
    fun({IdxID, Data}) ->
        {Db,CF}=etsget1({Alias,{Table,{handler, {index,IdxID}}}},
                           'no_index_handler'),
        Key={Data, PriK},
%        io:format("Idx ~p~n",[Key]),
        ok=iput(Db, Txn, CF, ?idxenc(Key), <<>>)
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

idelete(Db, undefined, CF, Key) ->
  rocksdb:delete(Db, CF, Key, []);
idelete(_, env, CF, Key) ->
  Txn=erlang:get(rockstable_txn),
  true=is_reference(Txn),
  rocksdb:transaction_delete(Txn,CF, Key);
idelete(_, Txn, CF, Key) when is_reference(Txn) ->
  rocksdb:transaction_delete(Txn,CF, Key).


iget(Db, undefined, CF, Key) ->
  rocksdb:get(Db, CF, Key, []);
iget(_, env, CF, Key) ->
  Txn=erlang:get(rockstable_txn),
  true=is_reference(Txn),
  rocksdb:transaction_get(Txn,CF, Key, []);
iget(_, Txn, CF, Key) when is_reference(Txn) ->
  rocksdb:transaction_get(Txn,CF, Key, []).

iput(Db, undefined, CF, Key, Val) ->
  rocksdb:put(Db, CF, Key, Val, []);
iput(_, env, CF, Key, Val) ->
  Txn=erlang:get(rockstable_txn),
  true=is_reference(Txn),
  rocksdb:transaction_put(Txn, CF, Key, Val);
iput(_, Txn, CF, Key, Val) when is_reference(Txn) ->
  rocksdb:transaction_put(Txn, CF, Key, Val).

iitr(Db, undefined, CF) ->
  rocksdb:iterator(Db, CF, []);
iitr(Db, env, CF) ->
  Txn=erlang:get(rockstable_txn),
  true=is_reference(Txn),
  rocksdb:transaction_iterator(Db, Txn, CF, []);
iitr(Db, Txn, CF) when is_reference(Txn) ->
  rocksdb:transaction_iterator(Db, Txn, CF, []).

