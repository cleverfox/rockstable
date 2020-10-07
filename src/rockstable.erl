-module(rockstable).

-export([trans_check/1]).
-compile([export_all,nowarn_export_all]).
-define (assertEqual(A,B), A=B).
-define (encode(X), term_to_binary(X)).
-define (decode(X), binary_to_term(X)).
-define (idxenc(X), sext:encode(X)).
-define (idxdec(X), sext:decode(X)).
-define (idxmatcher(X), sext:prefix(X)).

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

table(N) ->
  {RI, PriK, Idx, Init} = tabled(N),
  {_,Fields}=lists:foldl(
           fun(F, {I, Acc}) ->
               {I+1, maps:put(F, I, Acc)}
           end, {2, #{}}, RI),
  PriK1=list_to_tuple([ {F, maps:get(F, Fields)} || F <- PriK ]),
  Idx1=lists:map(
         fun(Fs) when is_atom(Fs) ->
             {Fs, maps:get(Fs, Fields)};
            ([_,_|_]=Fs) ->
             [ {F, maps:get(F, Fields)} || F <- Fs ]
         end, Idx),
  {N, {RI, PriK1, Idx1, Init}}.

tabled(test1) ->
  {
   record_info(fields,test1),
   [field1, field2],
   [field2, field3],
   fun() ->
       ok
   end
  };

tabled(test2) ->
  {
   record_info(fields,test2),
   [fieldA, fieldC],
   [fieldB, [fieldB, fieldC]],
   undefined
  };

tabled(justkv) ->
  {
   [key,value],
   [key],
   [],
   undefined
  }.

tables() ->
  [ table(T) || T<- [test1, test2, justkv] ].

open_db(Path, Tablespec) ->
  ExistsCols=try
               {ok,EC}=rocksdb:list_column_families(Path, []),
               EC
             catch _:_ ->
                     ["default"]
             end,
  {ok, Db, Handles} = rocksdb:open(Path,
                                    [{create_if_missing, true}],
                                    [ {CN, []} || CN <- ExistsCols ]
                                   ),
  Handles1=lists:zip(ExistsCols, Handles),
  Cols= lists:foldl(
          fun(T, Acc) ->
              Acc ++ table_cols(T)
          end, [], Tablespec),
  Handles2=lists:foldl(
    fun(Name, Acc) ->
        {ok, Handle} = rocksdb:create_column_family(Db, Name, []),
        Acc ++ [{Name, Handle}]
    end, Handles1, Cols--ExistsCols),
  {ok, {Db, Handles2}}. 

index_name(Basename, Idx) when is_atom(Basename) ->
  index_name(atom_to_list(Basename), Idx);

index_name(BaseName, {N,_}) when is_atom(N) ->
  BaseName++"_IDX_"++atom_to_list(N);

index_name(BaseName, [{N1,_},{N2,_}|_]=N) when is_atom(N1), is_atom(N2) ->
  lists:foldl(
    fun({Ni,_}, Acc) ->
        Acc++ "_" ++ atom_to_list(Ni)
    end, BaseName ++ "_IDX", N).

table_cols({Name, {_Fields, _PrimaryKey, Indexes, _InitFun}}) ->
 BaseName=atom_to_list(Name),
 Idxes=lists:map(fun(N) ->
                     index_name(BaseName, N)
                 end, Indexes),
 [ BaseName|Idxes ].

mk_pri_key({_,{_,PriKey,_,_}}, Record) ->
  lists:foldl(
    fun(N,Acc) ->
        {_,Num}=element(N,PriKey),
        setelement(N,Acc,element(Num, Record))
    end, PriKey, 
    lists:seq(1,size(PriKey))
   ).

mk_idx({TN,{_,_,Indexes,_}}, Record) ->
  lists:map(
    fun(Index) ->
        {
        index_name(TN, Index),
        mk_idx_key(Index, Record)
        }
    end, Indexes).

mk_idx_key({_,Num}, Record) ->
  element(Num, Record);

mk_idx_key(Index0, Record) when is_list(Index0) ->
  Index=list_to_tuple(Index0),
  io:format("Index ~p~n",[Index]),
  lists:foldl(
    fun(N,Acc) ->
        io:format("El ~p ~p = ~p~n",[N, Index, element(N,Index)]),
        {_,Num}=element(N,Index),
        setelement(N,Acc,element(Num, Record))
    end, Index, 
    lists:seq(1,size(Index))
   ).

del({Db, CFs}=DBH, {Table,{_,_,_,_}}=TD, Pattern) ->
  PriKey=mk_pri_key(TD, Pattern),
  Key=?encode(PriKey),
  LTable=atom_to_list(Table),
  CF=proplists:get_value(LTable, CFs),
  case rocksdb:get(Db,CF, Key, []) of
    not_found ->
      not_found;
    {ok, BinRecord} ->
      Record=?decode(BinRecord),
      Indexes=mk_idx(TD, Record),
      del_index(DBH, Indexes, PriKey),
      rocksdb:delete(Db,CF,Key,[])
  end.

put({Db, CFs}=DBH, {Table,{_,_,_,_}}=TD, Record) ->
  PriKey=mk_pri_key(TD, Record),
  Key=?encode(PriKey),
  LTable=atom_to_list(Table),
  CF=proplists:get_value(LTable, CFs),
  case rocksdb:get(Db,CF, Key, []) of
    not_found -> ok;
    {ok, _} -> io:format("FIX ME, check old record before replacement~n")
  end,
  ok=rocksdb:put(Db, CF, Key, ?encode(Record), []),
  Indexes=mk_idx(TD, Record),
  put_index(DBH, Indexes, PriKey),
  { PriKey, Indexes }.

is_tuple_usable_idx2({'_',_}) -> false;
is_tuple_usable_idx2({_,'_'}) -> false;
is_tuple_usable_idx2(_) -> true.

is_tuple_usable_idx3({'_',_,_}) -> false;
is_tuple_usable_idx3({_,'_',_}) -> false;
is_tuple_usable_idx3({_,_,'_'}) -> false;
is_tuple_usable_idx3(_) -> true.

is_tuple_usable_idx4({'_',_,_,_}) -> false;
is_tuple_usable_idx4({_,'_',_,_}) -> false;
is_tuple_usable_idx4({_,_,'_',_}) -> false;
is_tuple_usable_idx4({_,_,_,'_'}) -> false;
is_tuple_usable_idx4(_) -> true.

is_tuple_usable_idx5({'_',_,_,_,_}) -> false;
is_tuple_usable_idx5({_,'_',_,_,_}) -> false;
is_tuple_usable_idx5({_,_,'_',_,_}) -> false;
is_tuple_usable_idx5({_,_,_,'_',_}) -> false;
is_tuple_usable_idx5({_,_,_,_,'_'}) -> false;
is_tuple_usable_idx5(_) -> true.

is_tuple_usable_idx6({'_',_,_,_,_,_}) -> false;
is_tuple_usable_idx6({_,'_',_,_,_,_}) -> false;
is_tuple_usable_idx6({_,_,'_',_,_,_}) -> false;
is_tuple_usable_idx6({_,_,_,'_',_,_}) -> false;
is_tuple_usable_idx6({_,_,_,_,'_',_}) -> false;
is_tuple_usable_idx6({_,_,_,_,_,'_'}) -> false;
is_tuple_usable_idx6(_) -> true.

is_tuple_usable_idx({_,_}=Tuple) -> is_tuple_usable_idx2(Tuple);
is_tuple_usable_idx({_,_,_}=Tuple) -> is_tuple_usable_idx3(Tuple);
is_tuple_usable_idx({_,_,_,_}=Tuple) -> is_tuple_usable_idx4(Tuple);
is_tuple_usable_idx({_,_,_,_,_}=Tuple) -> is_tuple_usable_idx5(Tuple);
is_tuple_usable_idx({_,_,_,_,_,_}=Tuple) -> is_tuple_usable_idx6(Tuple);

is_tuple_usable_idx(Tuple) ->
  is_tuple_usable_idx_list(tuple_to_list(Tuple)).

is_tuple_usable_idx_list([]) -> true;
is_tuple_usable_idx_list(['_'|_]) -> false;
is_tuple_usable_idx_list([_|Rest]) -> is_tuple_usable_idx_list(Rest).

usable_indices([]) ->
  [];

usable_indices([{IndexName, Idx}|Rest]) ->
  if Idx == '_' ->
       usable_indices(Rest);
     is_tuple(Idx) ->
       case is_tuple_usable_idx(Idx) of
         true ->
           [{IndexName, Idx}|usable_indices(Rest)];
         false ->
           usable_indices(Rest)
       end;
     true ->
       [{IndexName, Idx}|usable_indices(Rest)]
  end.

is_record_match(Pattern, Record) ->
  R=match_record(Pattern, Record, 1, size(Pattern)),
  io:format("Match ~p with ~p ~p~n",[Pattern, Record, R]),
  R.

match_record(Pattern, Record, Field, End) when Field>End ->
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

get_idx({Db, CFs}=DBH, {Table,{_,_,_,_}}=TD, Pattern) ->
  LTable=atom_to_list(Table),
  TableCF=proplists:get_value(LTable, CFs),

  PriKey=mk_pri_key(TD, Pattern),
  case is_tuple_usable_idx(PriKey) of
    true ->
      io:format("Sequence scan~n"),
      case rocksdb:get(Db,TableCF, ?encode(PriKey), []) of
        not_found -> not_found;
        {ok, BinRecord} ->
          GotRecord=?decode(BinRecord),
          [GotRecord]
      end;
    false ->
      Indexes=usable_indices(mk_idx(TD, Pattern)),
      case Indexes of
        [] ->
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
          end;
        [{IndexName, IndexKey}|_] = Avail ->
          io:format("Avail index ~p~n",[Avail]),
          io:format("Using index ~s~n",[IndexName]),
          CF=proplists:get_value(IndexName, CFs),
          {ok, Itr} = rocksdb:iterator(Db, CF, []),
          try
            get_idx_itr({seek, ?idxmatcher({IndexKey,'_'})},
                        Itr,
                        IndexKey,
                        fun(E,A) ->
                            {ok, BinRecord}=rocksdb:get(Db,TableCF, ?encode(E), []),
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
          end
      end
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

iterator_seek_key_index(Key) ->
  <<131,Bin/binary>> = term_to_binary(Key),
  <<131,104,2, Bin/binary>>.

put_index({Db, CFs}, IdxData, PriK) ->
  lists:map(
    fun({IdxID, Data}) ->
        CF=proplists:get_value(IdxID, CFs),
        Key={Data, PriK},
        io:format("Idx ~p~n",[Key]),
        ok=rocksdb:put(Db, CF, ?idxenc(Key), <<>>, [])
    end, IdxData).

del_index({Db, CFs}, IdxData, PriK) ->
  lists:map(
    fun({IdxID, Data}) ->
        CF=proplists:get_value(IdxID, CFs),
        Key={Data, PriK},
        io:format("del Idx ~p~n",[Key]),
        ok=rocksdb:delete(Db, CF, ?idxenc(Key), [])
    end, IdxData).

trans_check(_X) ->
  {ok,Cols}=rocksdb:list_column_families("test.db", []),
  io:format("Cols ~p~n",[Cols]),
  {ok, Db, Handles} = rocksdb:open("test.db",
                                    [{create_if_missing, true}],
                                    [ {CN, []} || CN <- Cols ]
                                   ),
  io:format("Db ~p~n",[Db]),
  io:format("Handles ~p~n",[Handles]),

%  {ok, Handle1} = rocksdb:create_column_family(Db, "test1", []),
%  {ok, Handle2} = rocksdb:create_column_family(Db, "test2", []),
%  {ok, Db1} = rocksdb:open("test1.db",[{create_if_missing, true}]),


  io:format("get ~p~n",[rocksdb:get(Db,<<"a">>,[])]),
  io:format("get ~p~n",[rocksdb:get(Db,hd(Handles),<<"a">>,[])]),
  io:format("get ~p~n",[rocksdb:get(Db,hd(tl(Handles)),<<"a">>,[])]),

  dump(Db, hd(Handles)),

  ok = rocksdb:put(Db, <<"b">>, <<"v1">>, []),
  ok = rocksdb:put(Db, hd(tl(Handles)), <<"a">>, <<"v2">>, []),
%  ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db, <<"a">>, [])),
%  io:format("Seq1 ~p~n",[rocksdb:get_latest_sequence_number(Db)]),
%%  io:format("Seq1 ~p~n",[rocksdb:get_latest_sequence_number(Db1)]),

%  io:format("Last ~p~n",[rocksdb:get_latest_sequence_number(Db)]),
%  dump_transbin(Db),

%  {ok, Itr} = rocksdb:tlog_iterator(Db, 0),
%  {ok, Last, TransactionBin} = rocksdb:tlog_next_binary_update(Itr),
%  io:format("T ~p TB ~p~n",[Last, TransactionBin]),
%%  ?assertEqual(1, Last),
%  ok = rocksdb:tlog_iterator_close(Itr),


%  ?assertEqual(0, rocksdb:get_latest_sequence_number(Db1)),


%  ?assertEqual(not_found, rocksdb:get(Db1, <<"a">>, [])),
%  ok = rocksdb:write_binary_update(Db1, TransactionBin, []),
%  ?assertEqual({ok, <<"v1">>}, rocksdb:get(Db1, <<"a">>, [])),
%  ?assertEqual(1, rocksdb:get_latest_sequence_number(Db1)),
%  ok = rocksdb:tlog_iterator_close(Itr),


%  ok = rocksdb:close(Db1).
  ok = rocksdb:close(Db).

%dump_transbin(DBH) ->
%  {ok, Itr} = rocksdb:tlog_iterator(DBH, 0),
%  dump_transbin1(Itr),
%  ok = rocksdb:tlog_iterator_close(Itr).
%
%
%dump_transbin1(Itr) ->
%  {ok, Last, Log, TransactionBin} = rocksdb:tlog_next_update(Itr),
%  io:format("Itr ~p Update ~p: ~p~n",[Itr, Last, TransactionBin]),
%  io:format("~p~n",[Log]),
%  if(Last > 1 ) ->
%      dump_transbin1(Itr);
%    true ->
%      ok
%  end.


dump(Db, CF) ->
  fold(Db, CF, fun({K,V}, A) ->
                   io:format("K ~p V ~p~n",[K, V]),
                   case K of
                     <<131,_/binary>> ->
                       io:format("dK ~p dV ~p~n",[?decode(K), ?decode(V)]);
                     <<16,_/binary>> when V==<<>> ->
                       io:format("dK ~p~n",[?idxdec(K)]);
                     _ -> ok
                   end,
                   A+1
               end, 0, []).

fold(DBHandle, CF, Fun, Acc0, ReadOpts) ->
  {ok, Itr} = rocksdb:iterator(DBHandle, CF, ReadOpts),
  do_fold(Itr, Fun, Acc0).

do_fold(Itr, Fun, Acc0) ->
  try
    fold_loop(rocksdb:iterator_move(Itr, first), Itr, Fun, Acc0)
  after
    rocksdb:iterator_close(Itr)
  end.

fold_loop({error, iterator_closed}, _Itr, _Fun, Acc0) ->
  throw({iterator_closed, Acc0});
fold_loop({error, invalid_iterator}, _Itr, _Fun, Acc0) ->
  Acc0;
fold_loop({ok, K}, Itr, Fun, Acc0) ->
  Acc = Fun(K, Acc0),
  fold_loop(rocksdb:iterator_move(Itr, next), Itr, Fun, Acc);
fold_loop({ok, K, V}, Itr, Fun, Acc0) ->
  Acc = Fun({K, V}, Acc0),
  fold_loop(rocksdb:iterator_move(Itr, next), Itr, Fun, Acc).
