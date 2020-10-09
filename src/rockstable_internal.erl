-module(rockstable_internal).
-author("cleverfox <devel@viruzzz.org>").
-create_date("2020-10-08").

-export([open_db/2, table_cols/1, table_prik_idx/1, table_index_idx/1]).

open_db(Path, Cols) ->
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
  Cols1=[ ColName || {_,ColName} <- Cols ],
  Handles1=lists:zip(ExistsCols, Handles),
  Handles2=lists:foldl(
    fun(Name, Acc) ->
        {ok, Handle} = rocksdb:create_column_family(Db, Name, []),
        Acc ++ [{Name, Handle}]
    end, Handles1, Cols1--ExistsCols),
  {ok, {Db, Handles2}}. 

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

table_cols({Name, _Fields, _PrimaryKey, Indexes, _InitFun}) ->
 BaseName=atom_to_list(Name),
 Idxes=lists:map(fun(N) ->
                     {{index,index_name(N)},
                      index_name(BaseName, N)
                     }
                 end, Indexes),
 [ {table, BaseName}|Idxes ].

index_name(Basename, Idx) when is_atom(Basename) ->
  index_name(atom_to_list(Basename), Idx);

index_name(BaseName, N) when is_atom(N) ->
  BaseName++"_IDX_"++atom_to_list(N);

index_name(BaseName, [N1,N2|_]=N) when is_atom(N1), is_atom(N2) ->
  lists:foldl(
    fun(Ni, Acc) ->
        Acc++ "_" ++ atom_to_list(Ni)
    end, BaseName ++ "_IDX", N).

index_name(N) when is_atom(N) ->
  N;

index_name([N1,N2|_]=N) when is_atom(N1), is_atom(N2) ->
  list_to_tuple(
    lists:reverse(
      lists:foldl(
        fun(Ni, Acc) ->
            [Ni|Acc]
        end, [], N)
     )
   ).

table_prik_idx({_Name, RI, PriK, _Idx, _Init}) ->
  {_,Fields}=lists:foldl(
           fun(F, {I, Acc}) ->
               {I+1, maps:put(F, I, Acc)}
           end, {2, #{}}, RI),
  if is_list(PriK) ->
       list_to_tuple([ maps:get(F, Fields) || F <- PriK ]);
     true ->
       {maps:get(PriK, Fields)}
  end.


table_index_idx({_Name, RI, _PriK, Idx, _Init}) ->
  {_,Fields}=lists:foldl(
           fun(F, {I, Acc}) ->
               {I+1, maps:put(F, I, Acc)}
           end, {2, #{}}, RI),
  lists:map(
    fun(Fs) when is_atom(Fs) ->
        {index_name(Fs) , maps:get(Fs, Fields)};
       ([_,_|_]=Fs) ->
        {index_name(Fs), [ maps:get(F, Fields) || F <- Fs ]}
    end, Idx).


%table_fields({RI, PriK, Idx, Init}) ->
%  {_,Fields}=lists:foldl(
%           fun(F, {I, Acc}) ->
%               {I+1, maps:put(F, I, Acc)}
%           end, {2, #{}}, RI),
%  PriK1=list_to_tuple([ {F, maps:get(F, Fields)} || F <- PriK ]),
%  Idx1=lists:map(
%         fun(Fs) when is_atom(Fs) ->
%             {Fs, maps:get(Fs, Fields)};
%            ([_,_|_]=Fs) ->
%             [ {F, maps:get(F, Fields)} || F <- Fs ]
%         end, Idx),
%  {RI, PriK1, Idx1, Init}.
%
