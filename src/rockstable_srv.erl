%%%-------------------------------------------------------------------
%% @doc rockstable_srv gen_server
%% @end
%%%-------------------------------------------------------------------
-module(rockstable_srv).
-author("cleverfox <devel@viruzzz.org>").
-create_date("2020-10-06").

-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_Args) ->
  BTable=ets:new(rockstable,[named_table,protected,set,{read_concurrency,true}]),
  {ok, #{table => BTable} }.

handle_call({open_db, Alias, Path, Tablespec}, _From, #{table:=Tab}=State) ->
  Matched=ets:match_object(Tab,{{Alias,dbh},'_'}),
  case Matched of
    [{{Alias, dbh}, _Db}] ->
      {reply, {error, alias_in_use}, State};
    [] ->
      ExistsCols=try
                   {ok,EC}=rocksdb:list_column_families(Path, []),
                   EC
                 catch _:_ -> []
                 end,
      Cols= lists:foldl(
              fun(T, Acc) ->
                  Acc ++ rockstable_internal:table_cols(T)
              end, [], Tablespec),
      {ok, {Db, Handlers}} = rockstable_internal:open_db(Path, Cols),


      Data= lists:foldl(
              fun({TableName,Fields,PriKey, Indices,_}=CurTable, Acc) ->
                  TableCols=rockstable_internal:table_cols(CurTable),
                  IndexData=[
                             {{Alias, {TableName, {index, IndName}}}, IndDef} 
                             || {IndName, IndDef} <- rockstable_internal:table_index_idx(CurTable)
                            ],
                  Acc++
                  [{{Alias, {TableName, prikey_names}}, PriKey },
                   {{Alias, {TableName, prikey_nums}}, rockstable_internal:table_prik_idx(CurTable) },
                   {{Alias, {TableName, fields}}, Fields},
                   {{Alias, {TableName, indices}}, Indices}
                   |
                   lists:map(
                     fun({CN,CT}) ->
                         {{Alias,{TableName,{handler,CN}}},
                          {Db,proplists:get_value(CT,Handlers)}
                         }
                     end, TableCols)
                  ]++IndexData
              end, [{{Alias, dbh}, Db}], Tablespec),

      ets:insert(Tab,Data),

      lists:foreach(
        fun({_Name, _Fields, _PriKey, _Indexes, InitFun}=T) ->
            TCol=rockstable_internal:table_cols(T),
            {table, Name} = lists:keyfind(table,1,TCol),
            case lists:member(Name, ExistsCols) of
              true -> ok;
              false when is_function(InitFun) ->
                InitFun();
              false -> ok
            end
        end, Tablespec),

      {reply, ok, State}
  end;

handle_call({close_db, Alias}, _From, #{table:=Tab}=State) -> 
  Matched=ets:match_object(Tab,{{Alias,dbh},'_'}),
  case Matched of
    [{{Alias, dbh}, Db}] ->
      ok=rocksdb:close(Db),
      ets:match_delete(Tab, {{Alias,'_'},'_'}),
      {reply, Matched, State};
    [] ->
      {reply, {error, no_alias}, State}
  end;

handle_call(_Request, _From, State) ->
  logger:notice("Unknown call ~p",[_Request]),
  {reply, error, State}.

handle_cast(_Msg, State) ->
  logger:notice("Unknown cast ~p",[_Msg]),
  {noreply, State}.

handle_info(_Info, State) ->
  logger:notice("~s Unknown info ~p", [?MODULE,_Info]),
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

