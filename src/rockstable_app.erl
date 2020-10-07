%%%-------------------------------------------------------------------
%% @doc rockstable public API
%% @end
%%%-------------------------------------------------------------------

-module(rockstable_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    rockstable_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
