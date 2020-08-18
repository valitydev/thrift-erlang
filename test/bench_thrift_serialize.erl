-module(bench_thrift_serialize).

%% API
-export([
    thrift_serialize/1,
    bench_thrift_serialize/2,
    thrift_deserialize/1,
    bench_thrift_deserialize/2
]).

-spec snapshot_term() ->
    term().
snapshot_term() ->
    {ok, Bin} = file:read_file("test/ast.sample.term"),
    erlang:binary_to_term(Bin).

-spec thrift_type() ->
    {struct, _, {module(), atom()}}.
thrift_type() ->
    {struct, struct, {ast_thrift, 'Module'}}.

-spec thrift_serialize({input, _State}) ->
    term().
thrift_serialize({input, _}) ->
    snapshot_term().

-spec bench_thrift_serialize(term(), _State) ->
    term().
bench_thrift_serialize(Module, _) ->
    serialize(thrift_type(), Module).

-spec thrift_deserialize({input, _State}) ->
    binary().
thrift_deserialize({input, _}) ->
    serialize(thrift_type(), snapshot_term()).

-spec bench_thrift_deserialize(binary(), _State) ->
    term().
bench_thrift_deserialize(Binary, _) ->
    deserialize(thrift_type(), Binary).

serialize(Type, Data) ->
    Proto = thrift_strict_binary_codec:new(),
    case thrift_strict_binary_codec:write(Proto, Type, Data) of
        {ok, NewProto} ->
            thrift_strict_binary_codec:close(NewProto);
        {error, Reason} ->
            erlang:error({thrift, {protocol, Reason}})
    end.

deserialize(Type, Data) ->
    Proto = thrift_strict_binary_codec:new(Data),
    case thrift_strict_binary_codec:read(Proto, Type) of
        {ok, Result, _} ->
            Result;
        {error, Reason} ->
            erlang:error({thrift, {protocol, Reason}})
    end.

%% NOTE
%% Used for baseline measurements against regular protocol infrastructure.
%%
%% serialize(Type, Data) ->
%%     {ok, Trans} = thrift_membuffer_transport:new(),
%%     {ok, Proto} = thrift_binary_protocol:new(Trans, [{strict_read, true}, {strict_write, true}]),
%%     case thrift_protocol:write(Proto, {Type, Data}) of
%%         {NewProto, ok} ->
%%             {_, {ok, Result}} = thrift_protocol:close_transport(NewProto),
%%             Result;
%%         {_NewProto, {error, Reason}} ->
%%             erlang:error({thrift, {protocol, Reason}})
%%     end.
%%
%% deserialize(Type, Data) ->
%%     {ok, Trans} = thrift_membuffer_transport:new(Data),
%%     {ok, Proto} = thrift_binary_protocol:new(Trans, [{strict_read, true}, {strict_write, true}]),
%%     case thrift_protocol:read(Proto, Type) of
%%         {_NewProto, {ok, Result}} ->
%%             Result;
%%         {_NewProto, {error, Reason}} ->
%%             erlang:error({thrift, {protocol, Reason}})
%%     end.
