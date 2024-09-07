using System.Threading.Tasks;
using Geth;
using Grpc.Core;
using RedTape.Engine;

namespace RedTape.Node.Services;

public class ProtocolService(IMultiplexer multiplexer) : Protocol.ProtocolBase
{
    public override async Task Multiplex(IAsyncStreamReader<OperationIn> requestStream, IServerStreamWriter<OperationOut> responseStream, ServerCallContext context)
    {
        await foreach (var output in multiplexer.Multiplex(requestStream.ReadAllAsync(context.CancellationToken)))
            await responseStream.WriteAsync(output, context.CancellationToken);
    }
}