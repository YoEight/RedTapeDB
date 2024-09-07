using Geth;

namespace RedTape.Engine;

public class Multiplexer : IMultiplexer
{
    public IAsyncEnumerable<OperationOut> Multiplex(IAsyncEnumerable<OperationIn> inputs)
    {
        throw new NotImplementedException();
    }
}