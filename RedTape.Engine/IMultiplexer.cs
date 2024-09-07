using Geth;

namespace RedTape.Engine;

public interface IMultiplexer {
    IAsyncEnumerable<OperationOut> Multiplex(IAsyncEnumerable<OperationIn> inputs);
}