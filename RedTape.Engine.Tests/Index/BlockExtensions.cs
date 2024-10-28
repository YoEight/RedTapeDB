using System.Diagnostics;
using RedTape.Engine.Index;

namespace RedTape.Engine.Tests.Index;

public static class BlockExtensions
{
    public static void Put(this Block block, params (ulong, ulong, ulong)[] values)
    {
        foreach (var (stream, revision, position) in values)
            Debug.Assert(block.TryAdd(stream, revision, position));
    }
}