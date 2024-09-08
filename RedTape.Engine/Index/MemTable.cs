namespace RedTape.Engine.Index;

public class MemTable
{
    private const long MaxSize = 1_000_000;

    private readonly SortedDictionary<ulong, SortedList<ulong, ulong>> _inner = new();
    public int Count { get; private set; }

    public void Put(ulong stream, ulong revision, ulong position)
    {
        Count++;

        if (!_inner.TryGetValue(stream, out var entries))
        {
            entries = new SortedList<ulong, ulong> { { revision, position } };
            _inner.Add(stream, entries);
            return;
        }

        entries.Add(stream, revision);
    }

    public bool TryGet(ulong stream, ulong revision, out ulong position)
    {
        position = default;
        return _inner.TryGetValue(stream, out var entries)
               && entries.TryGetValue(revision, out position);
    }
}