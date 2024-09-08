namespace RedTape.Engine.Index;

public class MemTable
{
    private const long MaxSize = 1_000_000;

    private readonly SortedDictionary<ulong, SortedList<ulong, ulong>> _inner = new();
    public int Count { get; private set; }

    public void Put(ulong key, ulong revision, ulong position)
    {
        Count++;

        if (!_inner.TryGetValue(key, out var entries))
        {
            entries = new SortedList<ulong, ulong> { { revision, position } };
            _inner.Add(key, entries);
            return;
        }

        entries.Add(revision, position);
    }

    public bool TryGet(ulong key, ulong revision, out ulong position)
    {
        position = default;
        return _inner.TryGetValue(key, out var entries)
               && entries.TryGetValue(revision, out position);
    }

    public IEnumerable<Entry> ScanForward(ulong key, Position starting)
    {
        if (!_inner.TryGetValue(key, out var entries))
            yield break;

        ulong? anchor = starting switch
        {
            (true, _, _) => 0,
            (_, true, _) => null,
            var (_, _, pos) => pos,
        };

        if (anchor == null)
            yield break;

        foreach (var (revision, position) in entries)
        {
            if (revision < anchor)
                continue;

            yield return new Entry(key, revision, position);
        }
    }

    public IEnumerable<Entry> ScanBackward(ulong key, Position starting)
    {
        if (!_inner.TryGetValue(key, out var entries))
            yield break;

        ulong? anchor = starting switch
        {
            (true, _, _) => null,
            (_, true, _) => ulong.MaxValue,
            var (_, _, pos) => pos,
        };

        if (anchor == null)
            yield break;

        foreach (var (revision, position) in entries.Reverse())
        {
            if (revision > anchor)
                continue;

            yield return new Entry(key, revision, position);
        }
    }
}