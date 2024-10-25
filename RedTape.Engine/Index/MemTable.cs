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

    public IEnumerable<Entry> ScanForward(ulong key, StreamRevision starting)
    {
        if (!_inner.TryGetValue(key, out var entries))
            yield break;

        var anchor = new FromStartAnchor();
        starting.Visit(ref anchor);

        if (anchor.Value == null)
            yield break;

        foreach (var (revision, position) in entries)
        {
            if (revision < anchor.Value)
                continue;

            yield return new Entry(key, revision, position);
        }
    }

    public IEnumerable<Entry> ScanBackward(ulong key, StreamRevision starting)
    {
        if (!_inner.TryGetValue(key, out var entries))
            yield break;

        var anchor = new FromEndAnchor();
        starting.Visit(ref anchor);

        if (anchor.Value == null)
            yield break;

        foreach (var (revision, position) in entries.Reverse())
        {
            if (revision > anchor.Value)
                continue;

            yield return new Entry(key, revision, position);
        }
    }

    private struct FromStartAnchor : IStreamRevisionVisitor
    {
        public ulong? Value { get; private set; }

        public void End()
        {
            Value = null;
        }

        public void Start()
        {
            Value = 0;
        }

        public void Revision(ulong revision)
        {
            Value = revision;
        }
    }

    private struct FromEndAnchor : IStreamRevisionVisitor
    {
        public ulong? Value { get; private set; }

        public void End()
        {
            Value = ulong.MaxValue;
        }

        public void Start()
        {
            Value = null;
        }

        public void Revision(ulong revision)
        {
            Value = revision;
        }
    }
}