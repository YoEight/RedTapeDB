using System.Collections;
using Humanizer;

namespace RedTape.Engine.Index;

public class MemTable(ulong id) : IEnumerable<Entry>
{
    private static readonly int MaxSize = 1.Millions();

    private readonly SortedDictionary<ulong, SortedList<ulong, ulong>> _inner = new();

    public ulong Id { get; init; } = id;
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

    public IEnumerable<Entry> Merge(params MemTable[] tables)
    {
        List<MemTable> candidates = [this];
        candidates.AddRange(tables);

        return new Merge(candidates);
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

    public IEnumerator<Entry> GetEnumerator()
    {
        foreach (var stream in _inner)
        {
            foreach (var entry in stream.Value)
                yield return new Entry(stream.Key, entry.Key, entry.Value);
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

public class Merge(List<MemTable> tables) : IEnumerable<Entry>
{
    private readonly IEnumerator<Entry>[] _enumerators = tables.Select(x => x.GetEnumerator()).ToArray();
    private readonly Entry?[] _caches = new Entry?[tables.Count];

    public IEnumerator<Entry> GetEnumerator()
    {
        while (Fill())
            yield return Pull();
    }

    private bool Fill()
    {
        var found = false;

        for (var index = 0; index < tables.Count; index++)
        {
            if (_caches[index] == null)
            {
                if (!_enumerators[index].MoveNext())
                    continue;

                _caches[index] = _enumerators[index].Current;
            }

            found = true;
        }

        return found;
    }

    private Entry Pull()
    {
        Entry? lower = null;
        var foundIndex = -1;

        for (var index = 0; index < tables.Count; index++)
        {
            if (lower == null)
            {
                lower = _caches[index];
                foundIndex = index;
                continue;
            }

            if (_caches[index] == null)
                continue;

            switch (lower.Value.CompareKeyTo(_caches[index]!.Value))
            {
                case Ordering.Greater:
                    lower = _caches[index];
                    foundIndex = index;
                    break;

                case Ordering.Equal:
                    // If two entries have the same key, the leftmost memory table takes precedence.
                    _caches[index] = null;
                    break;

                case Ordering.Less:
                default:
                    continue;
            }
        }

        _caches[foundIndex] = null;

        return lower!.Value;
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}