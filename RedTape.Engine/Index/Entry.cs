namespace RedTape.Engine.Index;

public readonly struct Entry : IComparable<Entry>
{
    public readonly ulong Stream;
    public readonly ulong Revision;
    public readonly ulong Position;

    public Entry(ulong stream, ulong revision, ulong position)
    {
        Stream = stream;
        Revision = revision;
        Position = position;
    }

    public int CompareTo(Entry other)
    {
        var streamComparison = Stream.CompareTo(other.Stream);
        if (streamComparison != 0) return streamComparison;
        var revisionComparison = Revision.CompareTo(other.Revision);
        if (revisionComparison != 0) return revisionComparison;
        return Position.CompareTo(other.Position);
    }

    public Ordering CompareKeyTo(Entry other)
    {
        switch (Stream.CompareTo(other.Stream))
        {
            case -1:
                return Ordering.Less;
            case 1:
                return Ordering.Greater;
            default:
                return Revision.CompareTo(other.Revision) switch
                {
                    -1 => Ordering.Less,
                    1 => Ordering.Greater,
                    _ => Ordering.Equal
                };
        }
    }
}