using Geth;

namespace RedTape.Engine;

public abstract class StreamRevision
{
    public static StreamRevision Start { get; } = new StartStreamRevision();
    public static StreamRevision End { get; } = new EndStreamRevision();
    public static StreamRevision Revision(ulong value) => new RevisionStreamRevision(value);

    public abstract void Visit<T>(ref T visitor) where T: IStreamRevisionVisitor;

    private class StartStreamRevision : StreamRevision
    {
        public override void Visit<T>(ref T visitor) => visitor.Start();
    }

    private class EndStreamRevision: StreamRevision
    {
        public override void Visit<T>(ref T visitor) => visitor.End();
    }

    private class RevisionStreamRevision(ulong value) : StreamRevision
    {
        public override void Visit<T>(ref T visitor) => visitor.Revision(value);
    }
}

public interface IStreamRevisionVisitor
{
    void End();
    void Start();
    void Revision(ulong revision);
}