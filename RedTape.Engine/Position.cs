using Geth;

namespace RedTape.Engine;

public abstract class Position
{
    public static Position Start => new StartPosition();
    public static Position End => new EndPosition();
    public static Position Revision(ulong value) => new RevisionPosition(value);

    public abstract void Deconstruct(out bool start, out bool end, out ulong value);

    private class StartPosition : Position
    {
        public override void Deconstruct(out bool start, out bool end, out ulong value)
        {
            start = true;
            end = false;
            value = 0;
        }
    }

    private class EndPosition: Position
    {
        public override void Deconstruct(out bool start, out bool end, out ulong value)
        {
            start = false;
            end = true;
            value = 0;
        }
    }

    private class RevisionPosition(ulong value) : Position
    {
        public override void Deconstruct(out bool start, out bool end, out ulong value1)
        {
            start = false;
            end = false;
            value1 = value;
        }
    }
}