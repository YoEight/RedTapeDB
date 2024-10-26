using FluentAssertions;
using RedTape.Engine.Index;

namespace RedTape.Engine.Tests.Index;

public class MergeTests
{
    [Fact]
    public void TestMerge_1()
    {
        var mem1 = Build(0, (1, 0, 1), (2, 0, 2), (3, 0, 3));
        var mem2 = Build(1, (1, 0, 2), (2, 0, 4), (3, 0, 6), (4, 0, 8));
        var mem3 = Build(2, (2, 0, 12), (3, 0, 18), (4, 0, 24));

        var merge = new Merge([mem1, mem2, mem3]);
        Check(merge, (1, 0, 1), (2, 0, 2), (3, 0, 3), (4, 0, 8));

        merge = new Merge([mem3, mem1, mem2]);
        Check(merge, (1, 0, 1), (2, 0, 12), (3, 0, 18), (4, 0, 24));
    }

    [Fact]
    public void TestMerge_2()
    {
        var mem1 = Build(0, (1, 0, 11), (2, 0, 12), (3, 0, 13));
        var mem2 = Build(1, (4, 0, 21), (5, 0, 22), (6, 0, 23), (7, 0, 24));
        var mem3 = Build(2, (8, 0, 31), (9, 0, 32), (10, 0, 33), (11, 0, 34));
        var mem4 = Build(3, []);

        var merge = new Merge([
            mem1,
            mem2,
            mem3,
            mem4,
        ]);

        List<(ulong, ulong, ulong)> result = [
            (1, 0, 11),
            (2, 0, 12),
            (3, 0, 13),
            (4, 0, 21),
            (5, 0, 22),
            (6, 0, 23),
            (7, 0, 24),
            (8, 0, 31),
            (9, 0, 32),
            (10, 0, 33),
            (11, 0, 34),
        ];

        Check(merge, result.ToArray());

        merge = new Merge([
            mem2,
            mem4,
            mem3,
            mem1,
        ]);

        Check(merge, result.ToArray());

        merge = new Merge([
            mem4,
            mem3,
            mem2,
            mem1,
        ]);

        Check(merge, result.ToArray());
    }

    private static MemTable Build(ulong id, params (ulong, ulong, ulong)[] values)
    {
        var mem = new MemTable(id);

        foreach (var (stream, revision, pos) in values)
            mem.Put(stream, revision, pos);

        return mem;
    }

    private static void Check(Merge merge, params (ulong, ulong, ulong)[] expectations)
    {
        var enumerator = expectations.GetEnumerator();
        foreach (var actual in merge)
        {
            enumerator.MoveNext().Should().BeTrue();
            var (stream, revision, pos) = ((ulong, ulong, ulong)) enumerator.Current;

            actual.Stream.Should().Be(stream);
            actual.Revision.Should().Be(revision);
            actual.Position.Should().Be(pos);
        }
    }
}