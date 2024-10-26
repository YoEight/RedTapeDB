using FluentAssertions;
using RedTape.Engine.Index;

namespace RedTape.Engine.Tests.Index;

public class MemTableTests
{
    [Fact]
    public void ShouldLookUp()
    {
        var table = new MemTable(0);

        table.Put(1, 0, 1);
        table.Put(2, 0, 2);
        table.Put(3, 0, 3);

        Assert.True(table.TryGet(1, 0, out var aPos));
        Assert.True(table.TryGet(2, 0, out var bPos));
        Assert.True(table.TryGet(3, 0, out var cPos));

        aPos.Should().Be(1);
        bPos.Should().Be(2);
        cPos.Should().Be(3);
    }

    [Fact]
    public void ShouldForwardScan()
    {
        var table = new MemTable(0);

        table.Put(1, 0, 0);
        table.Put(1, 1, 5);
        table.Put(1, 2, 10);

        using var iter = table.ScanForward(1, StreamRevision.Start).GetEnumerator();

        iter.MoveNext();
        iter.Current.Stream.Should().Be(1);
        iter.Current.Revision.Should().Be(0);
        iter.Current.Position.Should().Be(0);

        iter.MoveNext();
        iter.Current.Stream.Should().Be(1);
        iter.Current.Revision.Should().Be(1);
        iter.Current.Position.Should().Be(5);

        iter.MoveNext();
        iter.Current.Stream.Should().Be(1);
        iter.Current.Revision.Should().Be(2);
        iter.Current.Position.Should().Be(10);
    }

    [Fact]
    public void ShouldForwardScanFromRevision()
    {
        var table = new MemTable(0);

        table.Put(1, 0, 0);
        table.Put(1, 1, 5);
        table.Put(1, 2, 10);

        using var iter = table.ScanForward(1, StreamRevision.Revision(1)).GetEnumerator();

        iter.MoveNext();
        iter.Current.Stream.Should().Be(1);
        iter.Current.Revision.Should().Be(1);
        iter.Current.Position.Should().Be(5);

        iter.MoveNext();
        iter.Current.Stream.Should().Be(1);
        iter.Current.Revision.Should().Be(2);
        iter.Current.Position.Should().Be(10);
    }

    [Fact]
    public void ShouldBackwardScan()
    {
        var table = new MemTable(0);

        table.Put(1, 0, 0);
        table.Put(1, 1, 5);
        table.Put(1, 2, 10);

        using var iter = table.ScanBackward(1, StreamRevision.End).GetEnumerator();

        iter.MoveNext();
        iter.Current.Stream.Should().Be(1);
        iter.Current.Revision.Should().Be(2);
        iter.Current.Position.Should().Be(10);

        iter.MoveNext();
        iter.Current.Stream.Should().Be(1);
        iter.Current.Revision.Should().Be(1);
        iter.Current.Position.Should().Be(5);

        iter.MoveNext();
        iter.Current.Stream.Should().Be(1);
        iter.Current.Revision.Should().Be(0);
        iter.Current.Position.Should().Be(0);
    }

    [Fact]
    public void ShouldBackwardScanFromRevision()
    {
        var table = new MemTable(0);

        table.Put(1, 0, 0);
        table.Put(1, 1, 5);
        table.Put(1, 2, 10);

        using var iter = table.ScanBackward(1, StreamRevision.Revision(1)).GetEnumerator();

        iter.MoveNext();
        iter.Current.Stream.Should().Be(1);
        iter.Current.Revision.Should().Be(1);
        iter.Current.Position.Should().Be(5);

        iter.MoveNext();
        iter.Current.Stream.Should().Be(1);
        iter.Current.Revision.Should().Be(0);
        iter.Current.Position.Should().Be(0);
    }
}