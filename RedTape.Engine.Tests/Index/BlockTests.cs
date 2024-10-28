using FluentAssertions;
using RedTape.Engine.Index;

namespace RedTape.Engine.Tests.Index;

public class BlockTests
{
    [Fact]
    public void ReadSingleKey()
    {
        var block = new Block();
        block.TryAdd(1, 1, 1).Should().BeTrue();
        block.TryRead(0, out var entry).Should().BeTrue();

        block.Count.Should().Be(1);
        entry.Should().NotBeNull();
        entry!.Value.Stream.Should().Be(1);
        entry.Value.Revision.Should().Be(1);
        entry.Value.Position.Should().Be(1);
    }

    [Fact]
    public void DealingWithFullBlock()
    {
        var block = new Block(Block.GetMaxBlockSize(1));

        block.TryAdd(1, 1, 1).Should().BeTrue();
        block.TryAdd(1, 2, 2).Should().BeFalse();
        block.Count.Should().Be(1);
    }

    [Fact]
    // We build a block until it's full. Then we flush its content and make sure that we can build back up another
    // block from it's serialized content.
    public void FullCycle()
    {
        var block = new Block(Block.GetMaxBlockSize(3));
        block.TryAdd(1, 1, 1).Should().BeTrue();
        block.TryAdd(1, 2, 2).Should().BeTrue();
        block.TryAdd(1, 3, 3).Should().BeTrue();
        block.IsFull.Should().BeTrue();
        block.Freeze();

        var deserializedBlock = Block.From(block.Data);

        deserializedBlock.TryRead(0, out var entry).Should().BeTrue();
        entry.Should().NotBeNull();
        entry!.Value.Stream.Should().Be(1);
        entry!.Value.Revision.Should().Be(1);
        entry!.Value.Position.Should().Be(1);

        deserializedBlock.TryRead(1, out entry).Should().BeTrue();
        entry.Should().NotBeNull();
        entry!.Value.Stream.Should().Be(1);
        entry!.Value.Revision.Should().Be(2);
        entry!.Value.Position.Should().Be(2);

        deserializedBlock.TryRead(2, out entry).Should().BeTrue();
        entry.Should().NotBeNull();
        entry!.Value.Stream.Should().Be(1);
        entry!.Value.Revision.Should().Be(3);
        entry!.Value.Position.Should().Be(3);
    }

    [Fact]
    public void ScanForwardSkipped()
    {
        var block = new Block();
        block.Put(
            (1, 0, 1),
            (1, 1, 2),
            (1, 2, 3),
            (2, 0, 4),
            (2, 2, 6),
            (3, 0, 7),
            (3, 1, 8),
            (3, 2, 9)
        );

        using var enumerator = block.ScanForward(2, 0, ulong.MaxValue).GetEnumerator();

        enumerator.MoveNext().Should().BeTrue();
        enumerator.Current.Stream.Should().Be(2);
        enumerator.Current.Revision.Should().Be(0);
        enumerator.Current.Position.Should().Be(4);

        enumerator.MoveNext().Should().BeTrue();
        enumerator.Current.Stream.Should().Be(2);
        enumerator.Current.Revision.Should().Be(2);
        enumerator.Current.Position.Should().Be(6);

        enumerator.MoveNext().Should().BeFalse();
    }

    [Fact]
    public void ScanBackwardSkipped()
    {
        var block = new Block();
        block.Put(
            (1, 0, 1),
            (1, 1, 2),
            (1, 2, 3),
            (2, 0, 4),
            (2, 2, 6),
            (3, 0, 7),
            (3, 1, 8),
            (3, 2, 9)
        );

        using var enumerator = block.ScanBackward(2, ulong.MaxValue, ulong.MaxValue).GetEnumerator();

        enumerator.MoveNext().Should().BeTrue();
        enumerator.Current.Stream.Should().Be(2);
        enumerator.Current.Revision.Should().Be(2);
        enumerator.Current.Position.Should().Be(6);

        enumerator.MoveNext().Should().BeTrue();
        enumerator.Current.Stream.Should().Be(2);
        enumerator.Current.Revision.Should().Be(0);
        enumerator.Current.Position.Should().Be(4);

        enumerator.MoveNext().Should().BeFalse();
    }

    [Fact]
    public void ScanForwardNotFound()
    {
        var block = new Block();
        block.Put((3, 0, 7), (3, 1, 8), (3, 2, 9));

        using var enumerator = block.ScanForward(2, 0, ulong.MaxValue).GetEnumerator();
        enumerator.MoveNext().Should().BeFalse();
    }

    [Fact]
    public void ScanBackwardNotFound()
    {
        var block = new Block();
        block.Put((3, 0, 7), (3, 1, 8), (3, 2, 9));

        using var enumerator = block.ScanBackward(2, ulong.MaxValue, ulong.MaxValue).GetEnumerator();
        enumerator.MoveNext().Should().BeFalse();
    }

    [Fact]
    public void ScanForwardAfterDeserialization()
    {
        var referenceBlock = new Block();
        referenceBlock.Put(
            (1, 0, 1),
            (1, 1, 2),
            (1, 2, 3),
            (2, 0, 4),
            (2, 2, 6),
            (3, 0, 7),
            (3, 1, 8),
            (3, 2, 9)
        );

        referenceBlock.Freeze();
        var block = Block.From(referenceBlock.Data);

        using var enumerator = block.ScanForward(2, 0, ulong.MaxValue).GetEnumerator();

        enumerator.MoveNext().Should().BeTrue();
        enumerator.Current.Stream.Should().Be(2);
        enumerator.Current.Revision.Should().Be(0);
        enumerator.Current.Position.Should().Be(4);

        enumerator.MoveNext().Should().BeTrue();
        enumerator.Current.Stream.Should().Be(2);
        enumerator.Current.Revision.Should().Be(2);
        enumerator.Current.Position.Should().Be(6);

        enumerator.MoveNext().Should().BeFalse();
    }

    [Fact]
    public void ScanBackwardAfterDeserialization()
    {
        var block = new Block();
        block.Put(
            (1, 0, 1),
            (1, 1, 2),
            (1, 2, 3),
            (2, 0, 4),
            (2, 2, 6),
            (3, 0, 7),
            (3, 1, 8),
            (3, 2, 9)
        );

        using var enumerator = block.ScanBackward(2, ulong.MaxValue, ulong.MaxValue).GetEnumerator();

        enumerator.MoveNext().Should().BeTrue();
        enumerator.Current.Stream.Should().Be(2);
        enumerator.Current.Revision.Should().Be(2);
        enumerator.Current.Position.Should().Be(6);

        enumerator.MoveNext().Should().BeTrue();
        enumerator.Current.Stream.Should().Be(2);
        enumerator.Current.Revision.Should().Be(0);
        enumerator.Current.Position.Should().Be(4);

        enumerator.MoveNext().Should().BeFalse();
    }
}