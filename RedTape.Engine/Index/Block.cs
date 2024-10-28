using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Humanizer;

namespace RedTape.Engine.Index;

public class Block : IDisposable
{
    private static readonly ByteSize DefaultMaxSize = 4.Kilobytes();
    private static readonly int StreamKeySize = Marshal.SizeOf<ulong>();
    private static readonly int RevisionSize = Marshal.SizeOf<ulong>();
    private static readonly int PositionSize = Marshal.SizeOf<ulong>();
    private static readonly int OffsetSize = Marshal.SizeOf<short>();
    private static readonly int NumElementsSize = Marshal.SizeOf<short>();
    private static readonly int EntrySize = StreamKeySize + RevisionSize + PositionSize;

    private readonly byte[] _buffer;
    private readonly List<ushort> _offsets = [];
    private readonly int _blockSize;
    private int _count;
    private int _offset;
    private bool _frozen;
    private ulong? _firstKey;
    private ulong? _lastKey;

    public static Block From(ReadOnlySpan<byte> data)
    {
        var block = new Block();
        block._count = BitConverter.ToUInt16(data[^2..]);
        var offsetSection = data[^(block._count * OffsetSize + 2)..^2];

        while (!offsetSection.IsEmpty)
        {
            var offset = BitConverter.ToUInt16(offsetSection[..OffsetSize]);
            offsetSection = offsetSection[OffsetSize..];
            block._offsets.Add(offset);
        }

        data[..(block._count * EntrySize)].CopyTo(block._buffer);
        block._offset = block._count * EntrySize;

        if (block._offsets.Count != 0)
        {
            var firstOffset = block._offsets.First();
            block._firstKey = BitConverter.ToUInt64(data[firstOffset..(firstOffset + StreamKeySize)]);

            var lastOffset = block._offsets.Last();
            block._lastKey = BitConverter.ToUInt64(data[lastOffset..(lastOffset + StreamKeySize)]);
        }

        return block;
    }

    public static int GetMaxBlockSize(int maxEntryCount) => maxEntryCount * (EntrySize + OffsetSize) + NumElementsSize;

    private int EstimatedSize => _count * (EntrySize + OffsetSize) + NumElementsSize;

    public ReadOnlySpan<byte> Data => _buffer.AsSpan(0, _offset);

    public int Count => _count;

    public bool IsFull => EstimatedSize >= _blockSize;

    public Block(int? blockSize = null)
    {
        _blockSize = blockSize ?? (int)DefaultMaxSize.Bytes;
        _buffer = ArrayPool<byte>.Shared.Rent(_blockSize);
    }

    public bool TryAdd(ulong stream, ulong revision, ulong position)
    {
        Debug.Assert(!_frozen);

        if (EstimatedSize + EntrySize > _blockSize)
            return false;

        _firstKey ??= stream;
        _lastKey = stream;

        _offsets.Add((ushort)_offset);

        using var writer = new BinaryWriter(new MemoryStream(_buffer, _offset, EntrySize));
        writer.Write(stream);
        writer.Write(revision);
        writer.Write(position);

        _offset += EntrySize;
        _count++;

        return true;
    }

    public bool TryRead(int index, out Entry? entry)
    {
        entry = null;

        if (index > _count - 1)
            return false;

        var offset = _offsets[index];
        using var reader = new BinaryReader(new MemoryStream(_buffer, offset, EntrySize));

        entry = new Entry(reader.ReadUInt64(), reader.ReadUInt64(), reader.ReadUInt64());
        return true;
    }


    public IEnumerable<Entry> ScanForward(ulong key, ulong start, ulong length)
    {
        if (length == 0)
            yield break;

        if (key < _firstKey || key > _lastKey)
            yield break;

        var end = start > ulong.MaxValue - length ? ulong.MaxValue : start + length;
        for (var index = 0; index < _count; index++)
        {
            if (!TryRead(index, out var entry))
                throw new IndexOutOfRangeException();

            if (entry!.Value.Stream > key)
                yield break;

            if (entry.Value.Stream < key)
                continue;

            if (entry.Value.Revision < start)
                continue;

            if (entry.Value.Revision >= end)
                yield break;

            yield return entry.Value;
        }
    }

    public IEnumerable<Entry> ScanBackward(ulong key, ulong start, ulong length)
    {
        if (length == 0)
            yield break;

        if (key < _firstKey || key > _lastKey)
            yield break;

        var end = length >= start ? 0 : start - length;

        for (var index = _count - 1; index >= 0; index--)
        {
            if (!TryRead(index, out var entry))
                throw new IndexOutOfRangeException();

            if (entry!.Value.Stream > key)
                continue;

            if (entry.Value.Stream < key)
                yield break;

            if (entry.Value.Revision > start)
                continue;

            if (entry.Value.Revision < end)
                yield break;

            yield return entry.Value;
        }
    }

    public void Freeze()
    {
        Debug.Assert(!_frozen);
        _frozen = true;

        var requiredSpace = _count * OffsetSize + NumElementsSize;
        using var writer = new BinaryWriter(new MemoryStream(_buffer, _offset, requiredSpace));

        foreach (var offset in _offsets)
            writer.Write(offset);

        writer.Write((ushort)_count);
        _offset += requiredSpace;
    }

    public void Dispose()
    {
        ArrayPool<byte>.Shared.Return(_buffer);
    }
}