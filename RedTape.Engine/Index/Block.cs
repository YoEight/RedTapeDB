using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Humanizer;

namespace RedTape.Engine.Index;

public class Block : IDisposable
{
    private static readonly ByteSize MaxSize = 4.Kilobytes();
    private static readonly int StreamKeySize = Marshal.SizeOf<ulong>();
    private static readonly int RevisionSize = Marshal.SizeOf<ulong>();
    private static readonly int PositionSize = Marshal.SizeOf<ulong>();
    private static readonly int OffsetSize = Marshal.SizeOf<short>();
    private static readonly int NumElementsSize = Marshal.SizeOf<short>();
    private static readonly int EntrySize = StreamKeySize + RevisionSize + PositionSize;

    private readonly byte[] _buffer = ArrayPool<byte>.Shared.Rent((int)MaxSize.Bytes);
    private readonly List<short> _offsets = [];
    private int _count;
    private int _offset;
    private bool _frozen;

    private int EstimatedSize => _count * (EntrySize + OffsetSize) + NumElementsSize;

    public ReadOnlySpan<byte> Data => _buffer.AsSpan(0, _offset);

    public bool TryAdd(ulong stream, ulong revision, ulong position)
    {
        Debug.Assert(!_frozen);

        if (EstimatedSize + EntrySize > MaxSize.Bytes)
            return false;

        _offsets.Add((short)_offset);

        using var writer = new BinaryWriter(new MemoryStream(_buffer, _offset, EntrySize));
        writer.Write(stream);
        writer.Write(revision);
        writer.Write(position);

        _offset += EntrySize;
        _count++;

        return true;
    }

    public void Freeze()
    {
        Debug.Assert(!_frozen);
        _frozen = true;

        var requiredSpace = _count * OffsetSize + NumElementsSize;
        using var writer = new BinaryWriter(new MemoryStream(_buffer, _offset, requiredSpace));

        foreach (var offset in _offsets)
            writer.Write(offset);

        writer.Write(_count);
        _offset += requiredSpace;
    }

    public void Dispose()
    {
        ArrayPool<byte>.Shared.Return(_buffer);
    }
}