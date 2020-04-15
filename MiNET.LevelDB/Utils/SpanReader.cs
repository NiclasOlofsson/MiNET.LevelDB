using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;

namespace MiNET.LevelDB.Utils
{
	public ref struct SpanReader
	{
		private ReadOnlySpan<byte> _buffer;

		public int Position { get; set; }
		public int Length => _buffer.Length;
		public bool Eof => Position >= _buffer.Length;

		public SpanReader(ReadOnlySpan<byte> buffer)
		{
			_buffer = buffer;
			Position = 0;
		}

		public int Seek(int offset, SeekOrigin origin)
		{
			if (offset > Length) throw new ArgumentOutOfRangeException(nameof(offset), "offset longer than stream");

			var tempPosition = Position;
			switch (origin)
			{
				case SeekOrigin.Begin:
					tempPosition = offset;
					break;
				case SeekOrigin.Current:
					tempPosition += offset;
					break;
				case SeekOrigin.End:
					tempPosition = Length + offset;
					break;
				default:
					throw new ArgumentOutOfRangeException(nameof(origin), origin, null);
			}

			if (tempPosition < 0) throw new IOException("Seek before beginning of stream");

			Position = tempPosition;

			return Position;
		}


		public byte ReadByte()
		{
			byte val = _buffer[Position];
			Position++;
			return val;
		}

		public int ReadInt32()
		{
			int val = BinaryPrimitives.ReadInt32LittleEndian(_buffer.Slice(Position, 4));
			Position += 4;
			return val;
		}

		public uint ReadUInt32()
		{
			uint val = BinaryPrimitives.ReadUInt32LittleEndian(_buffer.Slice(Position, 4));
			Position += 4;
			return val;
		}

		public long ReadInt64()
		{
			long val = BinaryPrimitives.ReadInt64LittleEndian(_buffer.Slice(Position, 8));
			Position += 8;
			return val;
		}

		public ulong ReadUInt64()
		{
			ulong val = BinaryPrimitives.ReadUInt64LittleEndian(_buffer.Slice(Position, 8));
			Position += 8;
			return val;
		}

		//public uint ReadVarInt()
		//{
		//	return ReadVarIntInternal();
		//}

		//public int ReadSignedVarInt()
		//{
		//	return DecodeZigZag32((uint) ReadVarIntInternal());
		//}

		private static int DecodeZigZag32(uint n)
		{
			return (int) (n >> 1) ^ -(int) (n & 1);
		}

		public ulong ReadVarLong()
		{
			return ReadVarLongInternal();
		}

		public string ReadLengthPrefixedString()
		{
			return Encoding.UTF8.GetString(ReadLengthPrefixedBytes());
		}

		public ReadOnlySpan<byte> ReadLengthPrefixedBytes()
		{
			var length = ReadVarLongInternal();
			return Read(length);
		}

		private ulong ReadVarLongInternal()
		{
			ulong result = 0;
			for (int shift = 0; shift <= 63; shift += 7)
			{
				ulong b = _buffer[Position++];

				// add the lower 7 bits to the result
				result |= ((b & 0x7f) << shift);

				// if high bit is not set, this is the last byte in the number
				if ((b & 0x80) == 0)
				{
					return result;
				}
			}

			throw new Exception("last byte of variable length int has high bit set");
		}

		public ReadOnlySpan<byte> Read(ulong offset, ulong count)
		{
			long n = Length - Position;
			if (n > (long) count)
				n = (long) count;
			if (n <= 0)
				return ReadOnlySpan<byte>.Empty;

			Span<byte> result = new byte[offset + count];

			Read(n).CopyTo(result.Slice((int) offset, (int) n));

			return result;
		}

		public ReadOnlySpan<byte> Read(ulong length)
		{
			return Read((int) length);
		}

		public ReadOnlySpan<byte> Read(long length)
		{
			return Read((int) length);
		}

		public ReadOnlySpan<byte> Read(int length)
		{
			if (length > Length - Position) throw new ArgumentOutOfRangeException(nameof(length), length, $"Value outside of range: {Length - Position}");

			ReadOnlySpan<byte> bytes = _buffer.Slice(Position, length);
			Position += length;

			return bytes;
		}
	}
}