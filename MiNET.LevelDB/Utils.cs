using System;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Text;

namespace MiNET.LevelDB
{
	public class LevelLbUtils
	{
		public static string HexDump(byte[] bytes, int bytesPerLine = 16, bool printLineCount = false, bool printText = true, bool cutAfterFive = false)
		{
			StringBuilder sb = new StringBuilder();
			for (int line = 0; line < bytes.Length; line += bytesPerLine)
			{
				if (cutAfterFive && line >= bytesPerLine*5)
				{
					sb.AppendLine(".. output cut after 5 lines");
					break;
				}

				byte[] lineBytes = bytes.Skip(line).Take(bytesPerLine).ToArray();
				if (printLineCount) sb.AppendFormat("{0:x8} ", line);
				sb.Append(string.Join(" ", lineBytes.Select(b => b.ToString("x2"))
						.ToArray())
					.PadRight(bytesPerLine*3));
				if (printText)
				{
					sb.Append(" ");
					sb.Append(new string(lineBytes.Select(b => b < 32 ? '.' : (char) b)
						.ToArray()));
				}
				if (bytesPerLine < bytes.Length)
					sb.AppendLine();
			}

			return sb.ToString();
		}

		public static ulong ReadVarint(Stream sliceInput)
		{
			ulong result = 0;
			for (int shift = 0; shift <= 63; shift += 7)
			{
				ulong b = (ulong) sliceInput.ReadByte();

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

		public static string ReadLengthPrefixedString(Stream seek)
		{
			return Encoding.UTF8.GetString(ReadLengthPrefixedBytes(seek));
		}

		public static byte[] ReadLengthPrefixedBytes(Stream seek)
		{
			ulong size = seek.ReadVarint();
			byte[] buffer = new byte[size];
			seek.Read(buffer, 0, buffer.Length);
			return buffer;
		}
	}

	public static class LevelDbHelpers
	{
		public static Span<byte> UserKey(this Span<byte> fullKey)
		{
			return fullKey.Slice(0, fullKey.Length - 8);
		}

		public static Span<byte> UserKey(this byte[] fullKey)
		{
			return fullKey.AsSpan(0, fullKey.Length - 8);
		}

		public static string ToHexString(this Span<byte> bytes)
		{
			return bytes.ToArray().HexDump(bytes.Length, cutAfterFive: true, printText: false, printLineCount: false);
		}

		public static string ToHexString(this ReadOnlySpan<byte> bytes)
		{
			return bytes.HexDump(bytes.Length, cutAfterFive: true, printText: false, printLineCount: false);
		}

		public static string HexDump(this byte[] value, int bytesPerLine = 16, bool printLineCount = false, bool printText = true, bool cutAfterFive = false)
		{
			return LevelLbUtils.HexDump(value, bytesPerLine, printLineCount, printText, cutAfterFive);
		}

		public static string HexDump(this ReadOnlySpan<byte> value, int bytesPerLine = 16, bool printLineCount = false, bool printText = true, bool cutAfterFive = false)
		{
			return LevelLbUtils.HexDump(value.ToArray(), bytesPerLine, printLineCount, printText, cutAfterFive);
		}

		public static ulong ReadVarint(this Stream sliceInput)
		{
			return LevelLbUtils.ReadVarint(sliceInput);
		}

		public static string ReadLengthPrefixedString(this Stream seek)
		{
			return LevelLbUtils.ReadLengthPrefixedString(seek);
		}

		public static byte[] ReadLengthPrefixedBytes(this Stream seek)
		{
			return LevelLbUtils.ReadLengthPrefixedBytes(seek);
		}
	}

	public class BytewiseComparator
	{
		public string Name { get; } = "leveldb.BytewiseComparator";

		public int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
		{
			if (a.Length == b.Length)
			{
				var result = a.SequenceCompareTo(b);
				return result == 0 ? 0 : result > 0 ? 1 : -1;
			}
			else
			{
				var maxLen = Math.Min(a.Length, b.Length);
				var result = a.Slice(0, maxLen).SequenceCompareTo(b.Slice(0, maxLen));
				if (result != 0) return result > 0 ? 1 : -1;

				result = a.Length - b.Length;
				return result > 0 ? 1 : -1;
			}
		}

		public void FindShortestSeparator(string start, Span<byte> limit)
		{
		}

		public void FindShortSuccessor(string key)
		{
		}
	}

	public ref struct SpanReader
	{
		private ReadOnlySpan<byte> _buffer;

		public int Position { get; set; }
		public int Length => _buffer.Length;

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

		public ReadOnlySpan<byte> Read(int length)
		{
			ReadOnlySpan<byte> bytes = _buffer.Slice(Position, length);
			Position += length;

			return bytes;
		}

		public uint ReadVarInt()
		{
			return ReadVarIntInternal();
		}

		public int ReadSignedVarInt()
		{
			return DecodeZigZag32((uint) ReadVarIntInternal());
		}

		private static int DecodeZigZag32(uint n)
		{
			return (int) (n >> 1) ^ -(int) (n & 1);
		}

		public ulong ReadVarLong()
		{
			return ReadVarLongInternal();
		}

		private uint ReadVarIntInternal()
		{
			uint result = 0;
			for (int shift = 0; shift <= 31; shift += 7)
			{
				byte b = _buffer[Position++];

				// add the lower 7 bits to the result
				result |= (uint) ((b & 0x7f) << shift);

				// if high bit is not set, this is the last byte in the number
				if ((b & 0x80) == 0)
				{
					return result;
				}
			}
			throw new Exception("last byte of variable length int has high bit set");
		}

		public ulong ReadVarLongInternal()
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
	}
}