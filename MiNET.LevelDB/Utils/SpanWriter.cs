using System;
using System.IO;

namespace MiNET.LevelDB.Utils
{
	public ref struct SpanWriter
	{
		private Span<byte> _buffer;

		public int Position { get; set; }
		public int Length => _buffer.Length;
		public bool Eof => Position >= _buffer.Length;

		public SpanWriter(Span<byte> buffer)
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

		public void Write(Span<byte> value)
		{
			value.CopyTo(_buffer.Slice(Position));
			Position += value.Length;
		}

		public void Write(byte value)
		{
			_buffer[Position] = value;
			Position++;
		}

		public void Write(short value)
		{
			_buffer[Position++] = (byte) value;
			_buffer[Position++] = (byte) (value >> 8);
		}

		public void Write(ushort value)
		{
			_buffer[Position++] = (byte) value;
			_buffer[Position++] = (byte) (value >> 8);
		}

		public void Write(int value)
		{
			_buffer[Position++] = (byte) value;
			_buffer[Position++] = (byte) (value >> 8);
			_buffer[Position++] = (byte) (value >> 16);
			_buffer[Position++] = (byte) (value >> 24);
		}

		public void Write(uint value)
		{
			_buffer[Position++] = (byte) value;
			_buffer[Position++] = (byte) (value >> 8);
			_buffer[Position++] = (byte) (value >> 16);
			_buffer[Position++] = (byte) (value >> 24);
		}

		public void Write(long value)
		{
			_buffer[Position++] = (byte) value;
			_buffer[Position++] = (byte) (value >> 8);
			_buffer[Position++] = (byte) (value >> 16);
			_buffer[Position++] = (byte) (value >> 24);
			_buffer[Position++] = (byte) (value >> 32);
			_buffer[Position++] = (byte) (value >> 40);
			_buffer[Position++] = (byte) (value >> 48);
			_buffer[Position++] = (byte) (value >> 56);
		}

		public void Write(ulong value)
		{
			_buffer[Position++] = (byte) value;
			_buffer[Position++] = (byte) (value >> 8);
			_buffer[Position++] = (byte) (value >> 16);
			_buffer[Position++] = (byte) (value >> 24);
			_buffer[Position++] = (byte) (value >> 32);
			_buffer[Position++] = (byte) (value >> 40);
			_buffer[Position++] = (byte) (value >> 48);
			_buffer[Position++] = (byte) (value >> 56);
		}

		public void WriteVarInt(ulong value)
		{
			var num = value;
			while (num >= 0x80U)
			{
				_buffer[Position++] = ((byte) (num | 0x80U));
				num >>= 7;
			}
			_buffer[Position++] = (byte) num;
		}
	}
}