#region LICENSE

// The contents of this file are subject to the Common Public Attribution
// License Version 1.0. (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at
// https://github.com/NiclasOlofsson/MiNET/blob/master/LICENSE.
// The License is based on the Mozilla Public License Version 1.1, but Sections 14
// and 15 have been added to cover use of software over a computer network and
// provide for limited attribution for the Original Developer. In addition, Exhibit A has
// been modified to be consistent with Exhibit B.
// 
// Software distributed under the License is distributed on an "AS IS" basis,
// WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License for
// the specific language governing rights and limitations under the License.
// 
// The Original Code is MiNET.
// 
// The Original Developer is the Initial Developer.  The Initial Developer of
// the Original Code is Niclas Olofsson.
// 
// All portions of the code written by Niclas Olofsson are Copyright (c) 2014-2020 Niclas Olofsson.
// All Rights Reserved.

#endregion

using System;
using System.IO;
using System.Text;

namespace MiNET.LevelDB.Utils
{
	public ref struct SpanWriter
	{
		private readonly Span<byte> _buffer;

		public int Position { get; set; }
		public int Length => _buffer.Length;
		public bool Eof => Position >= _buffer.Length;

		public Span<byte> Buffer => _buffer;

		public SpanWriter(int size)
		{
			_buffer = new Span<byte>(new byte[size]);
			Position = 0;
		}

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

		public void Write(string value)
		{
			WriteVarLong((ulong) value.Length);
			Write(Encoding.UTF8.GetBytes(value));
		}

		public void WriteWithLen(Span<byte> value)
		{
			WriteVarLong((ulong) value.Length);
			Write(value);
		}

		public void WriteVarLong(int value)
		{
			WriteVarLong((ulong) value);
		}

		public void WriteVarLong(ulong value)
		{
			while ((value & 0xFFFFFFFFFFFFFF80) != 0)
			{
				_buffer[Position++] = ((byte) ((value & 0x7F) | 0x80));
				value >>= 7;
			}

			_buffer[Position++] = ((byte) value);
		}
	}
}