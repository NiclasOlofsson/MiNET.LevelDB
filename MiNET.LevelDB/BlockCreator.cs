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
using System.Collections.Generic;
using System.IO;
using log4net;
using MiNET.LevelDB.Utils;

namespace MiNET.LevelDB
{
	/// <summary>
	///     An entry for a particular key-value pair has the form:
	///     shared_bytes: varint32
	///     unshared_bytes: varint32
	///     value_length: varint32
	///     key_delta: char[unshared_bytes]
	///     value: char[value_length]
	///     shared_bytes == 0 for restart points.
	///     The trailer of the block has the form:
	///     restarts: uint32[num_restarts]
	///     num_restarts: uint32
	///     restarts[i] contains the offset within the block of the ith restart point.
	/// </summary>
	public class BlockCreator
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(BlockCreator));

		private List<uint> _restarts = new List<uint>() {0};
		private byte[] _lastKey = new byte[0];
		private MemoryStream _stream = new MemoryStream();
		private int _restartCounter = 0;

		public byte[] LastKey => _lastKey;
		public long CurrentSize => _stream.Position;

		public void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data)
		{
			if (key == ReadOnlySpan<byte>.Empty || key == null || key.Length == 0) throw new ArgumentException("Empty key");
			//if (key == ReadOnlySpan<byte>.Empty || key == null || key.Length == 0) return;

			int sharedLen = 0;
			if (_restartCounter < 16)
			{
				sharedLen = CountSharedBytes(_lastKey, key);
			}
			else
			{
				_restarts.Add((uint) _stream.Position);
				_restartCounter = 0;
			}

			// An entry for a particular key/value pair has the form:
			//     shared_bytes: varint32
			VarInt.WriteUInt64(_stream, (ulong) sharedLen);
			//     unshared_bytes: varint32
			VarInt.WriteUInt64(_stream, (ulong) (key.Length - sharedLen));
			//     value_length: varint32
			VarInt.WriteUInt64(_stream, (ulong) data.Length);
			//     key_delta: char[unshared_bytes]
			_stream.Write(key.Slice(sharedLen));
			//     data: char[unshared_bytes]
			_stream.Write(data);

			_lastKey = key.ToArray();
			_restartCounter++;
		}

		public byte[] Finish()
		{
			// Write uncompressed block
			foreach (uint restart in _restarts)
			{
				_stream.Write(BitConverter.GetBytes(restart));
			}

			_stream.Write(BitConverter.GetBytes((uint) _restarts.Count));
			byte[] result = _stream.ToArray();

			// Reset
			_stream.Position = 0;
			_stream.SetLength(0);
			_restartCounter = 0;
			_restarts.Clear();
			_restarts.Add(0); // first restart always at offset 0
			_lastKey = new byte[0];

			return result;
		}

		private int CountSharedBytes(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
		{
			int shared = 0;
			int minLength = Math.Min(a.Length, b.Length);

			while ((shared < minLength) && (a[shared] == b[shared]))
			{
				shared++;
			}

			return shared;
		}
	}
}