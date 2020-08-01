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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using log4net;
using MiNET.LevelDB.Utils;

namespace MiNET.LevelDB
{
	public class BlockEnumerator : IEnumerator<BlockEntry>
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(BlockEnumerator));

		private readonly ReadOnlyMemory<byte> _blockData;
		private BytewiseComparator _comparator = new BytewiseComparator();
		private int _restartOffset;
		private int _restartCount;
		private int _position;

		public BlockEnumerator(ReadOnlyMemory<byte> blockData)
		{
			_blockData = blockData;

			Initialize();
		}

		private void Initialize()
		{
			var reader = new SpanReader(_blockData.Span);
			reader.Seek(-4, SeekOrigin.End);
			_restartCount = (int) reader.ReadUInt32();
			Log.Warn($"Got {_restartCount} restart points");
			reader.Seek(-((1 + _restartCount) * sizeof(uint)), SeekOrigin.End);
			_restartOffset = reader.Position;

			Reset();
		}

		private bool HasNext()
		{
			return _position < _restartOffset;
		}

		public bool MoveNext()
		{
			if (!HasNext())
			{
				Current = null;
				return false;
			}

			return TryParseCurrentEntry();
		}

		public void Reset()
		{
			Current = null;
			SeekToRestartPoint(0);
		}

		public BlockEntry Current { get; private set; }

		object? IEnumerator.Current => Current;

		internal void SeekToRestartPoint(int restartIndex)
		{
			// Find offset from restart index
			uint offset = GetRestartPoint(restartIndex);
			_position = (int) offset;
			TryParseCurrentEntry();
		}

		private uint GetRestartPoint(int index)
		{
			if (index >= _restartCount) throw new IndexOutOfRangeException(nameof(index));

			var reader = new SpanReader(_blockData.Span);
			reader.Seek(_restartOffset + index * sizeof(uint), SeekOrigin.Begin);
			return reader.ReadUInt32();
		}

		private bool TryParseCurrentEntry()
		{
			if (!HasNext()) return false;

			var reader = new SpanReader(_blockData.Span) {Position = _position};

			// An entry for a particular key-value pair has the form:
			//     shared_bytes: varint32
			var sharedBytes = reader.ReadVarLong();
			//     unshared_bytes: varint32
			var nonSharedBytes = reader.ReadVarLong();
			//     value_length: varint32
			var valueLength = reader.ReadVarLong();
			//     key_delta: char[unshared_bytes]
			ReadOnlyMemory<byte> keyDelta = reader.Read(nonSharedBytes).ToArray();

			var entry = new BlockEntry();
			Memory<byte> combinedKey = new byte[sharedBytes + nonSharedBytes];
			if (sharedBytes > 0)
			{
				if (Current == null) throw new Exception("Faulty state. Got shared key, but had no current entry.");
				Current.Key.Slice(0, (int) sharedBytes).CopyTo(combinedKey.Slice(0, (int) sharedBytes));
			}
			keyDelta.Slice(0, (int) nonSharedBytes).CopyTo(combinedKey.Slice((int) sharedBytes, (int) nonSharedBytes));
			entry.Key = combinedKey;

			entry.Data = reader.Read(valueLength).ToArray();

			// Position stream after value of this entry
			_position = reader.Position;

			Current = entry;

			return true;
		}

		public void Dispose()
		{
		}
	}
}