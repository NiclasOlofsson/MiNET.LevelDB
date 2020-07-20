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
using System.Diagnostics;
using System.IO;
using log4net;
using MiNET.LevelDB.Utils;

namespace MiNET.LevelDB
{
	internal ref struct BlockSeeker
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(BlockSeeker));

		private ReadOnlySpan<byte> _blockData;
		private SpanReader _reader;
		private int _restartOffset;
		private int _restartCount;
		private BytewiseComparator _comparator;
		private BlockHandle _lastValue;

		internal Span<byte> Key { get; private set; }
		internal ReadOnlySpan<byte> CurrentValue => GetCurrentValue();

		internal BlockSeeker(ReadOnlySpan<byte> blockData)
		{
			_blockData = blockData;
			_reader = new SpanReader(blockData);
			Key = null;
			_restartOffset = 0;
			_restartCount = 0;
			_comparator = new BytewiseComparator();
			_lastValue = null;

			Initialize();
		}

		private void Initialize()
		{
			var stream = new SpanReader(_blockData);
			stream.Seek(-4, SeekOrigin.End);
			_restartCount = (int) stream.ReadUInt32();
			Log.Warn($"Got {_restartCount} restart points");
			stream.Seek(-((1 + _restartCount) * sizeof(uint)), SeekOrigin.End);
			_restartOffset = stream.Position;
		}

		private ReadOnlySpan<byte> GetCurrentValue()
		{
			_reader.Position = (int) _lastValue.Offset;
			return _reader.Read(_lastValue.Length);
		}

		internal bool Seek(Span<byte> key)
		{
			// binary search for key

			int left = 0;
			int right = _restartCount - 1;

			while (left < right)
			{
				var mid = (left + right + 1) / 2;
				SeekToRestartPoint(mid);
				if (_comparator.Compare(Key, key) < 0)
				{
					left = mid;
				}
				else
				{
					right = mid - 1;
				}
			}

			// linear search in current restart index

			SeekToRestartPoint(left);
			do
			{
				if (Key == null || Key.IsEmpty) return false;

				if (_comparator.Compare(Key, key) >= 0)
				{
					return true;
				}
			} while (TryParseCurrentEntry());

			return false;
		}

		private void SeekToStart()
		{
			// Find offset from restart index
			var offset = GetRestartPoint(0);
			_reader.Position = (int) offset;
			TryParseCurrentEntry();
		}

		private void SeekToRestartPoint(int restartIndex)
		{
			// Find offset from restart index
			var offset = GetRestartPoint(restartIndex);
			_reader.Position = (int) offset;
			TryParseCurrentEntry();
		}

		private uint GetRestartPoint(int index)
		{
			if (index >= _restartCount) throw new IndexOutOfRangeException(nameof(index));

			SpanReader stream = new SpanReader(_blockData);
			stream.Seek(_restartOffset + index * sizeof(uint), SeekOrigin.Begin);
			return stream.ReadUInt32();
		}


		private bool TryParseCurrentEntry()
		{
			if (_reader.Eof) return false;

			// An entry for a particular key-value pair has the form:
			//     shared_bytes: varint32
			var sharedBytes = _reader.ReadVarLong();
			Debug.Assert(Key != null || sharedBytes == 0);
			//     unshared_bytes: varint32
			var nonSharedBytes = _reader.ReadVarLong();
			//     value_length: varint32
			var valueLength = _reader.ReadVarLong();
			//     key_delta: char[unshared_bytes]
			ReadOnlySpan<byte> keyDelta = _reader.Read(nonSharedBytes);

			Span<byte> combinedKey = new byte[sharedBytes + nonSharedBytes];
			Key.Slice(0, (int) sharedBytes).CopyTo(combinedKey.Slice(0, (int) sharedBytes));
			keyDelta.Slice(0, (int) nonSharedBytes).CopyTo(combinedKey.Slice((int) sharedBytes, (int) nonSharedBytes));
			Key = combinedKey;

			// Save handle and skip entry data
			_lastValue = new BlockHandle((ulong) _reader.Position, valueLength);
			_reader.Seek((int) valueLength, SeekOrigin.Current);

			return true;
		}
	}
}