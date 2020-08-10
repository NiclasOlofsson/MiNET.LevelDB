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
			stream.Seek(-((1 + _restartCount) * sizeof(uint)), SeekOrigin.End);
			_restartOffset = stream.Position;
		}

		internal ReadOnlySpan<byte> GetCurrentValue()
		{
			_reader.Position = (int) _lastValue.Offset;
			return _reader.Read(_lastValue.Length);
		}

		internal bool HasNext()
		{
			return Key != null && !Key.IsEmpty;
		}

		internal bool Next()
		{
			if (!HasNext()) return false;

			return TryParseCurrentEntry();
		}

		internal bool Seek(Span<byte> key)
		{
			if (_restartCount == 0) return false;

			// binary search for key

			int left = 0;
			int right = _restartCount - 1;

			while (left < right)
			{
				int mid = (left + right + 1) / 2;
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

			for (SeekToRestartPoint(left); HasNext(); Next())
			{
				if (_comparator.Compare(Key, key) >= 0) return true;
			}

			for (int i = left - 1; i >= 0; i--)
			{
				for (SeekToRestartPoint(i); HasNext(); Next())
				{
					if (_comparator.Compare(Key, key) >= 0) return true;
				}
			}

			return false;
		}

		internal void SeekToStart()
		{
			SeekToRestartPoint(0);
		}

		private void SeekToRestartPoint(int restartIndex)
		{
			// Find offset from restart index
			var offset = GetRestartPoint(restartIndex);
			_reader.Position = (int) offset;
			Key = null;
			TryParseCurrentEntry();
		}

		private uint GetRestartPoint(int index)
		{
			if (index > _restartCount - 1) throw new IndexOutOfRangeException(nameof(index) + $" can not be bigger than {_restartCount - 1}. Actual value {index}");
			if (index < 0) throw new IndexOutOfRangeException(nameof(index) + $" can not be less than 0. Actual value {index}");

			var stream = new SpanReader(_blockData);
			stream.Seek(_restartOffset + (index * sizeof(uint)), SeekOrigin.Begin);
			return stream.ReadUInt32();
		}


		private bool TryParseCurrentEntry()
		{
			if (_reader.Position >= _reader.Length - 4)
			{
				Key = null;
				return false;
			}

			// An entry for a particular key-value pair has the form:
			//     shared_bytes: varint32
			ulong sharedBytes = _reader.ReadVarLong();
			if (Key == null && sharedBytes != 0) throw new Exception("Shared bytes, but no key");
			//     unshared_bytes: varint32
			ulong nonSharedBytes = _reader.ReadVarLong();
			//     value_length: varint32
			ulong valueLength = _reader.ReadVarLong();
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