using System;
using System.Diagnostics;
using System.IO;
using MiNET.LevelDB.Utils;

namespace MiNET.LevelDB
{
	internal ref struct BlockSeeker
	{
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
			SpanReader stream = new SpanReader(_blockData);
			stream.Seek(-4, SeekOrigin.End);
			_restartCount = (int) stream.ReadUInt32();
			stream.Seek(-((1 + _restartCount)*sizeof(uint)), SeekOrigin.End);
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
				var mid = (left + right + 1)/2;
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
			stream.Seek(_restartOffset + index*sizeof(uint), SeekOrigin.Begin);
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