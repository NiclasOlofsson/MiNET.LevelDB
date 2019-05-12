using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using log4net;
using MiNET.LevelDB.Utils;

namespace MiNET.LevelDB
{
	public class TableReader
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(TableReader));

		private readonly FileInfo _file;
		private byte[] _blockIndex;
		private byte[] _metaIndex;
		private BloomFilterPolicy _bloomFilterPolicy;
		private Dictionary<byte[], BlockHandle> _blockIndexes;
		private FileStream _fileStream;
		private BytewiseComparator _comparator = new BytewiseComparator();

		public TableReader(FileInfo file)
		{
			_file = file;
			_fileStream = _file.OpenRead();
		}

		public void ReadTable()
		{
			// A table have
			// - a set of blocks with a set of entries in sorted order
			// - metadata index
			// - block index
			// - footer
		}

		public ResultStatus Get(Span<byte> key)
		{
			if (Log.IsDebugEnabled) Log.Debug($"\nSearch Key={key.ToHexString()}");

			// To find a key in the table you:
			// 1) Read the block index. This index have one entry for each block in the file. For each entry it holds the
			//    last index and a block handle for the block. Binary search this and find the correct block.
			// 2) Search either each entry in the block (brute force) OR use the restart index at the end of the block to find an entry
			//    closer to the index you looking for. The restart index contain a subset of the keys with an offset of where it is located.
			//    Use this offset to start closer to the key (entry) you looking for.
			// 3) Match the key and return the data
			var fileStream = _fileStream;

			// Search block index
			if (_blockIndex == null || _metaIndex == null)
			{
				Footer footer = Footer.Read(fileStream);
				_blockIndex = footer.BlockIndexBlockHandle.ReadBlock(fileStream);
				_metaIndex = footer.MetaindexBlockHandle.ReadBlock(fileStream);
			}

			BlockHandle handle = FindBlockHandleInBlockIndex(key);
			if (handle == null)
			{
				Log.Error($"Expected to find block, but did not");
				return ResultStatus.NotFound;
			}

			if (_bloomFilterPolicy == null)
			{
				var filters = GetFilters();
				if (filters.TryGetValue("filter.leveldb.BuiltinBloomFilter2", out BlockHandle filterHandle))
				{
					var filterBlock = filterHandle.ReadBlock(fileStream);
					if (Log.IsDebugEnabled) Log.Debug("\n" + filterBlock.HexDump(cutAfterFive: true));

					_bloomFilterPolicy = new BloomFilterPolicy();
					_bloomFilterPolicy.Parse(filterBlock);
				}
			}

			if (_bloomFilterPolicy != null && !_bloomFilterPolicy.KeyMayMatch(key, handle.Offset))
			{
				return ResultStatus.NotFound;
			}

			if (!_blockCache.TryGetValue(handle, out byte[] targetBlock))
			{
				targetBlock = handle.ReadBlock(fileStream);
				_blockCache.Add(handle, targetBlock);
			}

			return FindEntryInBlockData(key, targetBlock);
		}

		Dictionary<BlockHandle, byte[]> _blockCache = new Dictionary<BlockHandle, byte[]>();

		private Dictionary<string, BlockHandle> GetFilters()
		{
			var result = new Dictionary<string, BlockHandle>();

			SpanReader reader = new SpanReader(_metaIndex);
			int indexSize = GetRestartIndexSize(ref reader);

			while (reader.Position < reader.Length - indexSize)
			{
				var offset = reader.ReadVarLong();
				var length = reader.ReadVarLong();
				var n3 = reader.ReadVarLong();

				var keyData = reader.Read(offset, length);

				var filterHandle = BlockHandle.ReadBlockHandle(ref reader);

				Log.Debug($"Key={Encoding.UTF8.GetString(keyData)}, BlockHandle={filterHandle}");

				result.Add(Encoding.UTF8.GetString(keyData), filterHandle);
			}

			return result;
		}

		private BlockHandle FindBlockHandleInBlockIndex(Span<byte> key)
		{
			if (_blockIndexes == null)
			{
				_blockIndexes = new Dictionary<byte[], BlockHandle>();

				SpanReader reader = new SpanReader(_blockIndex);
				int indexSize = GetRestartIndexSize(ref reader);

				while (reader.Position < reader.Length - indexSize)
				{
					var offset = reader.ReadVarLong();
					var length = reader.ReadVarLong();
					var n3 = reader.ReadVarLong();

					var keyData = reader.Read(offset, length);

					var handle = BlockHandle.ReadBlockHandle(ref reader);
					_blockIndexes.Add(keyData.ToArray(), handle);
				}
			}

			foreach (var blockIndex in _blockIndexes)
			{
				if (_comparator.Compare(key, blockIndex.Key.UserKey()) <= 0) return blockIndex.Value;
			}

			return null;
		}

		private ResultStatus FindEntryInBlockData(Span<byte> key, ReadOnlySpan<byte> blockData)
		{
			SpanReader reader = new SpanReader(blockData);

			// Find offset from restart index
			var offsets = GetRestartOffsets(blockData);

			int indexSize = (1 + offsets.Count)*sizeof(uint);

			// This should be a binary search, but we brute just force top down
			Span<byte> lastKey = null;
			while (reader.Position < reader.Length - indexSize)
			{
				// An entry for a particular key-value pair has the form:
				//     shared_bytes: varint32
				var sharedBytes = reader.ReadVarLong();
				Debug.Assert(lastKey != null || sharedBytes == 0);
				//     unshared_bytes: varint32
				var unsharedBytes = reader.ReadVarLong();
				//     value_length: varint32
				var valueLength = reader.ReadVarLong();
				//     key_delta: char[unshared_bytes]
				ReadOnlySpan<byte> keyDelta = reader.Read(unsharedBytes);

				Span<byte> combinedKey = new Span<byte>(new byte[sharedBytes + unsharedBytes]);
				lastKey.Slice(0, (int) sharedBytes).CopyTo(combinedKey.Slice(0, (int) sharedBytes));
				keyDelta.Slice(0, (int) unsharedBytes).CopyTo(combinedKey.Slice((int) sharedBytes, (int) unsharedBytes));
				lastKey = combinedKey;

				var number = BitConverter.ToUInt64(combinedKey.Slice(combinedKey.Length - 8, 8));
				var sequence = number >> 8;
				var keyType = (byte) number;

				if (keyType == 0 && _comparator.Compare(key, combinedKey.UserKey()) == 0)
				{
					Log.Warn($"Found deleted entry for Key=(+{sharedBytes}) {combinedKey.ToHexString()}" +
							$"\nSearch Key={key.ToHexString()}");
				}

				if (keyType == 1 && _comparator.Compare(key, combinedKey.UserKey()) == 0)
				{
					//     value: char[value_length]
					var value = reader.Read(valueLength);

					if (Log.IsDebugEnabled)
						Log.Debug($"\nKey=(+{sharedBytes}) {combinedKey.ToHexString()}\n{value.HexDump(cutAfterFive: true)}");

					if (Log.IsDebugEnabled)
						Log.Debug($"\nFound key={combinedKey.ToHexString()}" +
								$"\nSearch Key={key.ToHexString()}");

					return new ResultStatus(ResultState.Exist, value);
				}
				else
				{
					// Skip entry data
					reader.Seek((int) valueLength, SeekOrigin.Current);
				}
			}

			return ResultStatus.NotFound;
		}

		private List<uint> GetRestartOffsets(ReadOnlySpan<byte> data)
		{
			var result = new List<uint>();

			SpanReader stream = new SpanReader(data);

			stream.Seek(-4, SeekOrigin.End);
			uint count = stream.ReadUInt32();
			stream.Position = (int) ((1 + count)*4);
			for (int i = 0; i < count; i++)
			{
				result.Add(stream.ReadUInt32());
			}

			return result;
		}

		private int GetRestartIndexSize(ref SpanReader reader)
		{
			var currPos = reader.Position;
			reader.Seek(-4, SeekOrigin.End);
			int count = reader.ReadInt32();
			reader.Position = currPos;
			return (1 + count)*4;
		}
	}
}