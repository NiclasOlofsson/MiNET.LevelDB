using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using log4net;

namespace MiNET.LevelDB
{
	public class TableReader
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(TableReader));

		private readonly FileInfo _file;
		private byte[] _blockIndex;
		private byte[] _metaIndex;

		public TableReader(FileInfo file)
		{
			_file = file;
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
			Log.Debug($"\nSearch Key={key.ToHexString()}");

			// To find a key in the table you:
			// 1) Read the block index. This index have one entry for each block in the file. For each entry it holds the
			//    last index and a block handle for the block. Binary search this and find the correct block.
			// 2) Search either each entry in the block (brute force) OR use the restart index at the end of the block to find an entry
			//    closer to the index you looking for. The restart index contain a subset of the keys with an offset of where it is located.
			//    Use this offset to start closer to the key (entry) you looking for.
			// 3) Match the key and return the data

			using (var fileStream = _file.OpenRead())
			{
				// Search block index
				if (_blockIndex == null || _metaIndex == null)
				{
					Footer footer = Footer.Read(fileStream);
					_blockIndex = BlockHandle.ReadBlock(fileStream, footer.BlockIndexBlockHandle);
					_metaIndex = BlockHandle.ReadBlock(fileStream, footer.MetaindexBlockHandle);
				}

				BlockHandle handle = FindBlockHandleInBlockIndex(key);
				if (handle == null)
				{
					Log.Error($"Expected to find block, but did not");
					return ResultStatus.NotFound;
				}

				var filters = GetFilters();
				if (filters.TryGetValue("filter.leveldb.BuiltinBloomFilter2", out BlockHandle filterHandle))
				{
					var filterBlock = BlockHandle.ReadBlock(fileStream, filterHandle);
					if (Log.IsDebugEnabled) Log.Debug("\n" + filterBlock.HexDump(cutAfterFive: true));

					BloomFilterPolicy policy = new BloomFilterPolicy();
					policy.Parse(filterBlock);
					if (!policy.KeyMayMatch(key, handle.Offset))
					{
						Log.Warn("Failed match with bloom filter");
						return ResultStatus.NotFound;
					}
				}

				var targetBlock = BlockHandle.ReadBlock(fileStream, handle);
				return FindEntryInBlockData(key, targetBlock);
			}
		}

		private Dictionary<string, BlockHandle> GetFilters()
		{
			var result = new Dictionary<string, BlockHandle>();

			MemoryStream stream = new MemoryStream(_metaIndex);
			int indexSize = GetRestartIndexSize(stream);

			while (stream.Position < stream.Length - indexSize)
			{
				int n1 = (int) stream.ReadVarint();
				int n2 = (int) stream.ReadVarint();
				int n3 = (int) stream.ReadVarint();

				byte[] keyData = new byte[n2];
				stream.Read(keyData, n1, n2);

				var filterHandle = BlockHandle.ReadBlockHandle(stream);

				Log.Debug($"Key={Encoding.UTF8.GetString(keyData)}, BlockHandle={filterHandle}");

				result.Add(Encoding.UTF8.GetString(keyData), filterHandle);
			}

			return result;
		}


		private BlockHandle FindBlockHandleInBlockIndex(Span<byte> key)
		{
			MemoryStream stream = new MemoryStream(_blockIndex);
			int indexSize = GetRestartIndexSize(stream);

			BlockHandle handle = null;

			while (stream.Position < stream.Length - indexSize)
			{
				int n1 = (int) stream.ReadVarint();
				int n2 = (int) stream.ReadVarint();
				int n3 = (int) stream.ReadVarint();

				byte[] keyData = new byte[n2];
				stream.Read(keyData, n1, n2);

				handle = BlockHandle.ReadBlockHandle(stream);

				var comparator = new BytewiseComparator();
				if (comparator.Compare(key, keyData.UserKey()) <= 0) return handle;
			}

			return null;
		}

		private ResultStatus FindEntryInBlockData(Span<byte> key, byte[] blockdata)
		{
			Stream stream = new MemoryStream(blockdata);

			// Find offset from restart index
			var offsets = GetRestartOffsets(stream);

			int indexSize = (1 + offsets.Count)*sizeof(uint);

			// This should be a binary search, but we brute just force top down
			Span<byte> lastKey = null;
			while (stream.Position < stream.Length - indexSize)
			{
				// An entry for a particular key-value pair has the form:
				//     shared_bytes: varint32
				var sharedBytes = stream.ReadVarint();
				Debug.Assert(lastKey != null || sharedBytes == 0);
				//     unshared_bytes: varint32
				var unsharedBytes = stream.ReadVarint();
				//     value_length: varint32
				var valueLength = stream.ReadVarint();
				//     key_delta: char[unshared_bytes]
				Span<byte> keyDelta = new byte[unsharedBytes];
				stream.Read(keyDelta);

				Span<byte> combinedKey = new Span<byte>(new byte[sharedBytes + unsharedBytes]);
				lastKey.Slice(0, (int) sharedBytes).CopyTo(combinedKey.Slice(0, (int) sharedBytes));
				keyDelta.Slice(0, (int) unsharedBytes).CopyTo(combinedKey.Slice((int) sharedBytes, (int) unsharedBytes));
				lastKey = combinedKey;

				//     value: char[value_length]
				byte[] value = new byte[valueLength];
				stream.Read(value, 0, (int) valueLength);

				var number = BitConverter.ToUInt64(combinedKey.Slice(combinedKey.Length - 8, 8));
				var sequence = number >> 8;
				var keyType = (byte) number;

				if (Log.IsDebugEnabled) Log.Debug($"\nKey=(+{sharedBytes}) {combinedKey.ToHexString()}\n{value.HexDump(cutAfterFive: true)}");

				var comparator = new BytewiseComparator();
				if (keyType == 0 && comparator.Compare(key, combinedKey.UserKey()) == 0)
				{
					Log.Warn($"Found deleted entry for Key=(+{sharedBytes}) {combinedKey.ToHexString()}" +
							$"\nSearch Key={key.ToHexString()}");
				}

				if (keyType == 1 && comparator.Compare(key, combinedKey.UserKey()) == 0)
				{
					if (Log.IsDebugEnabled)
						Log.Debug($"\nFound key={combinedKey.ToHexString()}" +
								$"\nSearch Key={key.ToHexString()}");

					return new ResultStatus(ResultState.Exist, value);
				}
				else
				{
					//TODO: Fix and use this when we don't need to debug so much
					// Skip entry data
					//stream.Seek((long) valueLength, SeekOrigin.Current);
				}
			}

			return ResultStatus.NotFound;
		}

		private List<uint> GetRestartOffsets(Stream stream)
		{
			long currPos = stream.Position;

			var result = new List<uint>();

			stream.Seek(-4, SeekOrigin.End);
			BinaryReader reader = new BinaryReader(stream);
			uint count = reader.ReadUInt32();
			stream.Position = (1 + count)*4;
			for (int i = 0; i < count; i++)
			{
				result.Add(reader.ReadUInt32());
			}

			stream.Position = currPos;

			return result;
		}

		private int GetRestartIndexSize(Stream stream)
		{
			long currPos = stream.Position;
			stream.Seek(-4, SeekOrigin.End);
			BinaryReader reader = new BinaryReader(stream);
			int count = reader.ReadInt32();
			stream.Position = currPos;
			return (1 + count)*4;
		}
	}
}