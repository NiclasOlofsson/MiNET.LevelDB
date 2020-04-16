using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Text;
using log4net;
using MiNET.LevelDB.Utils;

namespace MiNET.LevelDB
{
	public class Table : IDisposable
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(Table));

		private readonly FileInfo _file;
		private byte[] _blockIndex;
		private byte[] _metaIndex;
		private BloomFilterPolicy _bloomFilterPolicy;
		private Dictionary<byte[], BlockHandle> _blockIndexes;
		private BytewiseComparator _comparator = new BytewiseComparator();
		private Dictionary<BlockHandle, byte[]> _blockCache = new Dictionary<BlockHandle, byte[]>();
		private MemoryMappedFile _memFile;
		private Stream _memViewStream;

		public Table(FileInfo file)
		{
			_file = file;
			_memFile = MemoryMappedFile.CreateFromFile(_file.FullName, FileMode.Open);
			//_memViewStream = _memFile.CreateViewStream(0, _file.Length);
			//_memViewStream = _file.OpenRead();
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

			// Search block index
			if (_blockIndex == null || _metaIndex == null)
			{
				Footer footer;
				using (MemoryMappedViewStream stream = _memFile.CreateViewStream(_file.Length - Footer.FooterLength, Footer.FooterLength, MemoryMappedFileAccess.Read))
				{
					footer = Footer.Read(stream);
				}
				_blockIndex = footer.BlockIndexBlockHandle.ReadBlock(_memFile);
				_metaIndex = footer.MetaindexBlockHandle.ReadBlock(_memFile);
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
					//var filterBlock = filterHandle.ReadBlock(_memViewStream);
					var filterBlock = filterHandle.ReadBlock(_memFile);
					if (Log.IsDebugEnabled) Log.Debug("\n" + filterBlock.HexDump(cutAfterFive: true));

					_bloomFilterPolicy = new BloomFilterPolicy();
					_bloomFilterPolicy.Parse(filterBlock);
				}
			}
			if (_bloomFilterPolicy?.KeyMayMatch(key, handle.Offset) ?? true)
			{
				var targetBlock = GetBlock(handle);

				return SeekKeyInBlockData(key, targetBlock);
			}

			return ResultStatus.NotFound;
		}

		private byte[] GetBlock(BlockHandle handle)
		{
			if (!_blockCache.TryGetValue(handle, out byte[] targetBlock))
			{
				targetBlock = handle.ReadBlock(_memFile);
				AddBlockToCache(handle, targetBlock);
			}

			return targetBlock;
		}

		private void AddBlockToCache(BlockHandle handle, byte[] targetBlock)
		{
			_blockCache.Add(handle, targetBlock);
		}

		private Dictionary<string, BlockHandle> GetFilters()
		{
			var result = new Dictionary<string, BlockHandle>();

			SpanReader reader = new SpanReader(_metaIndex);
			int indexSize = GetRestartIndexSize(ref reader);

			while (reader.Position < reader.Length - indexSize)
			{
				var shared = reader.ReadVarLong();
				var nonShared = reader.ReadVarLong();
				var size = reader.ReadVarLong();

				//TODO: This is pretty wrong since it assumes no sharing. However, it works so far.
				if (shared != 0) throw new Exception($"Got {shared} shared bytes for index block. We can't handle that right now.");

				var keyData = reader.Read(shared, nonShared);

				var handle = BlockHandle.ReadBlockHandle(ref reader);

				if (Log.IsDebugEnabled) Log.Debug($"Key={Encoding.UTF8.GetString(keyData)}, BlockHandle={handle}");

				result.Add(Encoding.UTF8.GetString(keyData), handle);
			}

			return result;
		}

		private BlockHandle FindBlockHandleInBlockIndex(Span<byte> key)
		{
			_blockIndexes ??= new Dictionary<byte[], BlockHandle>();

			// cache seriously important for performance
			//TODO: This cache isn't working when values are almost same. Fails on bedrock worlds when getting version and chunks.
			//foreach (var blockIndex in _blockIndexes)
			//{
			//	if (_comparator.Compare(blockIndex.Key.AsSpan().UserKey(), key) >= 0) return blockIndex.Value;
			//}

			BlockSeeker seeker = new BlockSeeker(_blockIndex);
			if (seeker.Seek(key))
			{
				var foundKey = seeker.Key;
				if (_comparator.Compare(foundKey.UserKey(), key) >= 0)
				{
					var value = seeker.CurrentValue;
					if (value != null)
					{
						var handle = BlockHandle.ReadBlockHandle(value);
						_blockIndexes.Add(foundKey.ToArray(), handle);
						return handle;
					}
				}
			}

			return null;
		}

		private ResultStatus SeekKeyInBlockData(Span<byte> key, ReadOnlySpan<byte> blockData)
		{
			BlockSeeker seeker = new BlockSeeker(blockData);
			if (seeker.Seek(key))
			{
				var foundKey = seeker.Key;
				//var sequence = foundKey.SequenceNumber();
				byte keyType = foundKey.OperationType();

				bool matched = _comparator.Compare(key, foundKey.UserKey()) == 0;

				if (keyType == 0 && matched)
				{
					if (Log.IsDebugEnabled) Log.Warn($"Found deleted entry for Key={foundKey.ToHexString()}\nWas search for key={key.ToHexString()}");
				}

				if (keyType == 1 && matched)
				{
					//     value: char[value_length]
					var value = seeker.CurrentValue;

					if (Log.IsDebugEnabled)
						Log.Debug($"\nKey={foundKey.ToHexString()}\n{value.HexDump(cutAfterFive: true)}");

					if (Log.IsDebugEnabled)
						Log.Debug($"\nFound key={foundKey.ToHexString()}\nSearch Key={key.ToHexString()}");

					return new ResultStatus(ResultState.Exist, value);
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
			stream.Position = (int) ((1 + count) * 4);
			for (int i = 0; i < count; i++)
			{
				result.Add(stream.ReadUInt32());
			}

			return result;
		}

		private int GetRestartIndexSize(ref SpanReader reader)
		{
			var currentPosition = reader.Position;
			reader.Seek(-4, SeekOrigin.End);
			int count = reader.ReadInt32();
			reader.Position = currentPosition;
			return (1 + count) * 4;
		}

		public void Dispose()
		{
			_memViewStream?.Dispose();
			_memFile?.Dispose();
		}
	}
}