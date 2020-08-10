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
using System.IO.MemoryMappedFiles;
using System.Text;
using log4net;
using MiNET.LevelDB.Enumerate;
using MiNET.LevelDB.Utils;

namespace MiNET.LevelDB
{
	public class Table : IDisposable, IEnumerable<BlockEntry>
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(Table));

		private readonly FileInfo _file;
		internal byte[] _blockIndex;
		private byte[] _metaIndex;
		private BloomFilterPolicy _bloomFilterPolicy;
		private BytewiseComparator _comparator = new BytewiseComparator();
		private Dictionary<BlockHandle, byte[]> _blockCache = new Dictionary<BlockHandle, byte[]>();
		private MemoryMappedFile _memFile;
		private Stream _memViewStream;
		private bool _initialized;
		public bool Initialized => _initialized;

		public Table(FileInfo file)
		{
			_file = file;
			_memFile = MemoryMappedFile.CreateFromFile(_file.FullName, FileMode.Open);
		}

		public ResultStatus Get(Span<byte> key)
		{
			if (Log.IsDebugEnabled) Log.Debug($"Get Key from table: {key.ToHexString()}");

			// To find a key in the table you:
			// 1) Read the block index. This index have one entry for each block in the file. For each entry it holds the
			//    last index and a block handle for the block. Binary search this and find the correct block.
			// 2) Search either each entry in the block (brute force) OR use the restart index at the end of the block to find an entry
			//    closer to the index you looking for. The restart index contain a subset of the keys with an offset of where it is located.
			//    Use this offset to start closer to the key (entry) you looking for.
			// 3) Match the key and return the data

			// Search block index
			Initialize();

			BlockHandle handle = FindBlockHandleInBlockIndex(key);
			if (handle == null)
			{
				if (Log.IsDebugEnabled) Log.Warn($"Key was in range for table, but found no exact match in file {_file.Name}");
				return ResultStatus.NotFound;
			}

			if (_bloomFilterPolicy?.KeyMayMatch(key, handle.Offset) ?? true)
			{
				byte[] targetBlock = GetBlock(handle);

				return SeekKeyInBlockData(key, targetBlock);
			}

			return ResultStatus.NotFound;
		}

		internal void Initialize()
		{
			if (_initialized) return;

			if (_blockIndex == null || _metaIndex == null)
			{
				Log.Debug($"Initialize table {_file.Name}");

				Footer footer;
				using (MemoryMappedViewStream stream = _memFile.CreateViewStream(_file.Length - Footer.FooterLength, Footer.FooterLength, MemoryMappedFileAccess.Read))
				{
					footer = Footer.Read(stream);
				}
				_blockIndex = footer.BlockIndexBlockHandle.ReadBlock(_memFile);
				_metaIndex = footer.MetaIndexBlockHandle.ReadBlock(_memFile);

				Log.Debug($"initialized table index len {_blockIndex?.Length}");

				Dictionary<string, BlockHandle> filters = GetFilters();
				if (filters.TryGetValue("filter.leveldb.BuiltinBloomFilter2", out BlockHandle filterHandle))
				{
					byte[] filterBlock = filterHandle.ReadBlock(_memFile);
					if (Log.IsDebugEnabled) Log.Debug("Found filter block:\n" + filterBlock.HexDump(cutAfterFive: true));

					_bloomFilterPolicy = new BloomFilterPolicy();
					_bloomFilterPolicy.Parse(filterBlock);
				}

				_initialized = true;
			}
		}

		internal byte[] GetBlock(BlockHandle handle)
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
				ulong shared = reader.ReadVarLong();
				ulong nonShared = reader.ReadVarLong();
				ulong size = reader.ReadVarLong();

				//TODO: This is pretty wrong since it assumes no sharing. However, it works so far.
				if (shared != 0) throw new Exception($"Got {shared} shared bytes for index block. We can't handle that right now.");

				ReadOnlySpan<byte> keyData = reader.Read(shared, nonShared);

				var handle = BlockHandle.ReadBlockHandle(ref reader);

				if (Log.IsDebugEnabled) Log.Debug($"Key={Encoding.UTF8.GetString(keyData)}, BlockHandle={handle}");

				result.Add(Encoding.UTF8.GetString(keyData), handle);
			}

			return result;
		}

		private BlockHandle FindBlockHandleInBlockIndex(Span<byte> key)
		{
			var seeker = new BlockSeeker(_blockIndex);
			if (seeker.Seek(key))
			{
				Span<byte> foundKey = seeker.Key;
				if (Log.IsDebugEnabled) Log.Debug($"Found key in block index: {foundKey.ToHexString()}");
				if (_comparator.Compare(foundKey.UserKey(), key) >= 0)
				{
					ReadOnlySpan<byte> value = seeker.CurrentValue;
					if (value != null)
					{
						var handle = BlockHandle.ReadBlockHandle(value);
						return handle;
					}
				}
			}

			return null;
		}

		private ResultStatus SeekKeyInBlockData(Span<byte> key, ReadOnlySpan<byte> blockData)
		{
			var seeker = new BlockSeeker(blockData);
			if (seeker.Seek(key))
			{
				var foundKey = seeker.Key;
				//var sequence = foundKey.SequenceNumber();
				byte keyType = foundKey.OperationType();

				bool matched = _comparator.Compare(key, foundKey.UserKey()) == 0;

				if (matched)
				{
					if (keyType == 0)
					{
						if (Log.IsDebugEnabled) Log.Warn($"Found deleted entry for Key={foundKey.ToHexString()}\nWas search for key={key.ToHexString()}");
						return ResultStatus.Deleted;
					}
					if (keyType == 1)
					{
						//     value: char[value_length]
						ReadOnlySpan<byte> value = seeker.CurrentValue;

						if (Log.IsDebugEnabled) Log.Debug($"Seek key: {key.ToHexString()} and found key: {foundKey.ToHexString()} with data:\n{value.HexDump(cutAfterFive: true)}");

						return new ResultStatus(ResultState.Exist, value);
					}
					else
					{
						Log.Warn($"Found unknown key type: {keyType}");
					}
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
			_memViewStream = null;
			_memFile?.Dispose();
			_memFile = null;
		}

		public IEnumerator<BlockEntry> GetEnumerator()
		{
			return new TableEnumerator(this);
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}
	}
}