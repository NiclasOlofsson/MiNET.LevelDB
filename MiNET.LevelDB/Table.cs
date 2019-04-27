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

		public byte[] Get(Span<byte> key)
		{
			// To find a key in the table you:
			// 1) Read the block index. This index have one entry for each block in the file. For each entry it holds the
			//    last index and a block handle for the block. Binary search this and find the correct block.
			// 2) Search either each entry in the block (brute force) OR use the restart index at the end of the block to find an entry
			//    closer to the index you looking for. The restart index contain a subset of the keys with an offset of where it is located.
			//    Use this offset to start closer to the key (entry) you looking for.
			// 3) Match the key and return the data

			using (var fileStream = _file.OpenRead())
			{
				Footer footer = Footer.Read(fileStream);

				// Search block index
				if (_blockIndex == null)
				{
					_blockIndex = BlockHandle.ReadBlock(fileStream, footer.BlockIndexBlockHandle);
				}

				{
					BlockHandle handle = FindBlockHandleInBlockIndex(key);
					if (handle == null) return null;

					//TODO: Get filter block and do Bloom search first
					if (_metaIndex == null)
					{
						_metaIndex = BlockHandle.ReadBlock(fileStream, footer.MetaindexBlockHandle);
					}

					{
						var filters = GetFilters();
						if (filters.TryGetValue("filter.leveldb.BuiltinBloomFilter2", out BlockHandle filterHandle))
						{
							var filterBlock = BlockHandle.ReadBlock(fileStream, filterHandle);
							Log.Debug("\n" + filterBlock.HexDump(cutAfterFive: true));

							BloomFilterPolicy policy = new BloomFilterPolicy();
							policy.Parse(filterBlock);
							if (!policy.KeyMayMatch(key, handle.Offset))
							{
								Log.Warn("Failed match with bloom filter");
								return null;
							}
							else
							{
								Log.Warn("Found maybe match in bloom filter");
							}
						}
						else
						{
							Log.Error("No filters defined");
							throw new Exception("No filters defined");
						}
					}


					var targetBlock = BlockHandle.ReadBlock(fileStream, handle);
					return FindEntryInBlockData(key, targetBlock);
				}
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
				if (comparator.Compare(key, keyData) <= 0) return handle;
			}

			return null;
		}

		private byte[] FindEntryInBlockData(Span<byte> key, byte[] blockdata)
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

				Log.Debug($"\nKey=(+{sharedBytes}) {combinedKey.ToArray().HexDump(bytesPerLine: combinedKey.Length, cutAfterFive: true)}\n{value.HexDump(cutAfterFive: true)}");

				var comparator = new BytewiseComparator();
				if (comparator.Compare(key, combinedKey) == 0)
				{
					//byte[] value = new byte[valueLength];
					//stream.Read(value, 0, (int)valueLength);
					return value;
				}
				else
				{
					// Skip entry data
					//stream.Seek((long) valueLength, SeekOrigin.Current);
				}

				// shared_bytes == 0 for restart points.
				//
				// The trailer of the block has the form:
				//     restarts: uint32[num_restarts]
				//     num_restarts: uint32
				// restarts[i] contains the offset within the block of the ith restart point.
			}

			return null;
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