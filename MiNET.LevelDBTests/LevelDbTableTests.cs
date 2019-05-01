using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using log4net;
using MiNET.LevelDB;
using NUnit.Framework;

namespace MiNET.LevelDBTests
{
	[TestFixture]
	public class LevelDbTableTests
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(LevelDbTableTests));

		byte[] _indicatorChars =
		{
			0x64, 0x69, 0x6d, 0x65,
			0x6e, 0x73, 0x6f,
			0x6e, 0x30
		};

		[Test]
		public void LevelDbReadFindInTableTest()
		{
			FileInfo fileInfo = new FileInfo(@"TestWorld\000050.ldb");
			TableReader table = new TableReader(fileInfo);

			var result = table.Get(new byte[] {0xf6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x2f, 0x00,});
			if (result.Data != null)
			{
				if (Log.IsDebugEnabled) Log.Debug("Result:\n" + result.Data.HexDump(cutAfterFive: true));
				return;
			}

			Assert.Fail("Found no entry");
		}

		[Test]
		public void LevelDbReadTableTest()
		{
			foreach (var file in Directory.EnumerateFiles(@"TestWorld", "*.ldb"))
			{
				Log.Info($"Reading sstable: {file}");

				//var stream = File.OpenRead(@"D:\Temp\World Saves PE\ExoGAHavAAA=\db\000344.ldb");
				//var stream = File.OpenRead(@"D:\Temp\World Saves PE\ExoGAHavAAA=\db\000005.ldb");
				var fileStream = File.OpenRead(file);

				Footer footer = Footer.Read(fileStream);
				BlockHandle metaIndexHandle = footer.MetaindexBlockHandle;
				BlockHandle indexHandle = footer.BlockIndexBlockHandle;

				byte[] metaIndexBlock = BlockHandle.ReadBlock(fileStream, metaIndexHandle);
				if (Log.IsDebugEnabled) Log.Debug("\n" + metaIndexBlock.HexDump());

				var keyValues = GetKeyValues(metaIndexBlock);
				if (keyValues.TryGetValue("filter.leveldb.BuiltinBloomFilter2", out BlockHandle filterHandle))
				{
					var filterBlock = BlockHandle.ReadBlock(fileStream, filterHandle);
					Assert.NotNull(filterBlock);
					if (Log.IsDebugEnabled) Log.Debug("\n" + filterBlock.HexDump(cutAfterFive: true));
				}

				//MemoryStream filterIndex = new MemoryStream(keyValue.Value);
				//var offset = (long) ReadVarInt64(filterIndex);
				//var length = (int) ReadVarInt64(filterIndex);

				//var metaIndex = ReadBlock(stream, new BlockHandle(offset, length));
				//Console.WriteLine(HexDump(metaIndex));

				//offset = (long) ReadVarInt64(stream);
				//length = (int) ReadVarInt64(stream);

				//var data = ReadBlock(stream, new BlockHandle(offset, length));
				//Console.WriteLine(HexDump(data));

				byte[] indexBlock = BlockHandle.ReadBlock(fileStream, indexHandle);
				var blocks = DumpIndexOnly(indexBlock);

				//Reader reader = new Reader(indexBlock, fileStream);
				//DumpIndex(reader);

				DumpBlockData(fileStream, blocks);

				Assert.IsTrue(BitConverter.IsLittleEndian);
			}
		}

		private List<BlockHandle> DumpIndexOnly(byte[] block)
		{
			List<BlockHandle> blockHandles = new List<BlockHandle>();

			MemoryStream stream = new MemoryStream(block);
			int indexSize = GetRestartIndexSize(stream);

			while (stream.Position < stream.Length - indexSize)
			{
				int n1 = (int) stream.ReadVarint();
				int n2 = (int) stream.ReadVarint();
				int n3 = (int) stream.ReadVarint();

				byte[] keyData = new byte[n2];
				stream.Read(keyData, n1, n2);

				var handle = BlockHandle.ReadBlockHandle(stream);
				blockHandles.Add(handle);

				Log.Debug($"String key={Encoding.UTF8.GetString(keyData)}, BlockHandle={handle}");
			}

			return blockHandles;
		}

		private void DumpBlockData(FileStream reader, List<BlockHandle> blocks)
		{
			foreach (var blockHandle in blocks)
			{
				var block = BlockHandle.ReadBlock(reader, blockHandle);
				DumpBlock(block);
			}
		}

		private void DumpBlock(byte[] blockdata)
		{
			Stream stream = new MemoryStream(blockdata);
			stream.Position = 0;

			int indexSize = GetRestartIndexSize(stream);

			Span<byte> lastKey = null;
			while (stream.Position < stream.Length - indexSize)
			{
				// An entry for a particular key-value pair has the form:
				//     shared_bytes: varint32
				var sharedBytes = stream.ReadVarint();
				Assert.True(lastKey != null || sharedBytes == 0);
				//     unshared_bytes: varint32
				var unsharedBytes = stream.ReadVarint();
				//     value_length: varint32
				var valueLength = stream.ReadVarint();
				//     key_delta: char[unshared_bytes]
				Span<byte> keyDelta = new byte[unsharedBytes];
				stream.Read(keyDelta);
				Span<byte> combinedKey = new Span<byte>(new byte[sharedBytes + unsharedBytes]);
				lastKey.Slice(0, (int) sharedBytes).CopyTo(combinedKey.Slice(0, (int) sharedBytes));
				keyDelta.CopyTo(combinedKey.Slice((int) sharedBytes, (int) unsharedBytes));
				lastKey = combinedKey;

				//     value: char[value_length]
				byte[] value = new byte[valueLength];
				stream.Read(value, 0, (int) valueLength);

				// shared_bytes == 0 for restart points.
				//
				// The trailer of the block has the form:
				//     restarts: uint32[num_restarts]
				//     num_restarts: uint32
				// restarts[i] contains the offset within the block of the ith restart point.

				if (Log.IsDebugEnabled) Log.Debug($"\nKey=(+{sharedBytes}) {combinedKey.ToArray().HexDump(bytesPerLine: combinedKey.Length, cutAfterFive: true)}\n{value.HexDump(cutAfterFive: true)}");

				if (Log.IsDebugEnabled) Log.Debug($"\n{"new byte[] {0x" + combinedKey.ToArray().HexDump(bytesPerLine: combinedKey.Length, printText: false, cutAfterFive: true).Trim().Replace(" ", ", 0x") + "},"}");

				//if (!_indicatorChars.Contains(keyDelta[0]))
				//{
				//	var mcpeKey = ParseMcpeKey(keyDelta);
				//	//var mcpeKey = ParseMcpeKey(currentKey.Take(currentKey.Length - 8).ToArray());
				//	//if (mcpeKey.ChunkX == 0)
				//	Log.Debug($"ChunkX={mcpeKey.ChunkX}, ChunkZ={mcpeKey.ChunkZ}, Dimension={mcpeKey.Dimension}, Type={mcpeKey.BlockTag}, SubId={mcpeKey.SubChunkId}");

				//	BlockHandle blockHandle = BlockHandle.ReadBlockHandle(new MemoryStream(value));
				//	Stream file = reader.Data;
				//	var block = BlockHandle.ReadBlock(file, blockHandle);
				//	Log.Debug($"Offset={blockHandle.Offset}, Len={blockHandle.Length}");
				//	Log.Debug($"Offset={blockHandle.Offset}, Len={block.Length} (uncompressed)\n{block.Take(16*10).ToArray().HexDump()}");
				//	ParseMcpeBlockData(mcpeKey, block);
				//}
				//else
				//{
				//	Log.Debug($"Key ASCII: {Encoding.UTF8.GetString(keyDelta)}");

				//	var blockHandle = BlockHandle.ReadBlockHandle(new MemoryStream(value));
				//	Stream file = reader.Data;
				//	var block = BlockHandle.ReadBlock(file, blockHandle);
				//	Log.Debug($"Offset={blockHandle.Offset}, Len={blockHandle.Length}\n{block.Take(16*10).ToArray().HexDump()}");
				//}
			}
		}


		private void DumpIndex(Reader reader)
		{
			MemoryStream index = new MemoryStream(reader.Index);
			MemoryStream seek = new MemoryStream(reader.Index);

			byte[] num = new byte[4];
			index.Seek(-4, SeekOrigin.End);
			index.Read(num, 0, 4);
			var numRestarts = BitConverter.ToUInt32(num, 0);
			Log.Debug($"NumRestarts={numRestarts}");

			//n:= len(b) - 4 * (1 + numRestarts)
			index.Seek(index.Length - 4*(1 + numRestarts), SeekOrigin.Begin);
			Log.Debug($"Position={index.Position}");

			int recordedRestarts = 0;
			do
			{
				if (index.Read(num, 0, 4) == 4)
				{
					var key = BitConverter.ToUInt32(num, 0);
					seek.Seek(key, SeekOrigin.Begin);
					var mustBe0 = (long) seek.ReadVarint();
					Assert.AreEqual(0, mustBe0);
					var v1 = (ulong) seek.ReadVarint();
					var v2 = (ulong) seek.ReadVarint();

					byte[] currentKey = new byte[v1];
					seek.Read(currentKey, 0, (int) v1);

					byte[] currentVal = new byte[v2];
					seek.Read(currentVal, 0, (int) v2);

					recordedRestarts++;

					// Key=1, Offset=17, Lenght=3, 
					// CurrentKey = <00 00 00 00> <16 00 00 00> <30> 01 d1 c2 00 00 00 00 00
					if (Log.IsDebugEnabled) Log.Debug($"Key={key}, v1={v1}, v2={v2}\nCurrentKey={currentKey.HexDump(currentKey.Length, false, false)}\nCurrentVal={currentVal.HexDump(currentVal.Length, false, false)} ");

					if (!_indicatorChars.Contains(currentKey[0]))
					{
						var mcpeKey = ParseMcpeKey(currentKey);
						//var mcpeKey = ParseMcpeKey(currentKey.Take(currentKey.Length - 8).ToArray());
						//if (mcpeKey.ChunkX == 0)
						Log.Debug($"ChunkX={mcpeKey.ChunkX}, ChunkZ={mcpeKey.ChunkZ}, Dimension={mcpeKey.Dimension}, Type={mcpeKey.BlockTag}, SubId={mcpeKey.SubChunkId}");

						BlockHandle blockHandle = BlockHandle.ReadBlockHandle(new MemoryStream(currentVal));
						Stream file = reader.Data;
						var block = BlockHandle.ReadBlock(file, blockHandle);
						Log.Debug($"Offset={blockHandle.Offset}, Len={blockHandle.Length}");
						if (Log.IsDebugEnabled) Log.Debug($"Offset={blockHandle.Offset}, Len={block.Length} (uncompressed)\n{block.Take(16*10).ToArray().HexDump()}");
						ParseMcpeBlockData(mcpeKey, block);
					}
					else
					{
						Log.Debug($"Key ASCII: {Encoding.UTF8.GetString(currentKey)}");

						var blockHandle = BlockHandle.ReadBlockHandle(new MemoryStream(currentVal));
						Stream file = reader.Data;
						var block = BlockHandle.ReadBlock(file, blockHandle);
						if (Log.IsDebugEnabled) Log.Debug($"Offset={blockHandle.Offset}, Len={blockHandle.Length}\n{block.Take(16*10).ToArray().HexDump()}");
					}
				}
				else
				{
					Assert.Fail("Expected int");
				}
			} while (index.Position < index.Length - 4);

			Log.Debug($"NumRestarts={numRestarts}, NumRecordedRestarts={recordedRestarts}");
		}

		private void ParseMcpeBlockData(McpeKey key, byte[] data)
		{
			if (key.BlockTag != BlockTag.LegacyTerrain && key.BlockTag != BlockTag.SubChunkPrefix) return;
			//Assert.AreEqual(83200, data.Length);

			int h = key.BlockTag == BlockTag.SubChunkPrefix ? 16 : 256;

			MemoryStream stream = new MemoryStream(data);

			{
				byte[] blocks = new byte[16*16*h];
				stream.Read(blocks, 0, 16*16*h);
			}
			{
				byte[] metadata = new byte[16*16*h/2];
				stream.Read(metadata, 0, 16*16*h/2); // nibble
			}
			{
				byte[] skylight = new byte[16*16*h/2];
				stream.Read(skylight, 0, 16*16*h/2); // nibble
			}
			{
				byte[] blocklight = new byte[16*16*h/2];
				stream.Read(blocklight, 0, 16*16*h/2); // nibble
			}

			Log.Debug($"BlockLength={data.Length}, Avail={stream.Length - stream.Position}");
		}

		private static McpeKey ParseMcpeKey(byte[] key)
		{
			var chunkX = BitConverter.ToInt32(key, 0);
			var chunkZ = BitConverter.ToInt32(key, 4);
			BlockTag blockType = (BlockTag) key[8];
			Log.Debug($"Dim: {blockType}:{(int) blockType}");
			int dimension = 0;
			int subChunkId = 0;
			switch (blockType)
			{
				case BlockTag.SubChunkPrefix:
				case BlockTag.LegacyTerrain:
				case BlockTag.Data2D:
				case BlockTag.Data2DLegacy:
				case BlockTag.BlockEntity:
				case BlockTag.Entity:
				case BlockTag.PendingTicks:
				case BlockTag.BlockExtraData:
				case BlockTag.BiomeState:
					subChunkId = key[8 + 1];
					break;
				case BlockTag.Version:
					subChunkId = key[8 + 1];
					Log.Info($"Found version {subChunkId}");
					break;
				case BlockTag.Dimension0:
				case BlockTag.Dimension1:
				case BlockTag.Dimension2:
					dimension = BitConverter.ToInt32(key, 8);
					blockType = (BlockTag) key[8 + 4];
					subChunkId = key[8 + 4 + 1];
					break;
				default:
					break;
			}

			return new McpeKey(chunkX, chunkZ, (BlockTag) blockType, dimension, subChunkId);
		}

		public struct McpeKey
		{
			public McpeKey(int chunkX, int chunkZ, BlockTag blockTag, int dimension, int subChunkId)
			{
				ChunkX = chunkX;
				ChunkZ = chunkZ;
				BlockTag = blockTag;
				Dimension = dimension;
				SubChunkId = subChunkId;
			}

			public int ChunkX { get; private set; }
			public int ChunkZ { get; private set; }
			public BlockTag BlockTag { get; private set; }
			public int Dimension { get; private set; }
			public int SubChunkId { get; private set; }
		}

		private struct Reader
		{
			public byte[] Index { get; private set; }
			public Stream Data { get; private set; }

			public Reader(byte[] index, Stream data)
			{
				Index = index;
				Data = data;
			}
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

		private Dictionary<string, BlockHandle> GetKeyValues(byte[] block)
		{
			var result = new Dictionary<string, BlockHandle>();

			MemoryStream stream = new MemoryStream(block);
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

		public void ReadBloomFilter2(byte[] data)
		{
		}
	}
}