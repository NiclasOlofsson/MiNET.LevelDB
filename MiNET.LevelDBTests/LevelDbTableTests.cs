using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using Crc32C;
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
		public void LevelDbReadTableTest()
		{
			//foreach (var file in Directory.EnumerateFiles(@"D:\Temp\World Saves PE\ExoGAHavAAA=\db", "*.ldb"))
			foreach (var file in Directory.EnumerateFiles(@"D:\Temp\My World\db", "*.ldb"))
			{
				Log.Info($"Reading sstable: {file}");
				int blockTrailerLen = 5;
				int footerLen = 48;

				//var stream = File.OpenRead(@"D:\Temp\World Saves PE\ExoGAHavAAA=\db\000344.ldb");
				//var stream = File.OpenRead(@"D:\Temp\World Saves PE\ExoGAHavAAA=\db\000005.ldb");
				var fileStream = File.OpenRead(file);

				fileStream.Seek(-footerLen, SeekOrigin.End);
				byte[] footer = new byte[footerLen];
				fileStream.Read(footer, 0, footerLen);

				Assert.AreEqual(Footer.Magic, footer.Skip(footer.Length - Footer.Magic.Length).ToArray());

				fileStream.Seek(-footerLen, SeekOrigin.End);

				BlockHandle metaIndexHandle = ReadBlockHandle(fileStream);
				BlockHandle indexHandle = ReadBlockHandle(fileStream);

				byte[] metaIndexBlock = ReadBlock(fileStream, metaIndexHandle);
				Log.Debug("\n" + metaIndexBlock.HexDump());

				KeyValuePair<string, byte[]> keyValue = GetKeyValue(metaIndexBlock);
				Log.Debug($"{keyValue.Key}, {keyValue.Value}");
				//Assert.AreEqual("filter.leveldb.BuiltinBloomFilter2", keyValue.Key);

				//MemoryStream filterIndex = new MemoryStream(keyValue.Value);
				//var offset = (long) ReadVarInt64(filterIndex);
				//var length = (int) ReadVarInt64(filterIndex);

				//var metaIndex = ReadBlock(stream, new BlockHandle(offset, length));
				//Console.WriteLine(HexDump(metaIndex));

				//offset = (long) ReadVarInt64(stream);
				//length = (int) ReadVarInt64(stream);

				//var data = ReadBlock(stream, new BlockHandle(offset, length));
				//Console.WriteLine(HexDump(data));

				byte[] indexBlock = ReadBlock(fileStream, indexHandle);

				Reader reader = new Reader(indexBlock, fileStream);

				Assert.IsTrue(BitConverter.IsLittleEndian);

				DumpIndex(reader);
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
					Log.Debug($"Key={key}, v1={v1}, v2={v2}\nCurrentKey={currentKey.HexDump(currentKey.Length, false, false)}\nCurrentVal={currentVal.HexDump(currentVal.Length, false, false)} ");

					if (!_indicatorChars.Contains(currentKey[0]))
					{
						var mcpeKey = ParseMcpeKey(currentKey);
						//var mcpeKey = ParseMcpeKey(currentKey.Take(currentKey.Length - 8).ToArray());
						//if (mcpeKey.ChunkX == 0)
						Log.Debug($"ChunkX={mcpeKey.ChunkX}, ChunkZ={mcpeKey.ChunkZ}, Dimension={mcpeKey.Dimension}, Type={mcpeKey.BlockTag}, SubId={mcpeKey.SubChunkId}");

						BlockHandle blockHandle = ReadBlockHandle(new MemoryStream(currentVal));
						Stream file = reader.Data;
						var block = ReadBlock(file, blockHandle);
						Log.Debug($"Offset={blockHandle.Offset}, Len={blockHandle.Length}");
						Log.Debug($"Offset={blockHandle.Offset}, Len={block.Length} (uncompressed)\n{block.Take(16*10).ToArray().HexDump()}");
						ParseMcpeBlockData(mcpeKey, block);
					}
					else
					{
						Log.Debug($"Key ASCII: {Encoding.UTF8.GetString(currentKey)}");

						var blockHandle = ReadBlockHandle(new MemoryStream(currentVal));
						Stream file = reader.Data;
						var block = ReadBlock(file, blockHandle);
						Log.Debug($"Offset={blockHandle.Offset}, Len={blockHandle.Length}\n{block.Take(16*10).ToArray().HexDump()}");
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

		private BlockHandle ReadBlockHandle(Stream stream)
		{
			ulong offset = stream.ReadVarint();
			ulong length = stream.ReadVarint();

			return new BlockHandle(offset, length);
		}

		private KeyValuePair<string, byte[]> GetKeyValue(byte[] block)
		{
			MemoryStream stream = new MemoryStream(block);
			int n1 = (int) stream.ReadVarint();
			int n2 = (int) stream.ReadVarint();
			int n3 = (int) stream.ReadVarint();

			byte[] keyData = new byte[n2];
			stream.Read(keyData, n1, n2);


			byte[] valData = new byte[n3];
			stream.Read(valData, 0, n3);

			return new KeyValuePair<string, byte[]>(Encoding.UTF8.GetString(keyData), valData);
		}

		private byte[] ReadBlock(Stream stream, BlockHandle handle)
		{
			// File format contains a sequence of blocks where each block has:
			//    block_data: uint8[n]
			//    type: uint8
			//    crc: uint32

			byte[] data = new byte[handle.Length];
			stream.Seek((long) handle.Offset, SeekOrigin.Begin);
			stream.Read(data, 0, data.Length);

			byte compressionType = (byte) stream.ReadByte();

			byte[] checksum = new byte[4];
			stream.Read(checksum, 0, checksum.Length);
			uint crc = BitConverter.ToUInt32(checksum);

			uint checkCrc = Crc32CAlgorithm.Compute(data);
			checkCrc = BlockHandle.Mask(Crc32CAlgorithm.Append(checkCrc, new[] {compressionType}));

			Assert.AreEqual(crc, checkCrc);

			Log.Debug($"Compression={compressionType}, crc={crc}, checkcrc={checkCrc}");
			if (compressionType == 0)
			{
				// uncompressed
			}
			else if (compressionType == 1)
			{
				// Snapp, i can't read that
				throw new NotSupportedException("Can't read snappy compressed data");
			}
			else if (compressionType >= 2)
			{
				var dataStream = new MemoryStream(data);

				if (compressionType == 2)
				{
					if (dataStream.ReadByte() != 0x78)
					{
						throw new InvalidDataException("Incorrect ZLib header. Expected 0x78 0x9C");
					}
					dataStream.ReadByte();
				}

				using (var defStream2 = new DeflateStream(dataStream, CompressionMode.Decompress))
				{
					// Get actual package out of bytes
					using (MemoryStream destination = new MemoryStream())
					{
						defStream2.CopyTo(destination);
						data = destination.ToArray();
					}
				}
			}

			return data;
		}
	}
}