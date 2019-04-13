using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace MiNET.LevelDBTests
{
	[TestFixture]
	public class LevelDbTests
	{
		static StreamWriter file;

		[SetUp]
		public void Init()
		{
			/* ... */
		}

		[TearDown]
		public void Cleanup()
		{
			file.Close();
			file = null;
		}

		public enum BlockTag
		{
			Dimension0 = 0,
			Dimension1 = 1,

			Dimension2 = 2,
			Data2D = 45,
			Data2DLegacy = 46,
			SubChunkPrefix = 47,
			LegacyTerrain = 48,
			BlockEntity = 49,
			Entity = 50,
			PendingTicks = 51,
			BlockExtraData = 52,
			BiomeState = 53,
			Version = 118,

			Undefined = 255
		};


		byte[] _indicatorChars =
		{
			0x64, 0x69, 0x6d, 0x65,
			0x6e, 0x73, 0x6f,
			0x6e, 0x30
		};

		byte[] _magic = {0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb};

		[Test]
		public void LevelDbReadTest()
		{
			int blockTrailerLen = 5;
			int footerLen = 48;

			var stream = File.OpenRead(@"D:\Temp\World Saves PE\ExoGAHavAAA=\db\000344.ldb");

			stream.Seek(-footerLen, SeekOrigin.End);
			byte[] footer = new byte[footerLen];
			stream.Read(footer, 0, footerLen);
			Assert.AreEqual(_magic, footer.Skip(footer.Length - _magic.Length).ToArray());

			stream.Seek(-footerLen, SeekOrigin.End);

			var metaIndexHandle = ReadBlockHandle(stream);
			var indexHandle = ReadBlockHandle(stream);

			byte[] metaIndexBlock = ReadBlock(stream, metaIndexHandle);
			//Console.WriteLine(HexDump(metaIndexBlock));

			var keyValue = GetKeyValue(metaIndexBlock);
			Assert.AreEqual("filter.leveldb.BuiltinBloomFilter2", keyValue.Key);

			//MemoryStream filterIndex = new MemoryStream(keyValue.Value);
			//var offset = (long) ReadVarInt64(filterIndex);
			//var length = (int) ReadVarInt64(filterIndex);

			//var metaIndex = ReadBlock(stream, new BlockHandle(offset, length));
			//Console.WriteLine(HexDump(metaIndex));

			//offset = (long) ReadVarInt64(stream);
			//length = (int) ReadVarInt64(stream);

			//var data = ReadBlock(stream, new BlockHandle(offset, length));
			//Console.WriteLine(HexDump(data));

			byte[] indexBlock = ReadBlock(stream, indexHandle);

			Reader reader = new Reader(indexBlock, stream);

			Assert.IsTrue(BitConverter.IsLittleEndian);

			DumpIndex(reader);
		}

		private static void Log(string s)
		{
			if (file == null)
			{
				file = new StreamWriter(@"D:\Temp\test_log.txt", true);
			}

			file.WriteLine(s.Trim());
		}

		private void DumpIndex(Reader reader)
		{
			MemoryStream index = new MemoryStream(reader.Index);
			MemoryStream seek = new MemoryStream(reader.Index);

			byte[] num = new byte[4];
			index.Seek(-4, SeekOrigin.End);
			index.Read(num, 0, 4);
			var numRestarts = BitConverter.ToUInt32(num, 0);
			Log($"NumRestarts={numRestarts}");

			//n:= len(b) - 4 * (1 + numRestarts)
			index.Seek(index.Length - 4*(1 + numRestarts), SeekOrigin.Begin);
			Log($"Position={index.Position}");

			int recordedRestarts = 0;
			do
			{
				if (index.Read(num, 0, 4) == 4)
				{
					var key = BitConverter.ToUInt32(num, 0);
					seek.Seek(key, SeekOrigin.Begin);
					var mustBe0 = (long) ReadVarInt32(seek);
					Assert.AreEqual(0, mustBe0);
					var v1 = (ulong) ReadVarInt32(seek);
					var v2 = (ulong) ReadVarInt32(seek);

					byte[] currentKey = new byte[v1];
					seek.Read(currentKey, 0, (int) v1);
					byte[] currentVal = new byte[v2];
					seek.Read(currentVal, 0, (int) v2);

					recordedRestarts++;

					// Key=1, Offset=17, Lenght=3, 
					// CurrentKey = <00 00 00 00> <16 00 00 00> <30> 01 d1 c2 00 00 00 00 00
					Log($"Key={key}, v1={v1}, v2={v2}\nCurrentKey={HexDump(currentKey, currentKey.Length, false, false)}\nCurrentVal={HexDump(currentVal, currentVal.Length, false, false)} ");

					if (!_indicatorChars.Contains(currentKey[0]))
					{
						var mcpeKey = ParseMcpeKey(currentKey);
						//var mcpeKey = ParseMcpeKey(currentKey.Take(currentKey.Length - 8).ToArray());
						//if (mcpeKey.ChunkX == 0)
						Log($"ChunkX={mcpeKey.ChunkX}, ChunkZ={mcpeKey.ChunkZ}, Dimension={mcpeKey.Dimension}, Type={mcpeKey.BlockTag}, SubId={mcpeKey.SubChunkId}");

						var blockHandle = ReadBlockHandle(new MemoryStream(currentVal));
						Stream file = reader.Data;
						var block = ReadBlock(file, blockHandle);
						Log($"Offset={blockHandle.Offset}, Len={blockHandle.Length}\n{HexDump(block.Take(16*10).ToArray())}");
						ParseMcpeBlockData(mcpeKey, block);
					}
					else
					{
						Log($"Key ASCII: {Encoding.UTF8.GetString(currentKey)}");

						var blockHandle = ReadBlockHandle(new MemoryStream(currentVal));
						Stream file = reader.Data;
						var block = ReadBlock(file, blockHandle);
						Log($"Offset={blockHandle.Offset}, Len={blockHandle.Length}\n{HexDump(block.Take(16*10).ToArray())}");
					}
				}
				else
				{
					Assert.Fail("Expected int");
				}
			} while (index.Position < index.Length - 4);

			Log($"NumRestarts={numRestarts}, NumRecordedRestarts={recordedRestarts}");
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

			Console.WriteLine($"BlockLength={data.Length}, Avail={stream.Length - stream.Position}");
		}

		private static McpeKey ParseMcpeKey(byte[] key)
		{
			var chunkX = BitConverter.ToInt32(key, 0);
			var chunkZ = BitConverter.ToInt32(key, 4);
			BlockTag blockType = (BlockTag) key[8];
			Log($"Dim: {blockType}");
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
				case BlockTag.Version:
					subChunkId = key[8 + 1];
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

		static int Compare(byte[] a1, byte[] a2)
		{
			return StructuralComparisons.StructuralComparer.Compare(a1, a2);
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

		private struct BlockHandle
		{
			public long Offset { get; }
			public long Length { get; }

			public BlockHandle(long offset, long length)
			{
				Offset = offset;
				Length = length;
			}
		}

		private BlockHandle ReadBlockHandle(Stream stream)
		{
			long offset = (long) ReadVarInt32(stream);
			int length = (int) ReadVarInt32(stream);

			return new BlockHandle(offset, length);
		}

		private KeyValuePair<string, byte[]> GetKeyValue(byte[] block)
		{
			MemoryStream stream = new MemoryStream(block);
			int n1 = (int) ReadVarInt32(stream);
			int n2 = (int) ReadVarInt32(stream);
			int n3 = (int) ReadVarInt32(stream);

			byte[] keyData = new byte[n2];
			stream.Read(keyData, n1, n2);


			byte[] valData = new byte[n3];
			stream.Read(valData, 0, n3);

			return new KeyValuePair<string, byte[]>(Encoding.UTF8.GetString(keyData), valData);
		}

		private byte[] ReadBlock(Stream stream, BlockHandle handle)
		{
			byte[] data = new byte[handle.Length];
			byte[] checksum = new byte[4];
			stream.Seek(handle.Offset, SeekOrigin.Begin);
			stream.Read(data, 0, data.Length);

			byte compressionType = (byte) stream.ReadByte();
			//Console.WriteLine($"Compression={compressionType}");
			if (compressionType >= 2)
			{
				var dataStream = new MemoryStream(data);

				if (dataStream.ReadByte() != 0x78)
				{
					throw new InvalidDataException("Incorrect ZLib header. Expected 0x78 0x9C");
				}
				dataStream.ReadByte();

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

			int crc = Convert.ToInt32(stream.Read(checksum, 0, checksum.Length));

			return data;
		}

		public static string HexDump(byte[] bytes, int bytesPerLine = 16, bool printLineCount = false, bool printText = true, bool cutAfterFive = false)
		{
			StringBuilder sb = new StringBuilder();
			for (int line = 0; line < bytes.Length; line += bytesPerLine)
			{
				if (cutAfterFive && line >= bytesPerLine*5)
				{
					sb.AppendLine(".. output cut after 5 lines");
					break;
				}

				byte[] lineBytes = bytes.Skip(line).Take(bytesPerLine).ToArray();
				if (printLineCount) sb.AppendFormat("{0:x8} ", line);
				sb.Append(string.Join(" ", lineBytes.Select(b => b.ToString("x2"))
						.ToArray())
					.PadRight(bytesPerLine*3));
				if (printText)
				{
					sb.Append(" ");
					sb.Append(new string(lineBytes.Select(b => b < 32 ? '.' : (char) b)
						.ToArray()));
				}
				if (bytesPerLine < bytes.Length)
					sb.AppendLine();
			}

			return sb.ToString();
		}

		public static ulong ReadVarInt32(Stream buf, int maxSize = 10)
		{
			return (ulong) ReadVariableLengthInt(buf);
		}

		public static int ReadVariableLengthInt(Stream sliceInput)
		{
			int result = 0;
			for (int shift = 0; shift <= 28; shift += 7)
			{
				int b = sliceInput.ReadByte();
				// add the lower 7 bits to the result
				result |= ((b & 0x7f) << shift);

				// if high bit is not set, this is the last byte in the number
				if ((b & 0x80) == 0)
				{
					return result;
				}
			}
			throw new Exception("last byte of variable length int has high bit set");
		}

		public static long ReadVariableLengthLong(Stream sliceInput)
		{
			long result = 0;
			for (int shift = 0; shift <= 63; shift += 7)
			{
				long b = sliceInput.ReadByte();

				// add the lower 7 bits to the result
				result |= ((b & 0x7f) << shift);

				// if high bit is not set, this is the last byte in the number
				if ((b & 0x80) == 0)
				{
					return result;
				}
			}
			throw new Exception("last byte of variable length int has high bit set");
		}
	}

	abstract class LevelDbFactory
	{
		/**
		 * Loads/creates a (new) database, located at the given File path.
		 * @param dbFolder The root path of the database folder.
		 * @return An object with database controls as specified in {@link ILevelDB}.
		 */
		public abstract ILevelDb LoadLevelDb(DirectoryInfo dbFolder);
	}

	public interface ILevelDb
	{
		void Delete(byte[] key);

		void Put(byte[] key, byte[] value);

		byte[] Get(byte[] key);

		List<String> GetDbKeysStartingWith(String startWith);

		void Open();

		void Close();

		void Destroy();

		bool IsClosed();
	}
}