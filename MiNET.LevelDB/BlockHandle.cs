using System;
using System.IO;
using System.IO.Compression;
using System.IO.MemoryMappedFiles;
using log4net;
using MiNET.LevelDB.Utils;

namespace MiNET.LevelDB
{
	public class BlockHandle
	{
		private const int BlockTrailerSize = 5; // compression type (1) + checksum (4)
		private static readonly ILog Log = LogManager.GetLogger(typeof(BlockHandle));

		public ulong Offset { get; }
		public ulong Length { get; }

		public BlockHandle(ulong offset, ulong length)
		{
			Offset = offset;
			Length = length;
		}

		public static BlockHandle ReadBlockHandle(ReadOnlySpan<byte> data)
		{
			var spanReader = new SpanReader(data);
			return ReadBlockHandle(ref spanReader);
		}

		public static BlockHandle ReadBlockHandle(ref SpanReader reader)
		{
			ulong offset = reader.ReadVarLong();
			ulong length = reader.ReadVarLong();

			return new BlockHandle(offset, length);
		}

		public byte[] ReadBlock(MemoryMappedFile memFile, bool verifyChecksum = false)
		{
			using (var stream = memFile.CreateViewStream((long) Offset, (long) Length + BlockTrailerSize, MemoryMappedFileAccess.Read))
			{
				//if (stream.Position != 0) throw new Exception($"Position was {stream.Position}. Expected {0}");
				//if (stream.PointerOffset != (long) Offset%65536) throw new Exception($"Offset was {stream.PointerOffset}. Expected {Offset}");
				//if(stream.Length != (long) Length) throw new Exception($"Length was {stream.Length}. Expected {Length}");
				return ReadBlock(stream, Length, verifyChecksum);
			}
		}

		private byte[] ReadBlock(Stream stream, ulong length, bool verifyChecksum)
		{
			verifyChecksum = verifyChecksum || Database.ParanoidMode;

			byte[] data = new byte[length];
			stream.Seek((long) 0, SeekOrigin.Begin);
			stream.Read(data, 0, (int) length);

			byte compressionType = (byte) stream.ReadByte();

			byte[] checksum = new byte[4];
			stream.Read(checksum, 0, checksum.Length);
			uint crc = BitConverter.ToUInt32(checksum);

			if (verifyChecksum)
			{
				uint checkCrc = Crc32C.Compute(data);
				//checkCrc = Crc32C.Mask(Crc32C.Append(checkCrc, new[] { compressionType }));
				checkCrc = Crc32C.Mask(Crc32C.Append(checkCrc, compressionType));

				if (crc != checkCrc) throw new InvalidDataException($"Corrupted data. Failed checksum test. expected={crc}, actual={checkCrc}");
			}

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
				using (var dataStream = new MemoryStream(data))
				{
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
			}

			return data;
		}

		public override string ToString()
		{
			return $"{nameof(Offset)}: {Offset}, {nameof(Length)}: {Length}";
		}
	}
}