using System;
using System.IO;
using System.IO.Compression;
using Crc32C;
using log4net;

namespace MiNET.LevelDB
{
	public class BlockHandle
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(BlockHandle));

		public ulong Offset { get; }
		public ulong Length { get; }

		public BlockHandle(ulong offset, ulong length)
		{
			Offset = offset;
			Length = length;
		}

		public static BlockHandle ReadBlockHandle(Stream stream)
		{
			ulong offset = stream.ReadVarint();
			ulong length = stream.ReadVarint();

			return new BlockHandle(offset, length);
		}

		public static byte[] ReadBlock(Stream stream, BlockHandle handle)
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
			checkCrc = Mask(Crc32CAlgorithm.Append(checkCrc, new[] {compressionType}));

			if (crc != checkCrc) throw new InvalidDataException("Corrupted data. Failed checksum test");

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

		const uint MaskDelta = 0xa282ead8;

		public static uint Mask(uint crc)
		{
			// Rotate right by 15 bits and add a constant.
			return ((crc >> 15) | (crc << 17)) + MaskDelta;
		}

		public override string ToString()
		{
			return $"{nameof(Offset)}: {Offset}, {nameof(Length)}: {Length}";
		}
	}
}