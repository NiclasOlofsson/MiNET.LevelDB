using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using MiNET.LevelDB.Utils;

namespace MiNET.LevelDB
{
	public class Footer
	{
		//public static readonly byte[] Magic = {0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb};
		public static readonly ulong Magic = 0xdb4775248b80fb57;
		public const int FooterLength = 48; // (10 + 10) * 2 + 8


		public BlockHandle MetaindexBlockHandle { get; }
		public BlockHandle BlockIndexBlockHandle { get; }

		public Footer()
		{
		}

		public Footer(BlockHandle metaindexBlockHandle, BlockHandle blockIndexBlockHandle)
		{
			MetaindexBlockHandle = metaindexBlockHandle;
			BlockIndexBlockHandle = blockIndexBlockHandle;
		}

		/// <summary>
		///     The second layer of the search tree is a table file's block index of keys. The block index is part of the table
		///     file's metadata which is located at the end of its physical data file. The index contains one entry for each
		///     logical data block within the table file. The entry contains the last key in the block and the offset of the block
		///     within the table file. leveldb performs a binary search of the block index to locate a candidate data block. It
		///     reads the candidate data block from the table file.
		/// </summary>
		public static Footer Read(MemoryMappedViewStream stream)
		{
			stream.Seek(-FooterLength, SeekOrigin.End);
			Span<byte> footer = new byte[FooterLength];
			stream.Read(footer);

			SpanReader reader = new SpanReader(footer);

			BlockHandle metaIndexHandle = BlockHandle.ReadBlockHandle(ref reader);
			BlockHandle indexHandle = BlockHandle.ReadBlockHandle(ref reader);

			reader.Seek(-sizeof(ulong), SeekOrigin.End);
			var magic = reader.ReadUInt64();
			if (Magic != magic)
			{
				throw new Exception("Invalid footer. Magic end missing. This is not a proper table file");
			}

			return new Footer(metaIndexHandle, indexHandle);
		}
	}
}