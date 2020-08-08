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
using System.IO;
using MiNET.LevelDB.Utils;

namespace MiNET.LevelDB
{
	public class Footer
	{
		public static readonly byte[] Magic = { 0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb };
		public const int FooterLength = 48; // (10 + 10) * 2 + 8


		public BlockHandle MetaIndexBlockHandle { get; }
		public BlockHandle BlockIndexBlockHandle { get; }

		public Footer()
		{
		}

		public Footer(BlockHandle metaIndexBlockHandle, BlockHandle blockIndexBlockHandle)
		{
			MetaIndexBlockHandle = metaIndexBlockHandle;
			BlockIndexBlockHandle = blockIndexBlockHandle;
		}

		/// <summary>
		///     The second layer of the search tree is a table file's block index of keys. The block index is part of the table
		///     file's metadata which is located at the end of its physical data file. The index contains one entry for each
		///     logical data block within the table file. The entry contains the last key in the block and the offset of the block
		///     within the table file. leveldb performs a binary search of the block index to locate a candidate data block. It
		///     reads the candidate data block from the table file.
		/// </summary>
		public static Footer Read(Stream stream)
		{
			stream.Seek(-FooterLength, SeekOrigin.End);
			Span<byte> footer = new byte[FooterLength];
			stream.Read(footer);

			var reader = new SpanReader(footer);

			var metaIndexHandle = BlockHandle.ReadBlockHandle(ref reader);
			var indexHandle = BlockHandle.ReadBlockHandle(ref reader);

			reader.Seek(-sizeof(ulong), SeekOrigin.End);
			ReadOnlySpan<byte> magic = reader.Read(8);
			if (Magic.AsSpan().SequenceCompareTo(magic) != 0)
			{
				throw new Exception("Invalid footer. Magic end missing. This is not a proper table file");
			}

			return new Footer(metaIndexHandle, indexHandle);
		}

		public void Write(Stream stream)
		{
			long footerStartPos = stream.Position;
			stream.Write(MetaIndexBlockHandle.Encode());
			stream.Write(BlockIndexBlockHandle.Encode());
			long paddingLen = FooterLength - 8 - (stream.Position - footerStartPos);
			stream.Write(new byte[paddingLen]); // padding
			stream.Write(Magic);
			if (stream.Position - footerStartPos != FooterLength)
				throw new Exception($"Footer exceeded allowed size by {FooterLength - (stream.Position - footerStartPos)}. Padded with {paddingLen}");
		}
	}
}