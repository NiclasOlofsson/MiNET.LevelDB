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
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using log4net;
using MiNET.LevelDB.Utils;

namespace MiNET.LevelDB
{
	/// <summary>
	///     Writes new tables to a stream.
	/// </summary>
	public class TableCreator
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(TableCreator));

		private readonly Stream _stream;
		private BlockCreator _blockCreator = new BlockCreator();
		private BlockCreator _blockIndexCreator = new BlockCreator();
		private BlockCreator _filterIndexCreator = new BlockCreator();
		private List<byte[]> _pendingIndexes = new List<byte[]>();

		public TableCreator(Stream stream)
		{
			_stream = stream;
		}

		public void Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data)
		{
			_blockCreator.Add(key, data);

			if (_blockCreator.CurrentSize > 4096)
			{
				Log.Debug($"Flush because size is bigger than 4k bytes: {_blockCreator.CurrentSize}");
				Flush();
			}
		}

		private void Flush()
		{
			byte[] lastKey = _blockCreator.LastKey;
			var handle = WriteBlock(_stream, _blockCreator);
			_blockIndexCreator.Add(lastKey, handle.Encode());
		}

		public void Finish()
		{
			Flush();

			// writer filters block
			// writer meta index block
			//BlockHandle metaIndexHandle = WriteBlock(_stream, null); //TODO
			BlockHandle metaIndexHandle = WriteBlock(_stream, _filterIndexCreator);
			// write block index
			BlockHandle blockIndexHandle = WriteBlock(_stream, _blockIndexCreator);

			// write footer
			var footer = new Footer(metaIndexHandle, blockIndexHandle);
			footer.Write(_stream);
		}

		private static BlockHandle WriteBlock(Stream stream, BlockCreator blockCreator)
		{
			byte[] dataBlock = blockCreator.Finish();
			if (dataBlock.Length == 0) return null;

			// Compress here

			//byte compressionType = 0; // none

			//byte compressionType = 2; // zlib
			//memStream.WriteByte(0x87);
			//memStream.WriteByte(0x9C);

			byte compressionType = 4; // zlib raw
			using var memStream = new MemoryStream();
			using var compStream = new DeflateStream(memStream, CompressionLevel.Optimal);
			compStream.Write(dataBlock);
			compStream.Flush();
			dataBlock = memStream.ToArray();

			uint checkCrc = Crc32C.Compute(dataBlock);
			checkCrc = Crc32C.Mask(Crc32C.Append(checkCrc, compressionType));

			long offset = stream.Position;
			stream.Write(dataBlock);
			stream.WriteByte(compressionType);
			stream.Write(BitConverter.GetBytes(checkCrc));

			stream.Flush();

			return new BlockHandle((ulong) offset, (ulong) dataBlock.Length);
		}
	}
}