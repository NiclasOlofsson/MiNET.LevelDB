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
using log4net;
using MiNET.LevelDB.Utils;

namespace MiNET.LevelDB
{
	public class LogWriter : IDisposable
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(LogWriter));

		const int BlockSize = 32768; //TODO: This is the size of the blocks. Note they are padded. Use it!
		const int HeaderSize = 4 + 2 + 1; // Max block size need to include space for header.

		private readonly FileInfo _file;
		private Stream _stream;
		private bool _keepOpen;

		internal LogWriter(FileInfo file)
		{
			_file = file;
		}

		// For testing
		internal LogWriter(Stream stream, bool keepOpen = true)
		{
			_stream = stream;
			_keepOpen = keepOpen;
		}

		internal void WriteData(ReadOnlySpan<byte> data)
		{
			if (_stream == null)
			{
				_keepOpen = false;
				//if (_file.Exists) _file.Delete();
				_stream = _file.OpenWrite();
				_stream.Seek(0, SeekOrigin.End);
			}

			WriteData(_stream, data);
		}


		private void WriteData(Stream stream, ReadOnlySpan<byte> data)
		{
			var reader = new SpanReader(data);

			LogRecordType currentRecordType = LogRecordType.Zero;

			while (!reader.Eof)
			{
				int sizeLeft = (int) (BlockSize - stream.Position % BlockSize);
				int bytesLeft = reader.Length - reader.Position;
				int length = 0;

				if (currentRecordType == LogRecordType.Zero || currentRecordType == LogRecordType.Last || currentRecordType == LogRecordType.Full)
				{
					if (sizeLeft < 7)
					{
						//throw new Exception($"Size left={sizeLeft}");
						// pad with zeros
						stream.Seek(sizeLeft, SeekOrigin.Current);
						currentRecordType = LogRecordType.Zero;
						continue;
					}

					if (sizeLeft == 7)
					{
						//throw new Exception($"Size left={sizeLeft}");
						// emit empty first block
						currentRecordType = LogRecordType.First;
						WriteRecord(stream, currentRecordType, ReadOnlySpan<byte>.Empty);
						continue;
					}

					if (sizeLeft >= bytesLeft + 7)
					{
						currentRecordType = LogRecordType.Full;
						length = bytesLeft;
					}
					else
					{
						currentRecordType = LogRecordType.First;
						length = sizeLeft - 7;
					}
				}
				else if (currentRecordType == LogRecordType.First || currentRecordType == LogRecordType.Middle)
				{
					if (sizeLeft >= bytesLeft + 7)
					{
						currentRecordType = LogRecordType.Last;
						length = bytesLeft;
					}
					else
					{
						currentRecordType = LogRecordType.Middle;
						length = sizeLeft - 7;
					}
				}
				else
				{
					throw new Exception("Unexpected state while writing fragments");
				}

				var fragmentData = reader.Read(length);
				WriteRecord(stream, currentRecordType, fragmentData);
			}
		}

		private void WriteRecord(Stream stream, LogRecordType recordType, in ReadOnlySpan<byte> fragmentData)
		{
			uint crc = Crc32C.Compute((byte) recordType);
			crc = Crc32C.Mask(Crc32C.Append(crc, fragmentData));

			stream.Write(BitConverter.GetBytes(crc));
			stream.Write(BitConverter.GetBytes((ushort) fragmentData.Length));
			stream.Write(new[] {(byte) recordType});
			stream.Write(fragmentData);
		}

		public void Close()
		{
			Dispose();
		}

		public void Dispose()
		{
			if (!_keepOpen) _stream?.Dispose();
		}
	}
}