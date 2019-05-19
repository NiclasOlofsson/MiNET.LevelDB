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

		internal void EncodeBlocks(ReadOnlySpan<byte> data)
		{
			if (_stream == null)
			{
				_keepOpen = false;
				if (_file.Exists) _file.Delete();
				_stream = _file.OpenWrite();
			}

			EncodeBlocks(_stream, data);
		}


		internal void EncodeBlocks(Stream stream, ReadOnlySpan<byte> data)
		{
			SpanReader reader = new SpanReader(data);

			var currentRecordType = LogRecordType.Zero;

			while (!reader.Eof)
			{
				int sizeLeft = (int) (BlockSize - stream.Position%BlockSize);
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
						WriteFragment(stream, currentRecordType, ReadOnlySpan<byte>.Empty);
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
				WriteFragment(stream, currentRecordType, fragmentData);
			}
		}

		private void WriteFragment(Stream stream, LogRecordType recordType, in ReadOnlySpan<byte> fragmentData)
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