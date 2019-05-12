using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Force.Crc32;
using log4net;
using MiNET.LevelDB.Utils;

[assembly: InternalsVisibleTo("MiNET.LevelDBTests")]

namespace MiNET.LevelDB
{
	public class LogWriter
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(LogWriter));

		const int BlockSize = 32768; //TODO: This is the size of the blocks. Note they are padded. Use it!
		const int HeaderSize = 4 + 2 + 1; // Max block size need to include space for header.

		private readonly FileInfo _file;
		private Dictionary<byte[], LogReader.ResultCacheEntry> _resultCache;

		internal LogWriter(FileInfo file, Dictionary<byte[], LogReader.ResultCacheEntry> resultCache)
		{
			_file = file;
			_resultCache = resultCache;
		}

		internal LogWriter()
		{
		}


		public void Write()
		{
			using (var fileStream = _file.OpenWrite())
			{
				var groups = _resultCache.GroupBy(kvp => kvp.Value.Sequence);
				foreach (var group in groups)
				{
					var operations = @group.ToArray();
					var batch = EncodeBatch(operations);
					EncodeBlocks(fileStream, batch);
				}
			}
		}

		internal ReadOnlySpan<byte> EncodeBatch(KeyValuePair<byte[], LogReader.ResultCacheEntry>[] operations)
		{
			if (operations.Length == 0) throw new ArgumentException("Zero size batch", nameof(operations));

			long maxSize = 0;
			maxSize += 8; // sequence
			maxSize += 4*operations.Length; // count
			foreach (var entry in operations)
			{
				maxSize += 1; // op code
				maxSize += 10; // varint max
				maxSize += entry.Key.Length;
				if (entry.Value.ResultState == ResultState.Exist)
				{
					maxSize += 10; // varint max
					maxSize += entry.Value.Data?.Length ?? 0;
				}
			}

			Span<byte> data = new byte[maxSize]; // big enough to contain all data regardless of size

			//MemoryStream stream = new MemoryStream(data);
			//BinaryWriter writer = new BinaryWriter(stream);

			var writer = new SpanWriter(data);

			// write sequence
			writer.Write(operations.First().Value.Sequence);
			// write operations count
			writer.Write((int) operations.Length);

			foreach (var operation in operations)
			{
				var key = operation.Key;
				var entry = operation.Value;
				// write op type (byte)
				writer.Write(entry.ResultState == ResultState.Exist ? (byte) OperationType.Put : (byte) OperationType.Delete);
				// write key len (varint)
				writer.WriteVarInt((ulong) key.Length);
				// write key
				writer.Write(key);

				if (entry.ResultState == ResultState.Exist)
				{
					// write data len (varint)
					writer.WriteVarInt((ulong) entry.Data.Length);
					// write data
					writer.Write(entry.Data);
				}
			}

			return data.Slice(0, writer.Position);
		}

		internal void EncodeBlocks(Stream stream, ReadOnlySpan<byte> data)
		{
			SpanReader reader = new SpanReader(data);

			var currentRecordType = LogRecordType.Zero;

			while (reader.Position < reader.Length)
			{
				int sizeLeft = (int) (BlockSize - stream.Position%BlockSize);
				int bytesLeft = reader.Length - reader.Position;
				int length = 0;

				if (currentRecordType == LogRecordType.Zero || currentRecordType == LogRecordType.Last || currentRecordType == LogRecordType.Full)
				{
					if (sizeLeft < 7)
					{
						// pad with zeros
						stream.Seek(sizeLeft, SeekOrigin.Current);
						currentRecordType = LogRecordType.Zero;
						continue;
					}

					if (sizeLeft == 7)
					{
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
			uint crc = Crc32CAlgorithm.Compute(new[] {(byte) recordType});
			crc = Crc32CAlgorithm.Append(crc, fragmentData.ToArray());
			crc = BlockHandle.Mask(crc);

			stream.Write(BitConverter.GetBytes(crc));
			stream.Write(BitConverter.GetBytes((ushort) fragmentData.Length));
			stream.Write(new[] {(byte) recordType});
			stream.Write(fragmentData);
		}
	}
}