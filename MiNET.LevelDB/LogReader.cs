using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Force.Crc32;
using log4net;

namespace MiNET.LevelDB
{
	public class LogReader
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(LogReader));

		const int BlockSize = 32768; //TODO: This is the size of the blocks. Note they are padded. Use it!
		const int HeaderSize = 4 + 2 + 1; // Max block size need to include space for header.

		protected readonly FileInfo _file;
		private Stream _logStream; // global log stream
		private MemoryStream _blockStream; // Keep track of current block in a stream
		Dictionary<byte[], ResultCacheEntry> _resultCache = null;

		public LogReader(FileInfo file)
		{
			_file = file;
			_logStream = File.OpenRead(file.FullName);
		}

		public ResultStatus Get(Span<byte> key)
		{
			if (_resultCache == null)
			{
				LoadRecords();
			}

			BytewiseComparator comparator = new BytewiseComparator();
			foreach (var entry in _resultCache)
			{
				if (comparator.Compare(key, entry.Key) == 0)
				{
					return new ResultStatus(entry.Value.ResultState, entry.Value.Data);
				}
			}

			return ResultStatus.NotFound;
		}

		internal class ResultCacheEntry
		{
			public byte[] Data { get; set; }
			public ResultState ResultState { get; set; } = ResultState.Undefined;
		}

		private void LoadRecords()
		{
			_resultCache = new Dictionary<byte[], ResultCacheEntry>();

			while (true)
			{
				Record record = ReadRecord();

				if (record == null || record.LogRecordType == LogRecordType.Eof)
				{
					Log.Debug("Reached end of records: " + record);
					break;
				}

				if (record.LogRecordType != LogRecordType.Full) throw new Exception($"Invalid log file. Didn't find any records. Got record of type {record.LogRecordType}");

				SpanReader reader = new SpanReader(record.Data);

				long sequenceNumber = reader.ReadInt64();
				long size = reader.ReadInt32();

				while (reader.Position < reader.Length)
				{
					byte recType = reader.ReadByte();

					ulong v1 = reader.ReadVarLongInternal();
					byte[] currentKey = reader.Read((int) v1).ToArray();

					if (recType == 1)
					{
						var v2 = reader.ReadVarLong();

						var currentVal = reader.Read((int) v2);
						_resultCache.Add(currentKey, new ResultCacheEntry {ResultState = ResultState.Exist, Data = currentVal.ToArray()});
					}
					else if (recType == 0)
					{
						// says return "not found" in this case. Need to investigate since I believe there can multiple records with same key in this case.
						_resultCache.Add(currentKey, new ResultCacheEntry {ResultState = ResultState.Deleted});
					}
					else
					{
						// unknown recType
					}
				}
			}
		}

		protected void Reset()
		{
			_blockStream = null;
			_logStream.Position = 0;
		}

		public Record ReadRecord()
		{
			Record lastRecord = null;
			while (true)
			{
				if (_blockStream == null || _blockStream.Position >= _blockStream.Length)
				{
					byte[] buffer = new byte[BlockSize];
					if (_logStream.Read(buffer, 0, BlockSize) == 0)
					{
						Log.Debug("Reached end of file stream");
						break;
					}

					_blockStream = new MemoryStream(buffer);
				}

				Stream stream = _blockStream;

				while (true)
				{
					Record record = ReadFragments(stream);

					if (record == null)
					{
						Log.Debug("Reached end of block" + record);
						break;
					}

					if (record.LogRecordType == LogRecordType.Zero)
					{
						// Just ignore for now
						continue;
					}

					if (record.LogRecordType == LogRecordType.First)
					{
						Log.Debug("Read first part of full record fragment");
						lastRecord = record;
						continue;
					}

					if (lastRecord != null && (record.LogRecordType == LogRecordType.Middle || record.LogRecordType == LogRecordType.Last))
					{
						lastRecord.Length += record.Length;
						lastRecord.Data = lastRecord.Data.Concat(record.Data).ToArray();
						lastRecord.Checksum = 0;

						if (record.LogRecordType == LogRecordType.Middle)
						{
							Log.Debug("Read middle part of full record fragment");
							continue;
						}

						Log.Debug("Assembled all parts of fragment to full record");
						record = lastRecord;
						record.LogRecordType = LogRecordType.Full;
					}

					lastRecord = null;

					if (record.LogRecordType != LogRecordType.Full)
					{
						Log.Warn($"Read unhandled record of type {record.LogRecordType}");
						Log.Debug($"{record}");
						continue;
					}

					return record;
				}
			}

			return new Record() {LogRecordType = LogRecordType.Eof};
		}

		public static Record ReadFragments(Stream stream)
		{
			// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
			byte[] header = new byte[4 + 2 + 1];
			if (stream.Read(header, 0, header.Length) != header.Length) return null;

			uint expectedCrc = BitConverter.ToUInt32(header, 0);

			ushort length = BitConverter.ToUInt16(header, 4);

			byte type = header[6];

			byte[] data = new byte[length];
			stream.Read(data, 0, data.Length);

			uint actualCrc = Crc32CAlgorithm.Compute(new[] {type});
			actualCrc = Crc32CAlgorithm.Append(actualCrc, data);
			actualCrc = BlockHandle.Mask(actualCrc);

			Record rec = new Record()
			{
				Checksum = expectedCrc,
				Length = length,
				LogRecordType = (LogRecordType) type,
				Data = data
			};


			if (rec.LogRecordType != LogRecordType.Zero && expectedCrc != actualCrc)
			{
				throw new InvalidDataException($"Corrupted data. Failed checksum test. Excpeted {expectedCrc}, but calculated actual {actualCrc}");
			}

			return rec;
		}
	}

	public enum LogRecordType
	{
		// Zero is reserved for preallocated files
		Zero = 0,

		Full = 1,

		// For fragments
		First = 2,
		Middle = 3,
		Last = 4,

		// Util
		Eof = Last + 1,
		BadRecord = Last + 2,
	}

	public class Record
	{
		public LogRecordType LogRecordType { get; set; } = LogRecordType.Zero;
		public uint Checksum { get; set; }
		public ulong Length { get; set; }
		public byte[] Data { get; set; }

		public override string ToString()
		{
			return $"{nameof(LogRecordType)}: {LogRecordType}, {nameof(Length)}: {Length}, {nameof(Checksum)}: {Checksum}, "
					+ $"{nameof(Data)}:\n{Data?.HexDump(cutAfterFive: Data.Length > 16*10)}"
				;
		}
	}
}