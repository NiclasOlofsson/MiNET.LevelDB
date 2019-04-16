using System;
using System.IO;
using System.Linq;
using Crc32C;
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

		public LogReader(FileInfo file)
		{
			_file = file;
			_logStream = File.OpenRead(file.FullName);
		}

		public byte[] Get(Span<byte> key)
		{
			Reset();

			while (true)
			{
				Record record = ReadRecord();

				if (record == null)
				{
					Log.Debug("Reached end of block" + record);
					break;
				}

				if (record.LogRecordType != LogRecordType.Full) throw new Exception("Invalid log file. Didn't find any records");

				var stream = new MemoryStream(record.Data);
				var reader = new BinaryReader(stream);

				long sequenceNumber = reader.ReadInt64();
				long size = reader.ReadInt32();

				bool found = false;

				BytewiseComparator comparator = new BytewiseComparator();
				while (stream.Position < stream.Length)
				{
					byte recType = reader.ReadByte();

					ulong v1 = stream.ReadVarint();
					byte[] currentKey = new byte[v1];
					reader.Read(currentKey, 0, currentKey.Length);

					if (comparator.Compare(key, currentKey) == 0)
					{
						found = true;
					}

					ulong v2 = 0;
					byte[] currentVal = new byte[0];
					if (recType == 1)
					{
						v2 = stream.ReadVarint();
						currentVal = new byte[v2];
						reader.Read(currentVal, 0, (int) v2);

						if (found) return currentVal;
					}
					else if (recType == 0)
					{
						// says return "not found" in this case. Need to investigate since I believe there can multiple records with same key in this case.
						if (found) return null;
					}
					else
					{
						// unknown recType
					}

					Log.Debug($"RecType={recType}, Sequence={sequenceNumber}, Size={size}, v1={v1}, v2={v2}\nCurrentKey={currentKey.HexDump(currentKey.Length, false, false)}\nCurrentVal=\n{currentVal.HexDump(cutAfterFive: true)} ");
				}
			}

			return null;
		}

		protected void Reset()
		{
			_blockStream = null;
			_logStream.Position = 0;
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
		private static readonly ILog Log = LogManager.GetLogger(typeof(Record));

		public uint Checksum { get; set; }
		public ulong Length { get; set; }
		public LogRecordType LogRecordType { get; set; }
		public byte[] Data { get; set; }

		public override string ToString()
		{
			return $"{nameof(LogRecordType)}: {LogRecordType}, {nameof(Length)}: {Length}, {nameof(Checksum)}: {Checksum}, "
					+ $"{nameof(Data)}:\n{Data?.HexDump(cutAfterFive: Data.Length > 16*10)}"
				;
		}
	}
}