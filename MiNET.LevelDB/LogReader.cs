using System;
using System.IO;
using Force.Crc32;
using log4net;
using MiNET.LevelDB.Utils;

namespace MiNET.LevelDB
{
	public class LogReader : IDisposable
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(LogReader));

		const int BlockSize = 32768; //TODO: This is the size of the blocks. Note they are padded. Use it!
		const int HeaderSize = 4 + 2 + 1; // Max block size need to include space for header.

		protected readonly FileInfo _file;
		private Stream _logStream; // global log stream

		internal LogReader(Stream logStream = null)
		{
			_logStream = logStream;
		}

		public LogReader(FileInfo file)
		{
			_file = file;
			if (!_file.Exists)
			{
				_logStream = new MemoryStream();
			}
			else
			{
				_logStream = File.OpenRead(file.FullName);
			}
		}

		public virtual void Open()
		{
		}

		public void Close()
		{
			Dispose();
		}

		protected void Reset()
		{
			_logStream.Position = 0;
		}

		public Record ReadRecord()
		{
			Record lastRecord = Record.Undefined;

			while (_logStream.Position < _logStream.Length)
			{
				Stream stream = _logStream;

				while (true)
				{
					Record record = ReadFragments(stream);

					if (record.LogRecordType == LogRecordType.BadRecord)
					{
						if (Log.IsDebugEnabled) Log.Debug($"Reached end of block {record.ToString()}");
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

					if (!lastRecord.IsUndefined && (record.LogRecordType == LogRecordType.Middle || record.LogRecordType == LogRecordType.Last))
					{
						lastRecord.Length += record.Length;
						var lastData = lastRecord.Data;
						var destination = new Span<byte>(new byte[lastRecord.Data.Length + record.Data.Length]);
						lastData.CopyTo(destination);
						record.Data.CopyTo(destination.Slice(lastData.Length));
						lastRecord.Data = destination;

						if (record.LogRecordType == LogRecordType.Middle)
						{
							Log.Debug("Read middle part of full record fragment");
							continue;
						}

						Log.Debug("Assembled all parts of fragment to full record");
						record = lastRecord;
						record.LogRecordType = LogRecordType.Full;
					}

					lastRecord = Record.Undefined;

					if (record.LogRecordType != LogRecordType.Full)
					{
						Log.Warn($"Read unhandled record of type {record.LogRecordType}");
						if (Log.IsDebugEnabled) Log.Debug($"{record.ToString()}");
						continue;
					}

					return record;
				}
			}

			return new Record {LogRecordType = LogRecordType.Eof};
		}

		private Record ReadFragments(Stream stream)
		{
			// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
			byte[] header = new byte[4 + 2 + 1];
			if (stream.Read(header, 0, header.Length) != header.Length) return new Record(LogRecordType.BadRecord);

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

		public void Dispose()
		{
			_logStream?.Dispose();
		}
	}

	public enum OperationType
	{
		Delete = 0,
		Put = 1
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
		Undefined = Last + 2,
	}

	public ref struct Record
	{
		public static Record Undefined => new Record(LogRecordType.Undefined);

		public LogRecordType LogRecordType { get; set; }
		public uint Checksum { get; set; }
		public ulong Length { get; set; }
		public ReadOnlySpan<byte> Data { get; set; }
		public bool IsUndefined => LogRecordType == LogRecordType.Undefined;

		public Record(LogRecordType logRecordType)
		{
			LogRecordType = logRecordType;
			Checksum = 0;
			Length = 0;
			Data = ReadOnlySpan<byte>.Empty;
		}

		public override string ToString()
		{
			return $"{nameof(LogRecordType)}: {LogRecordType}, {nameof(Length)}: {Length}, {nameof(Checksum)}: {Checksum}, "
					+ $"{nameof(Data)}:\n{Data.HexDump(cutAfterFive: Data.Length > 16*10)}"
				;
		}
	}
}