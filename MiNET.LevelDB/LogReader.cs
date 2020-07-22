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
	public class LogReader : IDisposable
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(LogReader));

		const int BlockSize = 32768; //TODO: This is the size of the blocks. Note they are padded. Use it!
		const int HeaderSize = 4 + 2 + 1; // Max block size need to include space for header.

		private Stream _stream; // global log stream

		public bool Eof { get; set; }

		// For testing
		internal LogReader(Stream stream = null)
		{
			_stream = stream;
		}

		public LogReader(FileInfo file)
		{
			if (!file.Exists)
			{
				_stream = new MemoryStream();
			}
			else
			{
				_stream = File.OpenRead(file.FullName);
			}
		}

		public virtual void Open()
		{
		}

		public void Close()
		{
			Dispose();
		}

		public void Dispose()
		{
			_stream?.Dispose();
		}

		protected void Reset()
		{
			_stream.Position = 0;
			Eof = false;
		}

		public ReadOnlySpan<byte> ReadData()
		{
			Record lastRecord = Record.Undefined;

			while (_stream.Position < _stream.Length)
			{
				Stream stream = _stream;

				while (true)
				{
					Record record = ReadNextRecord(stream);

					if (record.LogRecordType == LogRecordType.BadRecord)
					{
						//if (Log.IsDebugEnabled) Log.Debug($"Reached end of block {record.ToString()}");
						break;
					}

					if (record.LogRecordType == LogRecordType.Zero)
					{
						// Just ignore for now
						continue;
					}

					if (record.LogRecordType == LogRecordType.First)
					{
						//Log.Debug("Read first part of full record fragment");
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
							//if (Log.IsDebugEnabled) Log.Debug("Read middle part of full record fragment");
							continue;
						}

						//if (Log.IsDebugEnabled) Log.Debug("Assembled all parts of fragment to full record");
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

					return record.Data;
				}
			}
			Eof = true;
			return null;
		}

		private Record ReadNextRecord(Stream stream)
		{
			// Blocks may be padded if size left is less than the header
			int sizeLeft = (int) (BlockSize - stream.Position % BlockSize);
			if (sizeLeft < 7) stream.Seek(sizeLeft, SeekOrigin.Current);

			// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
			byte[] header = new byte[4 + 2 + 1];
			if (stream.Read(header, 0, header.Length) != header.Length) return new Record(LogRecordType.BadRecord);

			uint expectedCrc = BitConverter.ToUInt32(header, 0);

			ushort length = BitConverter.ToUInt16(header, 4);

			byte type = header[6];

			if (length > stream.Length - stream.Position) throw new Exception("Not enough data in stream to read");

			byte[] data = new byte[length];
			int read = stream.Read(data, 0, data.Length);

			uint actualCrc = Crc32C.Compute(type);
			actualCrc = Crc32C.Mask(Crc32C.Append(actualCrc, data));

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

	public enum OperationType
	{
		Delete = 0,
		Value = 1
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
					+ $"{nameof(Data)}:\n{Data.HexDump(cutAfterFive: Data.Length > 16 * 10)}"
				;
		}
	}
}