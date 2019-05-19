using System;
using System.Collections.Generic;
using System.Linq;
using log4net;
using MiNET.LevelDB.Utils;

namespace MiNET.LevelDB
{
	public class MemCache
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(MemCache));

		internal Dictionary<byte[], ResultCacheEntry> _resultCache;
		private BytewiseComparator _comparator = new BytewiseComparator();

		public MemCache()
		{
		}

		public void Write(LogWriter writer)
		{
			var groups = _resultCache.GroupBy(kvp => kvp.Value.Sequence);
			foreach (var group in groups)
			{
				var operations = group.ToArray();
				var batch = EncodeBatch(operations);
				writer.EncodeBlocks(batch);
			}
		}

		internal ReadOnlySpan<byte> EncodeBatch(KeyValuePair<byte[], ResultCacheEntry>[] operations)
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

		public class ByteArrayComparer : IEqualityComparer<byte[]>
		{
			public bool Equals(byte[] left, byte[] right)
			{
				if (left == null || right == null)
				{
					return left == right;
				}
				return left.SequenceEqual(right);
			}

			public int GetHashCode(byte[] key)
			{
				if (key == null)
					throw new ArgumentNullException("key");
				return key.Sum(b => b);
			}
		}


		internal void Load(LogReader reader)
		{
			//_resultCache = new Dictionary<byte[], ResultCacheEntry>();
			_resultCache = new Dictionary<byte[], ResultCacheEntry>(new ByteArrayComparer());

			while (true)
			{
				Record record = reader.ReadRecord();

				if (record.LogRecordType == LogRecordType.Eof)
				{
					if (Log.IsDebugEnabled) Log.Debug($"Reached end of records: {record.ToString()}");
					break;
				}

				if (record.LogRecordType != LogRecordType.Full) throw new Exception($"Invalid log file. Didn't find any records. Got record of type {record.LogRecordType}");

				if (record.Length != (ulong) record.Data.Length) throw new Exception($"Invalid record state. Length not matching");

				var entries = DecodeBatch(record.Data);
				foreach (var entry in entries)
				{
					//_resultCache.TryAdd(entry.Key, entry.Value);
					_resultCache[entry.Key] = entry.Value;
				}
			}

			_resultCache = _resultCache.OrderByDescending(kvp => kvp.Value.Sequence).ToDictionary(k => k.Key, k => k.Value);
		}

		private List<KeyValuePair<byte[], ResultCacheEntry>> DecodeBatch(ReadOnlySpan<byte> data)
		{
			SpanReader batchReader = new SpanReader(data);

			var sequenceNumber = batchReader.ReadInt64();
			var operationCount = batchReader.ReadInt32();

			var result = new List<KeyValuePair<byte[], ResultCacheEntry>>(operationCount);

			for (int i = 0; i < operationCount; i++)
			{
				byte operationCode = batchReader.ReadByte();

				var keyLength = batchReader.ReadVarLong();
				var currentKey = batchReader.Read(keyLength);

				if (operationCode == (int) OperationType.Put) // Put
				{
					ulong valueLength = batchReader.ReadVarLong();

					var currentVal = batchReader.Read(valueLength);
					result.Add(new KeyValuePair<byte[], ResultCacheEntry>(currentKey.ToArray(), new ResultCacheEntry {Sequence = sequenceNumber, ResultState = ResultState.Exist, Data = currentVal.ToArray()}));
				}
				else if (operationCode == (int) OperationType.Delete) // Delete
				{
					// says return "not found" in this case. Need to investigate since I believe there can multiple records with same key in this case.
					result.Add(new KeyValuePair<byte[], ResultCacheEntry>(currentKey.ToArray(), new ResultCacheEntry {Sequence = sequenceNumber, ResultState = ResultState.Deleted}));
				}
				else
				{
					// unknown recType
				}
			}

			return result;
		}

		public ResultStatus Get(Span<byte> key)
		{
			if (_resultCache == null) throw new InvalidOperationException("Log not prepared for queries. Did you forget to call Open()?");

			foreach (var entry in _resultCache.OrderByDescending(kvp => kvp.Value.Sequence))
			{
				if (_comparator.Compare(key, entry.Key) == 0)
				{
					return new ResultStatus(entry.Value.ResultState, entry.Value.Data);
				}
			}

			return ResultStatus.NotFound;
		}

		public void Put(in Span<byte> key, in Span<byte> value)
		{
			if (_resultCache == null) throw new InvalidOperationException("Log not prepared for updates. Did you forget to call Open()?");

			var seq = _resultCache.Max(kvp => kvp.Value.Sequence) + 1; // Perhaps use DateTime.Ticks

			_resultCache[key.ToArray()] = new ResultCacheEntry {Sequence = seq, Data = value.ToArray(), ResultState = ResultState.Exist};
		}

		internal class ResultCacheEntry
		{
			public long Sequence { get; set; }
			public byte[] Data { get; set; }
			public ResultState ResultState { get; set; } = ResultState.Undefined;
		}
	}

	internal class TestCompare : IComparer<KeyValuePair<byte[], MemCache.ResultCacheEntry>>
	{
		private BytewiseComparator _comparator = new BytewiseComparator();

		public int Compare(KeyValuePair<byte[], MemCache.ResultCacheEntry> a, KeyValuePair<byte[], MemCache.ResultCacheEntry> b)
		{
			var comp = _comparator.Compare(a.Key, b.Key);
			if (comp != 0) return comp;

			var aseq = a.Value.Sequence;
			var bseq = b.Value.Sequence;

			if (aseq > bseq)
				return -1;

			if (aseq < bseq)
				return 1;

			return 0;
		}
	}
}