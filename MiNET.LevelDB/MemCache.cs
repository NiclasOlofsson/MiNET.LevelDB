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
using System.Linq;
using log4net;
using MiNET.LevelDB.Utils;

namespace MiNET.LevelDB
{
	public class MemCache
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(MemCache));

		internal Dictionary<byte[], ResultCacheEntry> _resultCache = new Dictionary<byte[], ResultCacheEntry>(new ByteArrayComparer());
		private BytewiseComparator _comparator = new BytewiseComparator();
		private ulong _estimatedSize = 0;

		public MemCache()
		{
		}

		public ulong GetEstimatedSize()
		{
			return _estimatedSize;
		}

		public void Write(LogWriter writer)
		{
			var groups = _resultCache.GroupBy(kvp => kvp.Value.Sequence);
			foreach (IGrouping<long, KeyValuePair<byte[], ResultCacheEntry>> group in groups)
			{
				KeyValuePair<byte[], ResultCacheEntry>[] operations = group.ToArray();
				ReadOnlySpan<byte> batch = EncodeBatch(operations);
				writer.WriteData(batch);
			}
		}

		internal ReadOnlySpan<byte> EncodeBatch(KeyValuePair<byte[], ResultCacheEntry>[] operations)
		{
			if (operations.Length == 0) throw new ArgumentException("Zero size batch", nameof(operations));

			long maxSize = 0;
			maxSize += 8; // sequence
			maxSize += 4 * operations.Length; // count
			foreach (KeyValuePair<byte[], ResultCacheEntry> entry in operations)
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
			writer.Write((ulong) operations.First().Value.Sequence);
			// write operations count
			writer.Write((uint) operations.Length);

			foreach (KeyValuePair<byte[], ResultCacheEntry> operation in operations)
			{
				byte[] key = operation.Key;
				ResultCacheEntry entry = operation.Value;
				// write op type (byte)
				writer.Write(entry.ResultState == ResultState.Exist ? (byte) OperationType.Value : (byte) OperationType.Delete);
				// write key
				writer.WriteLengthPrefixed(key);

				if (entry.ResultState == ResultState.Exist)
				{
					// write data
					writer.WriteLengthPrefixed(entry.Data);
				}
			}

			return data.Slice(0, writer.Position);
		}

		internal void Load(LogReader reader)
		{
			_resultCache = new Dictionary<byte[], ResultCacheEntry>(new ByteArrayComparer());

			int entriesCount = 0;

			while (true)
			{
				ReadOnlySpan<byte> data = reader.ReadData();

				if (reader.Eof)
				{
					if (Log.IsDebugEnabled) Log.Debug($"Reached end of stream. No more records to read.");
					break;
				}

				var entries = DecodeBatch(data);
				entriesCount += entries.Count;
				foreach (KeyValuePair<byte[], ResultCacheEntry> entry in entries.OrderBy(kvp => kvp.Value.Sequence))
				{
					// This should overwrite older entries and only the latest operation should be saved
					_resultCache[entry.Key] = entry.Value;
					_estimatedSize += (ulong) (entry.Key.Length + 8 + entry.Key.Length + 10 /* varlong * 2 */);
				}
			}
			Log.Debug($"Total count of entries read: {entriesCount}");
			Log.Debug($"Total count after filtering entries: {_resultCache.Count}");
		}

		private List<KeyValuePair<byte[], ResultCacheEntry>> DecodeBatch(ReadOnlySpan<byte> data)
		{
			var batchReader = new SpanReader(data);

			long sequenceNumber = (long) batchReader.ReadUInt64();
			int operationCount = (int) batchReader.ReadUInt32();

			var result = new List<KeyValuePair<byte[], ResultCacheEntry>>(operationCount);

			for (int i = 0; i < operationCount; i++)
			{
				byte operationCode = batchReader.ReadByte();

				ReadOnlySpan<byte> currentKey = batchReader.ReadLengthPrefixedBytes();

				if (operationCode == (int) OperationType.Value) // Put
				{
					ReadOnlySpan<byte> currentVal = batchReader.ReadLengthPrefixedBytes();
					result.Add(new KeyValuePair<byte[], ResultCacheEntry>(currentKey.ToArray(), new ResultCacheEntry
					{
						Sequence = sequenceNumber,
						ResultState = ResultState.Exist,
						Data = currentVal.ToArray()
					}));
				}
				else if (operationCode == (int) OperationType.Delete) // Delete
				{
					// says return "not found" in this case. Need to investigate since I believe there can multiple records with same key in this case.
					result.Add(new KeyValuePair<byte[], ResultCacheEntry>(currentKey.ToArray(), new ResultCacheEntry
					{
						Sequence = sequenceNumber,
						ResultState = ResultState.Deleted
					}));
				}
			}

			return result;
		}

		internal ResultStatus Get(Span<byte> key)
		{
			if (_resultCache == null) throw new InvalidOperationException("Log not prepared for queries. Did you forget to call Open()?");

			if (_resultCache.TryGetValue(key.ToArray(), out ResultCacheEntry entry))
			{
				return new ResultStatus(entry.ResultState, entry.Data);
			}

			return ResultStatus.NotFound;
		}

		internal void Put(WriteBatch batch)
		{
			if (_resultCache == null) throw new InvalidOperationException("Log not prepared for updates. Did you forget to call Open()?");

			foreach (BatchOperation operation in batch.Operations)
			{
				byte[] key = operation.Key;
				byte[] data = operation.Data;
				_estimatedSize += (ulong) (key.Length + 8 + data.Length + 10 /* varlong * 2 */);

				_resultCache[key] = new ResultCacheEntry
				{
					Sequence = (long) batch.Sequence,
					Data = data,
					ResultState = ResultState.Exist
				};
			}
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