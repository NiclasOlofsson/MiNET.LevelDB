using System;
using System.Collections.Generic;

namespace MiNET.LevelDB.Utils
{
	public class BytewiseComparator : IComparer<byte[]>
	{
		public string Name { get; } = "leveldb.BytewiseComparator";

		public int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
		{
			if (a.Length == b.Length)
			{
				var result = a.SequenceCompareTo(b);
				return result == 0 ? 0 : result > 0 ? 1 : -1;
			}
			else
			{
				var maxLen = Math.Min(a.Length, b.Length);
				var result = a.Slice(0, maxLen).SequenceCompareTo(b.Slice(0, maxLen));
				if (result != 0) return result > 0 ? 1 : -1;

				result = a.Length - b.Length;
				return result > 0 ? 1 : -1;
			}
		}

		public void FindShortestSeparator(string start, Span<byte> limit)
		{
		}

		public void FindShortSuccessor(string key)
		{
		}

		public int Compare(byte[] x, byte[] y)
		{
			return Compare(x.AsSpan(), y.AsSpan());
		}
	}
}