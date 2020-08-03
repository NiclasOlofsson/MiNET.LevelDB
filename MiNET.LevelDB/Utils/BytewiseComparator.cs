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
using log4net;

namespace MiNET.LevelDB.Utils
{
	public class BytewiseComparator : IComparer<byte[]>
	{
		public string Name { get; } = "leveldb.BytewiseComparator";

		public BytewiseComparator()
		{
		}

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

		public int Compare(byte[] x, byte[] y)
		{
			return Compare(x.AsSpan(), y.AsSpan());
		}
	}

	public class BytewiseMemoryComparator : IComparer<ReadOnlyMemory<byte>>
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(BytewiseMemoryComparator));

		private readonly bool _userKeyOnly;
		public string Name { get; } = "leveldb.BytewiseComparator";

		public BytewiseMemoryComparator(bool userKeyOnly = false)
		{
			_userKeyOnly = userKeyOnly;
		}

		private int Compare(ReadOnlySpan<byte> ain, ReadOnlySpan<byte> bin)
		{
			ReadOnlySpan<byte> a = _userKeyOnly ? ain.UserKey() : ain;
			ReadOnlySpan<byte> b = _userKeyOnly ? bin.UserKey() : bin;

			if (a.Length == b.Length)
			{
				int result = a.SequenceCompareTo(b);
				if (result == 0)
				{
					// Reverse order for sequence compare
					return bin.SequenceNumber().CompareTo(ain.SequenceNumber());
				}
				else
				{
					return result > 0 ? 1 : -1;
				}
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

		public int Compare(ReadOnlyMemory<byte> x, ReadOnlyMemory<byte> y)
		{
			return Compare(x.Span, y.Span);
		}
	}
}