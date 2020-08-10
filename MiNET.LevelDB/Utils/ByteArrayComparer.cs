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

namespace MiNET.LevelDB.Utils
{
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
			if (key == null) throw new ArgumentNullException("key");

			unchecked
			{
				int result = 0;
				foreach (byte b in key) result = (result * 31) ^ b;
				return result;
			}
		}
	}

	public class MemoryComparer : IEqualityComparer<ReadOnlyMemory<byte>>
	{
		public bool Equals(ReadOnlyMemory<byte> left, ReadOnlyMemory<byte> right)
		{
			return left.Span.SequenceEqual(right.Span);
		}

		public int GetHashCode(ReadOnlyMemory<byte> key)
		{
			return key.ToArray().Sum(b => b);
		}
	}
}