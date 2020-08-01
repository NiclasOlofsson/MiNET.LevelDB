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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using MiNET.LevelDB.Utils;

namespace MiNET.LevelDB.Enumerate
{
	public class MergeEnumerator : IEnumerator<BlockEntry>, IEnumerable<BlockEntry>
	{
		private readonly List<TableEnumerator> _enumerators;
		private BytewiseComparator _comparator = new BytewiseComparator();
		private Dictionary<ReadOnlyMemory<byte>, BlockEntry> _currentEntries = new Dictionary<ReadOnlyMemory<byte>, BlockEntry>(new MemoryComparer());

		public MergeEnumerator(List<TableEnumerator> enumerators)
		{
			_enumerators = enumerators;

			Reset();
		}

		public bool MoveNext()
		{
			var sorted = new SortedList<ReadOnlyMemory<byte>, BlockEntry>(_currentEntries, new BytewiseMemoryComparator(true));
			if (sorted.Count == 0) return false;

			ReadOnlyMemory<byte> first = sorted.Keys.First();
			if (_currentEntries.Remove(first, out BlockEntry entry))
			{
				AddNewCurrent(entry.Key);
			}
			Current = entry;
			return Current != null;
		}

		private void AddNewCurrent(ReadOnlyMemory<byte> toReplace)
		{
			foreach (TableEnumerator enumerator in _enumerators)
			{
				if(enumerator.Current == null) continue;

				if (enumerator.Current.Key.Equals(toReplace))
				{
					if (!enumerator.MoveNext())
					{
						break;
					}

					_currentEntries[enumerator.Current.Key] = enumerator.Current;
					return;
				}
			}
		}

		public void Reset()
		{
			_currentEntries.Clear();
			foreach (TableEnumerator enumerator in _enumerators)
			{
				enumerator.Reset();
				_currentEntries[enumerator.Current.Key] = enumerator.Current;
			}
		}

		public BlockEntry Current { get; private set; }

		object? IEnumerator.Current => Current;

		public void Dispose()
		{
		}

		public IEnumerator<BlockEntry> GetEnumerator()
		{
			return this;
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}
	}
}