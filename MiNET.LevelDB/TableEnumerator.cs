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
using log4net;

namespace MiNET.LevelDB
{
	public class BlockEntry
	{
		public ReadOnlyMemory<byte> Key { get; set; }
		public ReadOnlyMemory<byte> Data { get; set; }
	}

	public class TableEnumerator : IEnumerator<BlockEntry>, IEnumerable<BlockEntry>
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(TableEnumerator));

		private readonly Table _table;
		private readonly BlockEnumerator _blockIndexEnum;

		private BlockEnumerator _currentBlockEnum;

		public TableEnumerator(Table table)
		{
			_table = table;
			_blockIndexEnum = new BlockEnumerator(table._blockIndex);

			Reset();
		}

		public bool MoveNext()
		{
			if (!_currentBlockEnum.MoveNext())
			{
				Log.Debug($"Data block empty. Moving to next block.");

				if (!_blockIndexEnum.MoveNext())
				{
					Log.Debug($"Block index empty");
					return false;
				}

				BlockEntry entry = _blockIndexEnum.Current;
				if (entry == null)
				{
					Log.Warn($"Unexpected empty index entry");
					return false;
				}

				byte[] dataBlock = _table.GetBlock(BlockHandle.ReadBlockHandle(entry.Data.Span));
				_currentBlockEnum = new BlockEnumerator(dataBlock);
			}

			Current = _currentBlockEnum.Current;

			return Current != null;
		}

		public void Reset()
		{
			_blockIndexEnum.Reset();

			BlockEntry entry = _blockIndexEnum.Current;
			if (entry == null) throw new Exception("Error");

			byte[] dataBlock = _table.GetBlock(BlockHandle.ReadBlockHandle(entry.Data.Span));
			_currentBlockEnum = new BlockEnumerator(dataBlock);
			Current = _currentBlockEnum.Current;
		}

		public BlockEntry Current { get; private set; }

		object? IEnumerator.Current => Current;

		public void Dispose()
		{
		}

		public IEnumerator<BlockEntry> GetEnumerator()
		{
			return _blockIndexEnum;
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}
	}
}