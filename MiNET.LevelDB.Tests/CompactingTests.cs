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
using MiNET.LevelDB.Enumerate;
using MiNET.LevelDB.Utils;
using NUnit.Framework;

namespace MiNET.LevelDB.Tests
{
	[TestFixture]
	public class CompactingTests
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(CompactingTests));

		[Test]
		public void TableEnumeratorShouldIterateAllKeys()
		{
			var fileInfo = new FileInfo(Path.Combine(TestUtils.GetTestDirectory().FullName, "000050.ldb"));
			using var table = new Table(fileInfo);

			// Just initialize the block first.
			table.Initialize();

			int count = 0;
			foreach (BlockEntry blockEntry in table)
			{
				Log.Debug($"Current Key:{blockEntry.Key.ToHexString()}");
				Assert.AreNotEqual(0, blockEntry.Key.Length);
				count++;
			}

			Assert.AreEqual(5322, count);
		}

		[Test]
		public void CompactAllLevels()
		{
			DirectoryInfo dir = TestUtils.GetTestDirectory(false);

			// Setup new database and generate values enough to create 2 level 0 tables with overlapping keys.
			// We use this when we run the real test.
			using (var db = new Database(dir, true, new Options(){LevelSizeBaseFactor = 3}))
			{
				db.Open();

				for (int i = 0; i < 8000; i++)
				{
					byte[] key = TestUtils.FillArrayWithRandomBytes(14);
					byte[] data = TestUtils.FillArrayWithRandomBytes(1000, 128);
					db.Put(key, data);
				}

				//{
				//	int count = db.TryCompactLevel0();
				//	Assert.AreEqual(7752, count); // Some are in memcache and wasn't flushed. It's ok.
				//}

				for (int i = 0; i < 4000; i++)
				{
					byte[] key = TestUtils.FillArrayWithRandomBytes(14);
					byte[] data = TestUtils.FillArrayWithRandomBytes(1000, 128);
					db.Put(key, data);
				}

				//{
				//	int count = db.TryCompactLevel0();
				//	// This count is the combined records for both level 0 (newly added)
				//	// and level 1 (the previously compacted file(s).
				//	// Some are in memcache and wasn't flushed. It's ok. 
				//	Assert.AreEqual(11628, count);
				//	db.TryCompactLevelN();
				//}

				for (int j = 0; j < 5; j++)
				{
					for (int i = 0; i < 8000; i++)
					{
						byte[] key = TestUtils.FillArrayWithRandomBytes(14);
						byte[] data = TestUtils.FillArrayWithRandomBytes(1000, 128);
						db.Put(key, data);
					}
				}

				db.Close();
			}
		}
	}
}