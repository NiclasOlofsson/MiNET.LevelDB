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
using System.IO;
using System.Linq;
using System.Reflection;
using log4net;
using log4net.Core;
using log4net.Repository.Hierarchy;
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
			var keys = new List<byte[]>();

			DirectoryInfo dir = TestUtils.GetTestDirectory(false);

			// Setup new database and generate values enough to create 2 level 0 tables with overlapping keys.
			// We use this when we run the real test.
			using (var db = new Database(dir, true, new Options() {LevelSizeBaseFactor = 3}))
			{
				db.Open();

				//for (int i = 0; i < 8000; i++)
				//{
				//	byte[] key = TestUtils.FillArrayWithRandomBytes(14);
				//	byte[] data = TestUtils.FillArrayWithRandomBytes(1000, 128);
				//	db.Put(key, data);
				//	keys.Add(key);
				//}

				//for (int i = 0; i < 4000; i++)
				//{
				//	byte[] key = TestUtils.FillArrayWithRandomBytes(14);
				//	byte[] data = TestUtils.FillArrayWithRandomBytes(1000, 128);
				//	db.Put(key, data);
				//	keys.Add(key);
				//}

				for (int j = 0; j < 3; j++)
				{
					for (int i = 0; i < 8000; i++)
					{
						byte[] key = TestUtils.FillArrayWithRandomBytes(14);
						byte[] data = TestUtils.FillArrayWithRandomBytes(1000, 128);
						db.Put(key, data);
						keys.Add(key);
					}
				}

				db.Close();
			}

			using (var db = new Database(dir, false, new Options() {LevelSizeBaseFactor = 3}))
			{
				db.Open();

				((Hierarchy) LogManager.GetRepository(Assembly.GetEntryAssembly())).Root.Level = Level.Info;

				int count = 0;
				foreach (byte[] key in keys)
				{
					if (db.Get(key) == null) Log.Error($"Missing key {key.ToHexString()} at idx:{count++}");
				}

				db.Close();
			}
		}

		[Test]
		public void KeyTest()
		{
			// f4 49 00 00 00 00 00 00 01 f4 49 00 00 00 00 00
			Span<byte> bytes = new byte[] {0xf4, 0x49, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xf4, 0x49, 0x00, 0x00, 0x00, 0x00, 0x00};
			Assert.IsTrue(bytes.UserKey().SequenceEqual(new byte[] {0xf4, 0x49, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}));
			Assert.AreEqual(18932, bytes.SequenceNumber());
			Assert.AreEqual(1, bytes.OperationType());
		}

		public static byte[] RemovePadding(byte[] bytes)
		{
			byte[] result = new byte[bytes.Length];
			bool isLeading = true;
			int idx = 0;
			Array.Reverse(bytes);
			Log.Debug($"input:{bytes.ToHexString()}");
			foreach (byte b in bytes)
			{
				if (isLeading && b == 0) continue;
				isLeading = false;
				result[idx++] = b;
			}

			Log.Debug($"output:{result.ToHexString()}");

			return result;
		}

		[Test]
		public void CompactNumeric()
		{
			var keys = new List<byte[]>();

			DirectoryInfo dir = TestUtils.GetTestDirectory(false);

			// Setup new database and generate values enough to create 2 level 0 tables with overlapping keys.
			// We use this when we run the real test.
			ulong idx = 0;

			var options = new Options()
			{
				LevelSizeBaseFactor = 10,
				RetainAllFiles = true
			};

			List<FileMetadata> level0Files;
			Version version = null;
			using (var db = new Database(dir, true, options))
			{
				db.Open();

				for (int j = 0; j < 4; j++)
				{
					for (int i = 0; i < 8000; i++)
					{
						byte[] key = BitConverter.GetBytes(idx++);
						byte[] data = TestUtils.FillArrayWithRandomBytes(1000, 128);
						db.Put(key, data);
						keys.Add(key);
					}
				}
				level0Files = new List<FileMetadata>(db.Level0Tables);
				version = db.Version;
				db.Close();
			}

			((Hierarchy) LogManager.GetRepository(Assembly.GetEntryAssembly())).Root.Level = Level.Warn;

			{
				Log.Warn($"Reading {keys.Count} values using regular db.get()");
				using (var db = new Database(dir, false, options))
				{
					db.Open();

					ulong count = 0;
					ulong countMissing = 0;
					foreach (byte[] key in keys)
					{
						byte[] value = db.Get(key);
						if (value == null) Log.Error($"Missing key {key.ToHexString()} at idx:{count}, {countMissing++}");
						count++;
					}

					db.Close();
				}
			}

			return;

			//{
			// Log.Warn($"Reading {keys.Count} values, from log files");
			//	List<byte[]> keysToRemove = new List<byte[]>(keys);
			//	FileInfo[] logFiles = dir.GetFiles("*.log");
			//	foreach (FileInfo fileInfo in logFiles)
			//	{
			//		Log.Warn($"Reading from {fileInfo.Name}. Have {keysToRemove.Count} keys left");
			//		using var reader = new LogReader(fileInfo.Open(FileMode.Open));
			//		var cache = new MemCache();
			//		cache.Load(reader);
			//		foreach (byte[] key in keysToRemove.Take(5000).ToArray())
			//		{
			//			if (cache.Get(key).State == ResultState.Exist)
			//			{
			//				keysToRemove.Remove(key);
			//			}
			//		}
			//	}
			//	Assert.AreEqual(0, keysToRemove.Count);
			//}

			int keysInLevel0 = 0;
			var keysInCurrentLog = new List<byte[]>();
			{
				Log.Warn($"Reading {keys.Count} values, from level0 files");

				List<byte[]> keysToRemove = new List<byte[]>(keys);
				var enumerators = new List<TableEnumerator>();
				foreach (FileMetadata fileMeta in level0Files.OrderBy(f => f.FileNumber))
				{
					string filePath = Path.Combine(dir.FullName, $"{fileMeta.FileNumber:000000}.ldb");
					var fileInfo = new FileInfo(filePath);
					Log.Warn($"Reading from {fileInfo.Name}. Have {keysToRemove.Count} keys left");
					var table = new Table(fileInfo);
					foreach (byte[] key in keysToRemove.ToArray())
					{
						if (table.Get(key).State == ResultState.Exist)
						{
							keysInLevel0++;
							keysToRemove.Remove(key);
						}
					}
					enumerators.Add((TableEnumerator) table.GetEnumerator());
				}

				Assert.Less(0, keysInLevel0);

				// Read the remaining from current log file

				{
					string filePath = Path.Combine(dir.FullName, $"{version.LogNumber:000000}.log");
					var fileInfo = new FileInfo(filePath);

					Log.Warn($"Reading remaining {keysToRemove.Count} values from current log {fileInfo.Name}");

					using var reader = new LogReader(fileInfo.Open(FileMode.Open));
					var cache = new MemCache();
					cache.Load(reader);
					foreach (byte[] key in keysToRemove.ToArray())
					{
						if (cache.Get(key).State == ResultState.Exist)
						{
							keysInCurrentLog.Add(key);
							keysToRemove.Remove(key);
						}
					}

					Assert.AreEqual(0, keysToRemove.Count);
				}

				{
					Log.Warn($"Reading {keysInLevel0} values, based on merge enumerator of all level0 table files");

					var enumerator = new MergeEnumerator(enumerators);
					int enumCount = 0;
					while (enumerator.MoveNext())
					{
						enumCount++;
					}

					Assert.AreEqual(keysInLevel0, enumCount);
					// Close the tables
					foreach (TableEnumerator tableEnumerator in enumerators)
					{
						tableEnumerator.TEST_Close();
					}
				}
			}

			{
				var keysLeftToRemove = new List<byte[]>(keys).Except(keysInCurrentLog).ToList();
				Log.Warn($"Reading {keysLeftToRemove.Count} values, from all level+1 files + current level0");

				var level1Enumerators = new List<TableEnumerator>();
				FileInfo[] tableFiles = dir.GetFiles("*.ldb");
				foreach (var fileInfo in tableFiles.OrderBy(f => f.Name))
				{
					if (level0Files.Any(f => $"{f.FileNumber:000000}.ldb" == fileInfo.Name))
					{
						if (version.GetFiles(0).All(f => $"{f.FileNumber:000000}.ldb" != fileInfo.Name)) continue;
						Log.Warn($"Reading current level0 file {fileInfo.Name}");
					}

					Log.Warn($"Reading from {fileInfo.Name}. Have {keysLeftToRemove.Count} keys left");
					var table = new Table(fileInfo);
					table.Initialize();
					level1Enumerators.Add((TableEnumerator) table.GetEnumerator());
					foreach (byte[] key in keysLeftToRemove.ToArray())
					{
						if (table.Get(key).State == ResultState.Exist) keysLeftToRemove.Remove(key);
					}
				}
				//Assert.AreEqual(0, keysLeftToRemove.Count); // FAIL

				{
					keysLeftToRemove = new List<byte[]>(keys).Except(keysInCurrentLog).ToList();
					Log.Warn($"Reading {keysLeftToRemove.Count} values, from all level+1 files + current level0 using merge enumerator");

					var enumerator = new MergeEnumerator(level1Enumerators);
					int enumCount = 0;
					while (enumerator.MoveNext())
					{
						enumCount++;
						if (enumerator.Current != null)
						{
							byte[] key = enumerator.Current.Key.Span.UserKey().ToArray();
							keysLeftToRemove.RemoveAll(bytes => new BytewiseComparator().Compare(bytes, key) == 0);
						}
						else Log.Warn($"Current in enumerator is null");
					}

					Assert.AreEqual(keys.Count - keysInCurrentLog.Count, enumCount, "Expected to have count of all keys");
					Assert.AreEqual(0, keysLeftToRemove.Count, "Expected to have found all keys");

					foreach (TableEnumerator tableEnumerator in level1Enumerators)
					{
						tableEnumerator.TEST_Close();
					}
				}
			}

			Log.Warn($"Done!");
		}
	}
}