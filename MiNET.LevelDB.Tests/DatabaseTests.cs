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
using NUnit.Framework;

namespace MiNET.LevelDB.Tests
{
	[TestFixture()]
	public class DatabaseTests
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(DatabaseTests));

		[SetUp]
		public void Init()
		{
			Log.Info($" ************************ RUNNING TEST: {TestContext.CurrentContext.Test.Name} ****************************** ");
		}

		public DirectoryInfo GetTestDirectory()
		{
			var directory = new DirectoryInfo(@"TestWorld");
			string tempDir = Path.Combine(Path.GetTempPath(), $"LevelDB-{Guid.NewGuid().ToString()}");
			Directory.CreateDirectory(tempDir);

			FileInfo[] files = directory.GetFiles();
			foreach (var file in files)
			{
				string newPath = Path.Combine(tempDir, file.Name);
				file.CopyTo(newPath);
			}

			return new DirectoryInfo(tempDir);
		}


		[Test()]
		public void MakeSurePutWorksTest_ok()
		{
			var tempDir = new DirectoryInfo(Path.Combine(Path.GetTempPath(), $"LevelDB-{Guid.NewGuid()}"));
			Log.Debug($"Test directory:\n{tempDir.FullName}");

			var options = new Options {MaxMemCacheSize = 1500L};

			byte[] key = FillArrayWithRandomBytes(10);

			using (var db = new Database(tempDir, true, options))
			{
				db.Open();

				Assert.True(File.Exists(Path.Combine(tempDir.FullName, "CURRENT")), "Missing CURRENT");
				Assert.True(File.Exists(Path.Combine(tempDir.FullName, "MANIFEST-000001")), "Missing new manifest");
				Assert.False(File.Exists(Path.Combine(tempDir.FullName, "000001.log")), "Didn't expect to have log file yet");

				db.Put(key, FillArrayWithRandomBytes(2000));
				Assert.True(File.Exists(Path.Combine(tempDir.FullName, "000001.log")), "Missing log");
				Assert.IsFalse(File.Exists(Path.Combine(tempDir.FullName, "000002.log")), "Didn't expect a new log file");

				db.Put(FillArrayWithRandomBytes(10), FillArrayWithRandomBytes(1000));
				Assert.True(File.Exists(Path.Combine(tempDir.FullName, "000002.log")), "Missing log");

				byte[] result = db.Get(key);
				Assert.IsNotNull(result);

				db.Close();
			}

			// Verify that we written the necessary files to the db directory
			// 000001.log
			Assert.True(File.Exists(Path.Combine(tempDir.FullName, "000002.log")), "Missing log");
			// CURRENT 
			Assert.True(File.Exists(Path.Combine(tempDir.FullName, "CURRENT")), "Missing CURRENT");
			// MANIFEST-000001
			Assert.True(File.Exists(Path.Combine(tempDir.FullName, "MANIFEST-000004")), "Missing manifest");

			// Later, we also need verify table files.
			// however, not yet implemented conversion from log -> table

			tempDir.Refresh();

			using (var db = new Database(tempDir, false, options))
			{
				db.Open();

				Assert.False(File.Exists(Path.Combine(tempDir.FullName, "000001.log")), "Expected log to have been deleted");
				Assert.True(File.Exists(Path.Combine(tempDir.FullName, "000003.ldb")), "Missing level 0 table file");
				Assert.False(File.Exists(Path.Combine(tempDir.FullName, "MANIFEST-000001")), "Should have removed old manifest");
				Assert.True(File.Exists(Path.Combine(tempDir.FullName, "MANIFEST-000004")), "Missing new manifest");

				byte[] result = db.Get(key);
				Assert.IsNotNull(result);

				//db.Put(key, data);
				//Assert.True(File.Exists(Path.Combine(tempDir.FullName, "000002.log")), "Missing log");

				db.Close();
			}
		}

		public static byte[] FillArrayWithRandomBytes(int size)
		{
			var bytes = new byte[size];
			var random = new Random();
			for (int i = 0; i < bytes.Length; i++)
			{
				bytes[i] = (byte) random.Next(255);
			}

			return bytes;
		}
	}
}