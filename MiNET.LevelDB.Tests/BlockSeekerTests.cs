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
using MiNET.LevelDB.Utils;
using NUnit.Framework;

namespace MiNET.LevelDB.Tests
{
	[TestFixture]
	public class BlockSeekerTests
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(BlockSeekerTests));

		public DirectoryInfo GetTestDirectory(bool copy = true)
		{
			var directory = new DirectoryInfo(@"TestWorld");
			string tempDir = Path.Combine(Path.GetTempPath(), $"LevelDB-{Guid.NewGuid().ToString()}");
			Directory.CreateDirectory(tempDir);

			if (copy)
			{
				FileInfo[] files = directory.GetFiles();
				foreach (var file in files)
				{
					string newPath = Path.Combine(tempDir, file.Name);
					file.CopyTo(newPath);
				}
			}

			return new DirectoryInfo(tempDir);
		}

		[Test]
		public void ReadAllKeysTest()
		{
			var fileInfo = new FileInfo(Path.Combine(GetTestDirectory().FullName, "000050.ldb"));
			using var table = new Table(fileInfo);

			// Just initialize the block first.
			table.Get(new byte[] {0x00});

			int count = 0;
			foreach (BlockEntry blockEntry in table)
			{
				Log.Debug($"Current Key:{blockEntry.Key.ToHexString()}");
				Assert.AreNotEqual(0, blockEntry.Key.Length);
				count++;
			}

			Assert.AreEqual(5322, count);
		}
	}
}