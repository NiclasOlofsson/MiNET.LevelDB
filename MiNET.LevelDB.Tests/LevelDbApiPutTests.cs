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
using log4net;
using NUnit.Framework;

namespace MiNET.LevelDB.Tests
{
	[TestFixture]
	public class LevelDbApiPutTests
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(LevelDbApiPutTests));

		List<byte[]> testKeys = new List<byte[]>()
		{
			new byte[] {0xf6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x2f, 0x00,},
			new byte[] {0xf7, 0xff, 0xff, 0xff, 0x01, 0x00, 0x00, 0x00, 0x2f, 0x00,},
			new byte[] {0xf7, 0xff, 0xff, 0xff, 0xf1, 0xff, 0xff, 0xff, 0x76,},
			new byte[] {0xf7, 0xff, 0xff, 0xff, 0xfd, 0xff, 0xff, 0xff, 0x2f, 0x05,},
			new byte[] {0xfa, 0xff, 0xff, 0xff, 0xe7, 0xff, 0xff, 0xff, 0x2f, 0x02,},
			new byte[] {0xfa, 0xff, 0xff, 0xff, 0xe7, 0xff, 0xff, 0xff, 0x2f, 0x03,},
		};

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

		[Test]
		public void LevelDbBasicPut()
		{
			byte[] value;
			byte[] result;
			using (var db = new Database(GetTestDirectory()))
			{
				db.Open();

				value = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
				db.Put(testKeys.First(), new Span<byte>(value));
				db.Put(testKeys.First(), new Span<byte>(value));
				db.Put(testKeys.First(), new Span<byte>(value));
				db.Put(testKeys.First(), new Span<byte>(value));
				db.Put(testKeys.First(), new Span<byte>(value));
				db.Put(testKeys.First(), new Span<byte>(value));
				db.Put(testKeys.First(), new Span<byte>(value));
				db.Put(testKeys.First(), new Span<byte>(value));
				db.Put(testKeys.First(), new Span<byte>(value));
				db.Close();

				db.Open();
				result = db.Get(testKeys.First());
			}
			Assert.AreEqual(value, result);
		}

		[Test]
		public void LevelDbMultiPut()
		{
			using (var db = new Database(GetTestDirectory()))
			{
				db.Open();

				var random = new Random();
				for (int i = 0; i < 5_000; i++)
				{
					byte[] key = FillArrayWithRandomBytes(random.Next(10, 16));
					byte[] data = FillArrayWithRandomBytes(random.Next(100, 600)); // 32KB is maz size for a block, not that it matters for this
					db.Put(key, data);
				}

				db.Close();

				db.Open();
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