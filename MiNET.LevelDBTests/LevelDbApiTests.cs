using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using log4net;
using MiNET.LevelDB;
using NUnit.Framework;

namespace MiNET.LevelDBTests
{
	[TestFixture]
	public class LevelDbApiTests
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(LevelDbApiTests));

		[SetUp]
		public void Init()
		{
			Log.Info($" ************************ RUNNING TEST: {TestContext.CurrentContext.Test.Name} ****************************** ");
		}


		//DirectoryInfo directory = new DirectoryInfo(@"D:\Temp\My World\db\");
		DirectoryInfo directory = new DirectoryInfo(@"TestWorld");

		List<byte[]> testKeys = new List<byte[]>()
		{
			new byte[] {0xf6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x2f, 0x00, },
			new byte[] {0xf7, 0xff, 0xff, 0xff, 0x01, 0x00, 0x00, 0x00, 0x2f, 0x00, },

			new byte[] {0xf7, 0xff, 0xff, 0xff, 0xf1, 0xff, 0xff, 0xff, 0x76, },

			new byte[] {0xf7, 0xff, 0xff, 0xff, 0xfd, 0xff, 0xff, 0xff, 0x2f, 0x05, },

			new byte[] {0xfa, 0xff, 0xff, 0xff, 0xe7, 0xff, 0xff, 0xff, 0x2f, 0x02, },

			new byte[] {0xfa, 0xff, 0xff, 0xff, 0xe7, 0xff, 0xff, 0xff, 0x2f, 0x03, },
		};

		[Test]
		public void LevelDbOpenFromDirectory()
		{
			var db = new Database(directory);
			db.Open();
		}

		[Test]
		public void LevelDbGetValueFromKey()
		{
			var db = new Database(directory);
			db.Open();
			var result = db.Get(testKeys.First());
			// 08 01 08 00 00 40 44 44 14 41 44 00 70 41 44 44  .....@DD.AD.pADD


			Assert.AreEqual(new byte[] {0x08, 0x01, 0x08, 0x00, 0x00}, result.AsSpan(0, 5).ToArray());
		}

		[Test]
		public void LevelDbRepeatedGetValueFromKey()
		{
			var db = new Database(directory);
			db.Open();

			foreach (var testKey in testKeys)
			{
				var result = db.Get(testKey);
				Assert.IsNotNull(result, testKey.HexDump());
			}
		}

		[Test]
		public void LevelDbOpenFromMcpeWorldFile()
		{
			var db = new Database(new DirectoryInfo("My World.mcworld"));
			db.Open();
			var result = db.Get(new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2f, 0x00 });
			// Key=(+0) f7 ff ff ff, f3 ff ff ff, 2f 03 01 ab 5e 00 00 00 00 00
			// Key=(+8) f7 ff ff ff, f4 ff ff ff, 2f 00 01 0c 5d 00 00 00 00 00  
			// Key=(+8) 00 00 00 00, 00 00 00 00, 2f, 00, 01 b1 01 00 00 00 00 00

			Assert.IsNotNull(result);
			Assert.AreEqual(new byte[] { 0x08, 0x01, 0x08, 0x00, 0x11 }, result.AsSpan(0, 5).ToArray());
		}
	}
}