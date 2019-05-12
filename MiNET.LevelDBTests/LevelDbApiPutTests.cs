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
	public class LevelDbApiPutTests
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(LevelDbApiPutTests));

		DirectoryInfo directory = new DirectoryInfo(@"TestWorld");

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

		[Test]
		public void LevelDbBasicPut()
		{
			var db = new Database(directory);
			db.Open();

			var value = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
			db.Put(testKeys.First(), new Span<byte>(value));
			db.Close();

			db.Open();
			var result = db.Get(testKeys.First());
			Assert.AreEqual(value, result);
		}
	}
}