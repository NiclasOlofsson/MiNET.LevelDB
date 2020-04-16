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
			byte[] value;
			byte[] result;
			using (var db = new Database(directory))
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
			using (var db = new Database(directory))
			{
				db.Open();

				Random random = new Random();
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

		private byte[] FillArrayWithRandomBytes(int size)
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