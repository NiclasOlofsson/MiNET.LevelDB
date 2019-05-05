using System;
using System.Collections.Generic;
using System.Diagnostics;
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
			new byte[] {0xf6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x2f, 0x00,},
			new byte[] {0xf7, 0xff, 0xff, 0xff, 0x01, 0x00, 0x00, 0x00, 0x2f, 0x00,},

			new byte[] {0xf7, 0xff, 0xff, 0xff, 0xf1, 0xff, 0xff, 0xff, 0x76,},

			new byte[] {0xf7, 0xff, 0xff, 0xff, 0xfd, 0xff, 0xff, 0xff, 0x2f, 0x05,},

			new byte[] {0xfa, 0xff, 0xff, 0xff, 0xe7, 0xff, 0xff, 0xff, 0x2f, 0x02,},

			new byte[] {0xfa, 0xff, 0xff, 0xff, 0xe7, 0xff, 0xff, 0xff, 0x2f, 0x03,},
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
		public void LevelDbRepeatedGetValues()
		{
			var db = new Database(directory);
			db.Open();

			Stopwatch sw = new Stopwatch();
			sw.Restart();
			for (int i = 0; i < 100*16/6; i++)
			{
				foreach (var testKey in testKeys)
				{
					var result = db.Get(testKey);
					Assert.IsNotNull(result);
					//Assert.IsNotNull(result, result != null ? "" : testKey.HexDump());
				}
			}
			var time = sw.ElapsedMilliseconds;
			Log.Info($"time={time}");
		}

		[TestCase(new byte[] {0xf6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x2f, 0x00,})]
		[TestCase(new byte[] {0xf7, 0xff, 0xff, 0xff, 0x01, 0x00, 0x00, 0x00, 0x2f, 0x00,})]
		[TestCase(new byte[] {0xf7, 0xff, 0xff, 0xff, 0xf1, 0xff, 0xff, 0xff, 0x76,})]
		[TestCase(new byte[] {0xf7, 0xff, 0xff, 0xff, 0xfd, 0xff, 0xff, 0xff, 0x2f, 0x05,})]
		[TestCase(new byte[] {0xfa, 0xff, 0xff, 0xff, 0xe7, 0xff, 0xff, 0xff, 0x2f, 0x02,})]
		[TestCase(new byte[] {0xfa, 0xff, 0xff, 0xff, 0xe7, 0xff, 0xff, 0xff, 0x2f, 0x03,})]
		public void LevelDbRepeatedGetValueFromKey(byte[] testKey)
		{
			// fa ff ff ff e7 ff ff ff 2f 03
			var db = new Database(directory);
			db.Open();

			var result = db.Get(testKey);
			Assert.IsNotNull(result, testKey.HexDump());
		}

		[Test]
		public void LevelDbOpenFromMcpeWorldFile()
		{
			var db = new Database(new DirectoryInfo("My World.mcworld"));
			db.Open();
			var result = db.Get(new byte[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2f, 0x00});
			// Key=(+0) f7 ff ff ff, f3 ff ff ff, 2f 03 01 ab 5e 00 00 00 00 00
			// Key=(+8) f7 ff ff ff, f4 ff ff ff, 2f 00 01 0c 5d 00 00 00 00 00  
			// Key=(+8) 00 00 00 00, 00 00 00 00, 2f, 00, 01 b1 01 00 00 00 00 00

			Assert.IsNotNull(result);
			Assert.AreEqual(new byte[] {0x08, 0x01, 0x08, 0x00, 0x11}, result.AsSpan(0, 5).ToArray());
		}

		//[Test, Ignore("")]
		[Test]
		public void BedrockChunkLoadTest()
		{
			var db = new Database(new DirectoryInfo("My World.mcworld"));
			db.Open();

			//{
			//	var key = BitConverter.GetBytes(16).Concat(BitConverter.GetBytes(12)).Concat(new byte[] { 0x2f, 0 }).ToArray();
			//	var chunk = db.Get(key);
			//	Assert.IsNull(chunk);
			//	return;
			//}

			//Hierarchy hierarchy = (Hierarchy) LogManager.GetRepository(Assembly.GetEntryAssembly());
			//hierarchy.Root.Level = Level.Info;

			var chunks = GenerateChunks(new ChunkCoordinates(0, 0), 9);

			Assert.IsTrue(BitConverter.IsLittleEndian);

			Stopwatch sw = new Stopwatch();
			sw.Restart();

			int count = 0;
			foreach (var pair in chunks.OrderBy(kvp => kvp.Value))
			{
				var coordinates = pair.Key;
				for (byte y = 0; y < 16; y++)
				{
					var key = BitConverter.GetBytes(coordinates.X).Concat(BitConverter.GetBytes(coordinates.Z)).Concat(new byte[] {0x2f, y}).ToArray();
					var chunk = db.Get(key);

					if (y == 0)
					{
						if (chunk == null)
						{
							Log.Debug($"Missing chunk at coord={coordinates}");
						}
						else
						{
							count++;
							Log.Debug($"Found chunk at coord={coordinates}");
						}
					}

					if (chunk == null) break;
				}
			}

			var time = sw.ElapsedMilliseconds;
			Log.Info($"time={time}");

			{
				var key = BitConverter.GetBytes(32300009).Concat(BitConverter.GetBytes(10000456)).Concat(new byte[] {0x2f, 0}).ToArray();
				var chunk = db.Get(key);
				Assert.IsNull(chunk);
			}

			Assert.AreEqual(chunks.Count, count);
			Assert.AreEqual(253, count);
		}

		public Dictionary<ChunkCoordinates, double> GenerateChunks(ChunkCoordinates chunkPosition, double radius)
		{
			Dictionary<ChunkCoordinates, double> newOrders = new Dictionary<ChunkCoordinates, double>();

			double radiusSquared = Math.Pow(radius, 2);

			int centerX = chunkPosition.X;
			int centerZ = chunkPosition.Z;

			for (double x = -radius; x <= radius; ++x)
			{
				for (double z = -radius; z <= radius; ++z)
				{
					var distance = (x*x) + (z*z);
					if (distance > radiusSquared)
					{
						continue;
					}
					int chunkX = (int) (x + centerX);
					int chunkZ = (int) (z + centerZ);
					var index = new ChunkCoordinates(chunkX, chunkZ);
					newOrders[index] = distance;
				}
			}

			return newOrders;
		}
	}

	public class ChunkCoordinates
	{
		public ChunkCoordinates(int x, int z)
		{
			X = x;
			Z = z;
		}

		public int X { get; set; }
		public int Z { get; set; }

		public override string ToString()
		{
			return $"{nameof(X)}: {X}, {nameof(Z)}: {Z}";
		}
	}

	public class McpeWrapper
	{
	}
}