
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using log4net;
using log4net.Core;
using log4net.Repository.Hierarchy;
using MiNET.LevelDB.Utils;
using NUnit.Framework;

namespace MiNET.LevelDB.Tests
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
			using (var db = new Database(directory))
			{
				db.Open();
			}
		}

		[Test]
		public void LevelDbCreateFromDirectory()
		{
			DirectoryInfo tempDir = new DirectoryInfo(Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString()));

			using (var db = new Database(tempDir))
			{
				db.CreateIfMissing = true;
				db.Open();
			}
			//var result = db.Get(testKeys.Last());
		}

		[Test]
		public void LevelDbGetValueFromKey()
		{
			byte[] result;
			using (var db = new Database(directory))
			{
				db.Open();
				result = db.Get(testKeys.Last());
			}
			// 08 01 08 00 00 40 44 44 14 41 44 00 70 41 44 44  .....@DD.AD.pADD


			Assert.AreEqual(new byte[] {0x08, 0x01, 0x02, 0x00, 0x80}, result.AsSpan(0, 5).ToArray());
		}

		[Test]
		public void LevelDbRepeatedGetValues()
		{
			Stopwatch sw;
			using (var db = new Database(directory))
			{
				db.Open();

				sw = new Stopwatch();
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
			byte[] result;
			using (var db = new Database(directory))
			{
				db.Open();

				result = db.Get(testKey);
			}
			Assert.IsNotNull(result, testKey.HexDump());
		}

		[Test]
		public void LevelDbOpenFromMcpeWorldFile()
		{
			byte[] result;
			using (var db = new Database(new DirectoryInfo("My World.mcworld")))
			{
				db.Open();
				result = db.Get(new byte[] {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2f, 0x00});
			}
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
			int count = 0;
			int countMissed = 0;
			ulong totalSize = 0;
			int numberOfChunks = 10000;
			var chunks = GenerateChunks(new ChunkCoordinates(0, 0), 8).OrderBy(kvp => kvp.Value).ToArray();

			using (var db = new Database(new DirectoryInfo("My World.mcworld")))
			{
				db.Open();

				//Hierarchy hierarchy = (Hierarchy)LogManager.GetRepository(Assembly.GetEntryAssembly());
				//hierarchy.Root.Level = Level.Info;

				Assert.IsTrue(BitConverter.IsLittleEndian);

				Stopwatch sw = new Stopwatch();
				sw.Restart();

				while (count < numberOfChunks)
				{
					foreach (var pair in chunks)
					{
						if (count >= numberOfChunks) break;

						var coordinates = pair.Key;

						var index = BitConverter.GetBytes(coordinates.X).Concat(BitConverter.GetBytes(coordinates.Z)).ToArray();
						var version = db.Get(index.Concat(new byte[] {0x76}).ToArray());

						for (byte y = 0; y < 16; y++)
						{
							var chunk = db.Get(index.Concat(new byte[] {0x2f, y}).ToArray());

							if (y == 0)
							{
								if (chunk != null)
								{
									count++;
									//Log.Debug($"Found chunk at coord={coordinates}");
								}
								else
								{
									countMissed++;
									//Log.Debug($"Missing chunk at coord={coordinates}");
								}
							}

							if (chunk == null) break;

							totalSize += (ulong) chunk.Length;
						}

						var flatDataBytes = db.Get(index.Concat(new byte[] {0x2D}).ToArray());
						if (flatDataBytes != null)
							totalSize += (ulong) flatDataBytes.Length;
						var blockEntityBytes = db.Get(index.Concat(new byte[] {0x31}).ToArray());
						if (blockEntityBytes != null)
							totalSize += (ulong) blockEntityBytes.Length;
					}
				}

				var time = sw.ElapsedMilliseconds;
				Log.Info($"Fetch {count} chunk columns in {time}ms");
				Console.WriteLine($"Fetch {count} chunk columns in {time}ms. Total size={totalSize/1000000}MB. Missing={countMissed}");
			}
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
}