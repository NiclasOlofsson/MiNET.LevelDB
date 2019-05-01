using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using log4net;

namespace MiNET.LevelDB.Console
{
	class Program
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(Program));


		static void Main(string[] args)
		{
			new Program().BedrockChunkLoadTest();
		}

		public void BedrockChunkLoadTest()
		{
			System.Console.WriteLine($"Running tests...");

			var db = new Database(new DirectoryInfo("My World.mcworld"));
			db.Open();

			Stopwatch sw = new Stopwatch();
			sw.Restart();

			var chunks = GenerateChunks(new ChunkCoordinates(0, 0), new Dictionary<ChunkCoordinates, McpeWrapper>(), 20);

			Debug.Assert(1257 == chunks.Count);

			int count = 0;
			foreach (var coordinates in chunks.Keys)
			{
				for (byte y = 0; y < 16; y++)
				{
					var key = BitConverter.GetBytes(coordinates.X).Concat(BitConverter.GetBytes(coordinates.Z)).Concat(new byte[] {0x2f, y}).ToArray();
					var chunk = db.Get(key);

					//if (y == 0) Assert.IsNotNull(chunk, $"Coord={coordinates}");
					if (y == 0)
					{
						if (chunk == null)
						{
							Log.Error($"Missing chunk at coord={coordinates}");
							System.Console.Write("!");
						}
						else
						{
							Log.Warn($"Found chunk at coord={coordinates}");
						}
					}

					if (chunk == null) break;

					System.Console.Write(".");
					count++;
				}
				System.Console.WriteLine();
			}

			Debug.Assert(9279 == count);

			var time = sw.ElapsedMilliseconds;
			Log.Info($"time={time}");
			System.Console.WriteLine($"time={time}");
		}

		public Dictionary<ChunkCoordinates, double> GenerateChunks(ChunkCoordinates chunkPosition, Dictionary<ChunkCoordinates, McpeWrapper> chunksUsed, double radius)
		{
			lock (chunksUsed)
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

				foreach (var chunkKey in chunksUsed.Keys.ToArray())
				{
					if (!newOrders.ContainsKey(chunkKey))
					{
						chunksUsed.Remove(chunkKey);
					}
				}

				return newOrders;
			}
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