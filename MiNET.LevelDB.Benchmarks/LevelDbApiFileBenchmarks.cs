using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using BenchmarkDotNet.Attributes;
using log4net;
using log4net.Core;
using log4net.Repository.Hierarchy;

namespace MiNET.LevelDB.Benchmarks
{
	[MemoryDiagnoser]
	[GcServer(true)]
	public class LevelDbApiFileBenchmarks
	{
		[GlobalSetup]
		public void GlobalSetup()
		{
			Hierarchy hierarchy = (Hierarchy) LogManager.GetRepository(Assembly.GetEntryAssembly());
			hierarchy.Root.Level = Level.Error;

			_chunks = GenerateChunks(new ChunkCoordinates(0, 0), 8).OrderBy(kvp => kvp.Value).ToArray();
		}

		[GlobalCleanup]
		public void GlobalCleanup()
		{
		}


		private KeyValuePair<ChunkCoordinates, double>[] _chunks;

		[Params(100, 1_000)] public int NumberOfChunks = 0;

		[Benchmark]
		public void BedrockChunkLoadTest()
		{
			int count = 0;
			while (count < NumberOfChunks)
			{
				using (var db = new Database(new DirectoryInfo("My World.mcworld")))
				{
					db.Open();

					foreach (var pair in _chunks)
					{
						if (count >= NumberOfChunks) break; // ABORT!

						var coordinates = pair.Key;
						var index = BitConverter.GetBytes(coordinates.X).Concat(BitConverter.GetBytes(coordinates.Z)).ToArray();

						var version = db.Get(index.Concat(new byte[] {0x76}).ToArray());

						for (byte y = 0; y < 16; y++)
						{
							var chunk = db.Get(index.Concat(new byte[] {0x2f, y}).ToArray());

							if (y == 0)
							{
								if (chunk != null) count++;
							}

							if (chunk == null) break;
						}

						var flatDataBytes = db.Get(index.Concat(new byte[] {0x2D}).ToArray());
						var blockEntityBytes = db.Get(index.Concat(new byte[] {0x31}).ToArray());
					}
				}
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
					var distance = (x * x) + (z * z);
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
}