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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using log4net;
using log4net.Config;
using log4net.Core;
using log4net.Repository.Hierarchy;

namespace MiNET.LevelDB.Console
{
	class Program
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(Program));

		static void Main(string[] args)
		{
			var hierarchy = (Hierarchy) LogManager.GetRepository(Assembly.GetEntryAssembly());
			XmlConfigurator.Configure(hierarchy, new FileInfo(Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log4net.xml")));
			hierarchy.Root.Level = Level.Info;

			var program = new Program();
			program.GlobalSetup();
			System.Console.WriteLine("Start");
			program.BedrockChunkLoadTest();
			System.Console.WriteLine("Start");
		}

		public void GlobalSetup()
		{
		}

		public void BedrockChunkLoadTest()
		{
			var chunks = GenerateChunks(new ChunkCoordinates(0, 0), 18).OrderBy(kvp => kvp.Value).ToArray();
			int count = 0;
			ulong totalSize = 0;

			using var db = new Database(new DirectoryInfo("benchmark.mcworld"));
			db.Open();

			var sw = Stopwatch.StartNew();

			List<Task> tasks = new List<Task>();
			foreach (var pair in chunks)
			{
				void GetChunk(ChunkCoordinates coordinates)
				{
					byte[] index = Combine(BitConverter.GetBytes(coordinates.X), BitConverter.GetBytes(coordinates.Z));

					byte[] version = db.Get(Combine(index, 0x76));

					byte[] chunkDataKey = Combine(index, new byte[] {0x2f, 0});
					for (byte y = 0; y < 16; y++)
					{
						chunkDataKey[^1] = y;
						byte[] chunk = db.Get(chunkDataKey);

						if (y == 0)
						{
							if (chunk != null)
							{
								count++;
								//Log.Debug($"Found chunk at coord={coordinates}");
							}
							else
							{
								throw new Exception("All chunks exist. Should not fail. This is a bug in Table.FindBlockHandleInBlockIndex()");
							}
						}

						if (chunk == null) break;

						totalSize += (ulong) chunk.Length;
					}

					byte[] flatDataBytes = db.Get(Combine(index, 0x2D));
					if (flatDataBytes != null) totalSize += (ulong) flatDataBytes.Length;
					byte[] blockEntityBytes = db.Get(Combine(index, 0x31));
					if (blockEntityBytes != null) totalSize += (ulong) blockEntityBytes.Length;
				}

				GetChunk(pair.Key);
				//tasks.Add(Task.Run(() => GetChunk(pair.Key)));
			}
			Task.WaitAll(tasks.ToArray());

			long time = sw.ElapsedMilliseconds;
			Log.Info($"Fetch {count} chunk columns in {time}ms");
			System.Console.WriteLine($"Fetch {count} chunk columns in {time}ms. Total size={totalSize / 1000000}MB.");
			Log.Info($"Fetch {count} chunk columns in {time}ms. Total size={totalSize / 1000000}MB.");
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private static byte[] Combine(byte[] first, byte[] second)
		{
			var ret = new byte[first.Length + second.Length];
			Buffer.BlockCopy(first, 0, ret, 0, first.Length);
			Buffer.BlockCopy(second, 0, ret, first.Length, second.Length);
			return ret;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private static byte[] Combine(byte[] first, byte b)
		{
			var ret = new byte[first.Length + 1];
			Buffer.BlockCopy(first, 0, ret, 0, first.Length);
			ret[^1] = b;
			return ret;
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