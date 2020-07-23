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
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;

namespace MiNET.LevelDB.Benchmarks
{
	[GcServer(true)]
	[SimpleJob(RunStrategy.Throughput), MinIterationCount(200), MaxIterationCount(2_000)]
	public class LevelDbApiBenchmarks
	{
		[Params(100, 1_000, 10_000)] public int SizeOfValues;

		private Database _db;
		private byte[] _key;
		private byte[] _value;

		[GlobalSetup]
		public void GlobalSetup()
		{
			string tempDir = Path.Combine(Path.GetTempPath(), $"LevelDB-{Guid.NewGuid().ToString()}");
			_db = new Database(new DirectoryInfo(tempDir));
			_db.CreateIfMissing = true;
			_db.Open();
		}


		[IterationSetup]
		public void IterationSetup()
		{
			_key = FillArrayWithRandomBytes(1234, 16, 10);
			_value = FillArrayWithRandomBytes(1234, 100, SizeOfValues);
		}

		[GlobalCleanup]
		public void GlobalCleanup()
		{
			_db.Close();
		}

		[Benchmark]
		public void BedrockChunkLoadTest()
		{
			_db.Put(_key, _value);
		}


		private static byte[] FillArrayWithRandomBytes(int seed, int size, int max)
		{
			var bytes = new byte[size];
			var random = new Random(seed);
			for (int i = 0; i < bytes.Length; i++)
			{
				bytes[i] = (byte) random.Next(max);
			}

			return bytes;
		}
	}
}