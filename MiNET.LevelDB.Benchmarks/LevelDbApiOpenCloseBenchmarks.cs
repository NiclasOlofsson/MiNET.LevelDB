using System.IO;
using System.Reflection;
using BenchmarkDotNet.Attributes;
using log4net;
using log4net.Core;
using log4net.Repository.Hierarchy;

namespace MiNET.LevelDB.Benchmarks
{
	[MemoryDiagnoser]
	[GcServer(true)]
	public class LevelDbApiOpenCloseBenchmarks
	{
		[GlobalSetup]
		public void GlobalSetup()
		{
			Hierarchy hierarchy = (Hierarchy) LogManager.GetRepository(Assembly.GetEntryAssembly());
			hierarchy.Root.Level = Level.Error;
		}

		[GlobalCleanup]
		public void GlobalCleanup()
		{
		}


		[Benchmark]
		public void BedrockChunkLoadTest()
		{
			using (var db = new Database(new DirectoryInfo("My World.mcworld")))
			{
				db.Open();
			}
		}
	}
}