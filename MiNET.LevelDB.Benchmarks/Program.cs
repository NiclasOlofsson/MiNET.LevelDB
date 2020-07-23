using System.Reflection;
using BenchmarkDotNet.Running;
using log4net;
using log4net.Core;
using log4net.Repository.Hierarchy;

namespace MiNET.LevelDB.Benchmarks
{
	class Program
	{
		static void Main(string[] args)
		{
			var hierarchy = (Hierarchy) LogManager.GetRepository(Assembly.GetEntryAssembly());
			hierarchy.Root.Level = Level.Error;

			var summary = BenchmarkRunner.Run<LevelDbApiBenchmarks>();
			//var summary = BenchmarkRunner.Run<LevelDbApiOpenCloseBenchmarks>();
			//var summary = BenchmarkRunner.Run<LevelDbApiFileBenchmarks>();
			//var summary = BenchmarkRunner.Run<LevelDbApiFileBenchmarks>(DefaultConfig.Instance.With(new EtwProfiler(new EtwProfilerConfig())));
		}
	}
}