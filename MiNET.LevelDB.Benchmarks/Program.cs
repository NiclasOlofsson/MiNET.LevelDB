using BenchmarkDotNet.Running;

namespace MiNET.LevelDB.Benchmarks
{
	class Program
	{
		static void Main(string[] args)
		{
			//var summary = BenchmarkRunner.Run<LevelDbApiOpenCloseBenchmarks>();
			var summary = BenchmarkRunner.Run<LevelDbApiBenchmarks>();
			//var summary = BenchmarkRunner.Run<LevelDbApiFileBenchmarks>();
			//var summary = BenchmarkRunner.Run<LevelDbApiFileBenchmarks>(DefaultConfig.Instance.With(new EtwProfiler(new EtwProfilerConfig())));
		}
	}
}