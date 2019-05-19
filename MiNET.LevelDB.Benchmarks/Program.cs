using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnostics.Windows;
using BenchmarkDotNet.Running;
using Microsoft.Diagnostics.Tracing.Parsers.FrameworkEventSource;

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