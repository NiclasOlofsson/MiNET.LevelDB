using System.IO;
using System.Reflection;
using log4net;
using log4net.Config;
using NUnit.Framework;

namespace MiNET.LevelDBTests
{
	[SetUpFixture]
	public class SetUpFixture
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(SetUpFixture));

		[OneTimeSetUp]
		public void RunBeforeAnyTests()
		{
			var logRepository = LogManager.GetRepository(Assembly.GetEntryAssembly());
			XmlConfigurator.Configure(logRepository, new FileInfo(Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "log4net.xml")));
			Log.Info(" ------------------------ STARTING TESTS ------------------------ ");
		}

		[OneTimeTearDown]
		public void RunAfterAnyTests()
		{
			// ...
		}
	}
}