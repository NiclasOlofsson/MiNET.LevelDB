using System.IO;
using System.Reflection;
using log4net;
using log4net.Config;
using NUnit.Framework;

namespace MiNET.LevelDB.Tests
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

			Log.Info("Setting database to paranoid mode for testing");
			Database.ParanoidMode = true;
		}

		[OneTimeTearDown]
		public void RunAfterAnyTests()
		{
			// ...
		}
	}
}