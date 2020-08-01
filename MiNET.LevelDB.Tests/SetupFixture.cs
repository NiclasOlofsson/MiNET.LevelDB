using System;
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

	public static class TestUtils
	{
		public static DirectoryInfo GetTestDirectory(bool copy = true)
		{
			var directory = new DirectoryInfo(@"TestWorld");
			string tempDir = Path.Combine(Path.GetTempPath(), $"LevelDB-{Guid.NewGuid().ToString()}");
			Directory.CreateDirectory(tempDir);

			if (copy)
			{
				FileInfo[] files = directory.GetFiles();
				foreach (var file in files)
				{
					string newPath = Path.Combine(tempDir, file.Name);
					file.CopyTo(newPath);
				}
			}

			return new DirectoryInfo(tempDir);
		}

	}

}