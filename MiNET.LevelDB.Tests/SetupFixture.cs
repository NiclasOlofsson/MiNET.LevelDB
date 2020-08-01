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

			DeleteTmpDirs();
		}

		private void DeleteTmpDirs()
		{
			var directories = Directory.GetDirectories(Path.GetTempPath(), "LevelDB-*");
			foreach (string directory in directories)
			{
				Log.Debug($"Delete {directory}");
				Directory.Delete(directory, true);
			}

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

		public static byte[] FillArrayWithRandomBytes(int size)
		{
			var bytes = new byte[size];
			var random = new Random();
			for (int i = 0; i < bytes.Length; i++)
			{
				bytes[i] = (byte) random.Next(255);
			}

			return bytes;
		}
	}
}