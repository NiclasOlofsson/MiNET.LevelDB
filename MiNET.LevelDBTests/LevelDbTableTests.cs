using System.IO;
using log4net;
using MiNET.LevelDB;
using MiNET.LevelDB.Utils;
using NUnit.Framework;

namespace MiNET.LevelDBTests
{
	[TestFixture]
	public class LevelDbTableTests
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(LevelDbTableTests));

		byte[] _indicatorChars =
		{
			0x64, 0x69, 0x6d, 0x65,
			0x6e, 0x73, 0x6f,
			0x6e, 0x30
		};

		[Test]
		public void LevelDbReadFindInTableTest()
		{
			FileInfo fileInfo = new FileInfo(@"TestWorld\000050.ldb");
			TableReader table = new TableReader(fileInfo);

			var result = table.Get(new byte[] {0xf6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x2f, 0x00,});

			if (result.Data != null)
			{
				if (Log.IsDebugEnabled) Log.Debug("Result:\n" + result.Data.HexDump(cutAfterFive: true));
				return;
			}

			Assert.Fail("Found no entry");
		}
	}
}