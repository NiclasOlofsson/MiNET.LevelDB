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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using log4net;
using MiNET.LevelDB.Utils;
using NUnit.Framework;

namespace MiNET.LevelDB.Tests
{
	[TestFixture]
	public class LevelDbTableTests
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(LevelDbTableTests));

		byte[] _indicatorChars = {0x64, 0x69, 0x6d, 0x65, 0x6e, 0x73, 0x6f, 0x6e, 0x30};

		[Test]
		public void LevelDbReadFindInTableTest()
		{
			FileInfo fileInfo = new FileInfo(@"TestWorld\000050.ldb");
			ResultStatus result;
			using (Table table = new Table(fileInfo))
			{
				result = table.Get(new byte[] {0xf6, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x2f, 0x00,});
			}

			if (result.Data != null)
			{
				if (Log.IsDebugEnabled) Log.Debug("Result:\n" + result.Data.HexDump(cutAfterFive: true));
				return;
			}

			Assert.Fail("Found no entry");
		}

		[Test]
		public void WriteToNewTableTest()
		{
			using var logReader = new LogReader(new FileInfo(@"TestWorld\000047.log"));
			var memCache = new MemCache();
			memCache.Load(logReader);

			var newFileInfo = new FileInfo(Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".ldb"));
			using FileStream stream = File.Create(newFileInfo.FullName);
			var creator = new TableCreator(stream);

			foreach (KeyValuePair<byte[], MemCache.ResultCacheEntry> entry in memCache._resultCache.OrderBy(kvp => kvp.Key, new BytewiseComparator()))
			{
				if (entry.Value.ResultState != ResultState.Exist) continue;

				byte[] key = entry.Key;
				byte[] data = entry.Value.Data;

				Log.Debug($"Key:{key.ToHexString()} {entry.Value.Sequence}");
				//Key:e3 ff ff ff f9 ff ff ff 31 

				byte[] opAndSeq = BitConverter.GetBytes((ulong) entry.Value.Sequence);
				opAndSeq[0] = 1;
				creator.Add(key.Concat(opAndSeq).ToArray(), data);
			}
			creator.Finish();
			stream.Close();
			Log.Debug($"Wrote {memCache._resultCache.Count} values");

			//Key:fa 40 ab 14 4d 96 ec 7b 62 38 f7 63
			//Key:fe ff ff ff f1 ff ff ff 76 

			var table = new Table(newFileInfo);
			ResultStatus result = table.Get(new byte[] {0xfe, 0xff, 0xff, 0xff, 0xf1, 0xff, 0xff, 0xff, 0x76});
			Assert.AreEqual(ResultState.Exist, result.State);

			result = table.Get(new byte[] {0xfa, 0x40, 0xab, 0x14, 0x4d, 0x96, 0xec, 0x7b, 0x62, 0x38, 0xf7, 0x63});
			Assert.AreEqual(ResultState.Exist, result.State);

			foreach (KeyValuePair<byte[], MemCache.ResultCacheEntry> entry in memCache._resultCache.OrderBy(kvp => kvp.Key, new BytewiseComparator()))
			{
				if (entry.Value.ResultState != ResultState.Exist) continue;

				byte[] key = entry.Key;
				byte[] data = entry.Value.Data;

				result = table.Get(key);
				Assert.AreEqual(ResultState.Exist, result.State);
				Assert.AreEqual(data, result.Data.ToArray());
			}
		}
	}
}