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
using log4net;
using MiNET.LevelDB.Utils;
using NUnit.Framework;

namespace MiNET.LevelDB.Tests
{
	// https://github.com/basho/leveldb/wiki/mv-overview

	[TestFixture]
	public class LevelDbLogTests
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(LevelDbLogTests));

		[SetUp]
		public void Init()
		{
			Log.Info($" ************************ RUNNING TEST: {TestContext.CurrentContext.Test.Name} ****************************** ");
		}

		[Test]
		public void LevelDbSearchManifestTest()
		{
			DirectoryInfo directory = TestUtils.GetTestDirectory();

			var currentStream = File.OpenText(Path.Combine(directory.FullName, "CURRENT"));
			string manifestFilename = currentStream.ReadLine();
			currentStream.Close();

			Log.Debug($"Reading manifest from {manifestFilename}");

			// 08 01 02 00 00 01 00 00 00 00 00 00 00 00 00 00  ................

			ResultStatus result;
			using (Manifest manifest = new Manifest(directory))
			{
				using (var reader = new LogReader(new FileInfo(Path.Combine(directory.FullName, manifestFilename))))
				{
					manifest.Load(reader);
				}
				result = manifest.Get(new byte[] {0xf7, 0xff, 0xff, 0xff, 0xfd, 0xff, 0xff, 0xff, 0x2f, 0x05,});
			}
			Assert.AreEqual(new byte[] {0x08, 0x01, 0x02, 0x0, 0x0}, result.Data.Slice(0, 5).ToArray());
		}

		[Test]
		public void LevelDbSearchLogTest()
		{
			// https://github.com/google/leveldb/blob/master/doc/log_format.md

			DirectoryInfo directory = TestUtils.GetTestDirectory();

			LogReader logReader = new LogReader(new FileInfo(Path.Combine(directory.FullName, "000047.log")));
			logReader.Open();
			MemCache memCache = new MemCache();
			memCache.Load(logReader);

			var result = memCache.Get(new byte[] {0xeb, 0xff, 0xff, 0xff, 0xf3, 0xff, 0xff, 0xff, 0x31});

			Assert.IsTrue(ReadOnlySpan<byte>.Empty != result.Data);
			Assert.AreEqual(new byte[] {0xA, 0x00, 0x00, 0x02, 0x05}, result.Data.Slice(0, 5).ToArray());
		}

		[Test]
		public void LevelDbReadLogTest()
		{
			// https://github.com/google/leveldb/blob/master/doc/log_format.md

			DirectoryInfo directory = TestUtils.GetTestDirectory();

			LogReader logReader = new LogReader(new FileInfo(Path.Combine(directory.FullName, "000047.log")));

			BytewiseComparator comparator = new BytewiseComparator();

			bool found = false;

			while (true)
			{
				ReadOnlySpan<byte> data = logReader.ReadData();

				if (logReader.Eof) break;

				var dataReader = new SpanReader(data);

				long sequenceNumber = dataReader.ReadInt64();
				long size = dataReader.ReadInt32();

				while (!dataReader.Eof)
				{
					byte recType = dataReader.ReadByte();

					ulong v1 = dataReader.ReadVarLong();
					var currentKey = dataReader.Read(v1);

					//CurrentKey = f5 ff ff ff eb ff ff ff 36

					if (comparator.Compare(new byte[] {0xf5, 0xff, 0xff, 0xff, 0xeb, 0xff, 0xff, 0xff, 0x36}, currentKey) == 0)
					{
						Assert.False(found);
						found = true;
					}

					ulong v2 = 0;
					ReadOnlySpan<byte> currentVal = ReadOnlySpan<byte>.Empty;
					switch (recType)
					{
						case 1: // value
						{
							if (recType == 1)
							{
								v2 = dataReader.ReadVarLong();
								currentVal = dataReader.Read(v2);
							}
							break;
						}
						case 0: // delete
						{
							//Assert.Fail("Unexpected delete key");
							break;
						}
						default:
							throw new Exception("Unknown record format");
					}

					if (Log.IsDebugEnabled) Log.Debug($"RecType={recType}, Sequence={sequenceNumber}, Size={size}, v1={v1}, v2={v2}\nCurrentKey={currentKey.HexDump(currentKey.Length, false, false)}\nCurrentVal=\n{currentVal.HexDump(cutAfterFive: true)} ");
				}
			}

			Assert.True(found);
		}

		[Test]
		public void LevelDbWriteLogTest()
		{
			Version version;
			{
				DirectoryInfo directory = TestUtils.GetTestDirectory();

				var currentStream = File.OpenText(Path.Combine(directory.FullName, "CURRENT"));
				string manifestFilename = currentStream.ReadLine();
				currentStream.Close();

				Log.Debug($"Reading manifest from {manifestFilename}");

				using var reader = new LogReader(new FileInfo($@"{directory}{manifestFilename}"));
				version = Manifest.ReadVersionEdit(reader);
			}

			// Now we want to write this version

			var stream = new MemoryStream();

			{
				Span<byte> bytes = Manifest.EncodeVersion(version);

				Log.Debug($"Manifest (length:{bytes.Length}):\n{bytes.ToArray().HexDump()}");

				var writer = new LogWriter(stream);
				writer.WriteData(bytes);
				Log.Debug($"Manifest (length:{stream.Position}):\n{stream.ToArray().HexDump()}");
			}

			{
				stream.Position = 0;
				using var verify = new LogReader(stream);
				while (true)
				{
					ReadOnlySpan<byte> data = verify.ReadData();
					if (verify.Eof) break;

					Log.Debug($"Data:\n{data.HexDump()}");
				}

				stream.Position = 0;

				Manifest.ReadVersionEdit(verify);
			}
		}

		[Test]
		public void LevelDbWriteUserDataTest()
		{
			// Plan

			var operations = new KeyValuePair<byte[], MemCache.ResultCacheEntry>[3];
			for (int i = 0; i < 3; i++)
			{
				byte[] key = TestUtils.FillArrayWithRandomBytes(20);
				var entry = new MemCache.ResultCacheEntry();
				entry.ResultState = ResultState.Exist;
				entry.Sequence = 10;
				entry.Data = TestUtils.FillArrayWithRandomBytes(32768); // 32KB is maz size for a block, not that it matters for this
				operations[i] = new KeyValuePair<byte[], MemCache.ResultCacheEntry>(key, entry);
			}

			MemCache memCache = new MemCache();

			// Do

			ReadOnlySpan<byte> result = memCache.EncodeBatch(operations);

			// Check

			SpanReader reader = new SpanReader(result);
			Assert.AreEqual(10, reader.ReadInt64(), "Sequence number");
			Assert.AreEqual(3, reader.ReadInt32(), "Operations count");

			for (int i = 0; i < 3; i++)
			{
				var expectedKey = operations[i].Key;
				var expectedData = operations[i].Value.Data;

				Assert.AreEqual(1, reader.ReadByte(), "Operations type PUT");
				var keyLen = reader.ReadVarLong();

				Assert.AreEqual(expectedKey.Length, keyLen, "Key len");
				Assert.AreEqual(expectedKey, reader.Read(keyLen).ToArray(), "Key");

				var dataLen = reader.ReadVarLong();
				Assert.AreEqual(expectedData.Length, dataLen, "Data len");
				Assert.AreEqual(expectedData, reader.Read(dataLen).ToArray(), "Data");
			}

			// test encoding complete blocks

			var stream = new MemoryStream();
			LogWriter writer = new LogWriter(stream);
			writer.WriteData(result);
			Assert.Less(0, stream.Length);
			stream.Position = 0;

			// Roundtrip test by making sure i can read blocks I've encoded myself.

			LogReader logReader = new LogReader(stream);
			logReader.Open();

			MemCache memCache2 = new MemCache();
			memCache2.Load(logReader);

			var cache = memCache2._resultCache;
			Assert.AreEqual(3, cache.Count);

			int j = 0;
			foreach (var entry in cache)
			{
				var expectedKey = operations[j].Key;
				var expectedData = operations[j].Value.Data;

				Assert.AreEqual(ResultState.Exist, entry.Value.ResultState, "Value exists");

				Assert.AreEqual(expectedKey.Length, entry.Key.Length, "Key len");
				Assert.AreEqual(expectedKey, entry.Key, "Key");

				Assert.AreEqual(expectedData.Length, entry.Value.Data.Length, "Data len");
				Assert.AreEqual(expectedData, entry.Value.Data, "Data");
				j++;
			}
		}
	}
}