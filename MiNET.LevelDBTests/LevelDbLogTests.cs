using System;
using System.Collections.Generic;
using System.IO;
using log4net;
using MiNET.LevelDB;
using MiNET.LevelDB.Utils;
using Newtonsoft.Json;
using NUnit.Framework;

namespace MiNET.LevelDBTests
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
			var directory = @"TestWorld\";

			var currentStream = File.OpenText($@"{directory}CURRENT");
			string manifestFilename = currentStream.ReadLine();
			currentStream.Close();

			Log.Debug($"Reading manifest from {manifestFilename}");

			// 08 01 02 00 00 01 00 00 00 00 00 00 00 00 00 00  ................

			ManifestReader manifestReader = new ManifestReader(new FileInfo($@"{directory}{manifestFilename}"));
			manifestReader.Open();
			var result = manifestReader.Get(new byte[] {0xf7, 0xff, 0xff, 0xff, 0xfd, 0xff, 0xff, 0xff, 0x2f, 0x05,});
			Assert.AreEqual(new byte[] {0x08, 0x01, 0x02, 0x0, 0x0}, result.Data.Slice(0, 5).ToArray());
		}

		[Test]
		public void LevelDbReadManifestTest()
		{
			// https://github.com/google/leveldb/blob/master/doc/log_format.md
			//
			// The formatting of a manifest is the same as for a log-file.
			// The CURRENT file itself is just one line of text pointing to the 
			// current MANIFEST to use (MANIFEST-000703 in the example).

			//var directory = @"D:\Temp\World Saves PE\WoUIAK-EAQA=\db\";
			//var directory = @"D:\Temp\World Saves PE\ExoGAHavAAA=\db\";
			//var directory = @"D:\Temp\My World\db\";
			var directory = @"TestWorld\";

			var currentStream = File.OpenText($@"{directory}CURRENT");
			string manifestFilename = currentStream.ReadLine();
			currentStream.Close();

			Log.Debug($"Reading manifest from {manifestFilename}");

			ManifestReader manifestReader = new ManifestReader(new FileInfo($@"{directory}{manifestFilename}"));

			string comparator = null;
			ulong? logNumber = null;
			ulong? previousLogNumber = null;
			ulong? nextFileNumber = null;
			ulong? lastSequenceNumber = null;

			VersionEdit finalVersion = new VersionEdit();

			while (true)
			{
				Record record = manifestReader.ReadRecord();

				Log.Debug($"{record.ToString()}");

				if (record.LogRecordType != LogRecordType.Full) break;

				VersionEdit versionEdit = new VersionEdit();

				var reader = new SpanReader(record.Data);
				while (!reader.Eof)
				{
					LogTagType logTag = (LogTagType) reader.ReadVarLong();
					switch (logTag)
					{
						case LogTagType.Comparator:
						{
							versionEdit.Comparator = reader.ReadLengthPrefixedString();
							break;
						}
						case LogTagType.LogNumber:
						{
							versionEdit.LogNumber = reader.ReadVarLong();
							break;
						}
						case LogTagType.NextFileNumber:
						{
							versionEdit.NextFileNumber = reader.ReadVarLong();
							break;
						}
						case LogTagType.LastSequence:
						{
							versionEdit.LastSequenceNumber = reader.ReadVarLong();
							break;
						}
						case LogTagType.CompactPointer:
						{
							int level = (int) reader.ReadVarLong();
							var key = reader.ReadLengthPrefixedBytes();
							versionEdit.CompactPointers[level] = key.ToArray();
							break;
						}
						case LogTagType.DeletedFile:
						{
							int level = (int) reader.ReadVarLong();
							ulong fileNumber = reader.ReadVarLong();
							if (!versionEdit.DeletedFiles.ContainsKey(level)) versionEdit.DeletedFiles[level] = new List<ulong>();
							versionEdit.DeletedFiles[level].Add(fileNumber);
							if (!finalVersion.DeletedFiles.ContainsKey(level)) finalVersion.DeletedFiles[level] = new List<ulong>();
							finalVersion.DeletedFiles[level].Add(fileNumber);
							break;
						}
						case LogTagType.NewFile:
						{
							int level = (int) reader.ReadVarLong();
							ulong fileNumber = reader.ReadVarLong();
							ulong fileSize = reader.ReadVarLong();
							var smallest = reader.ReadLengthPrefixedBytes();
							var largest = reader.ReadLengthPrefixedBytes();

							FileMetadata fileMetadata = new FileMetadata();
							fileMetadata.FileNumber = fileNumber;
							fileMetadata.FileSize = fileSize;
							fileMetadata.SmallestKey = smallest.ToArray();
							fileMetadata.LargestKey = largest.ToArray();
							if (!versionEdit.NewFiles.ContainsKey(level)) versionEdit.NewFiles[level] = new List<FileMetadata>();
							versionEdit.NewFiles[level].Add(fileMetadata);
							if (!finalVersion.NewFiles.ContainsKey(level)) finalVersion.NewFiles[level] = new List<FileMetadata>();
							finalVersion.NewFiles[level].Add(fileMetadata);
							break;
						}
						case LogTagType.PrevLogNumber:
						{
							versionEdit.PreviousLogNumber = reader.ReadVarLong();
							break;
						}
						default:
						{
							throw new ArgumentOutOfRangeException($"Unknown tag={logTag}");
						}
					}
				}

				versionEdit.CompactPointers = versionEdit.CompactPointers.Count == 0 ? null : versionEdit.CompactPointers;
				versionEdit.DeletedFiles = versionEdit.DeletedFiles.Count == 0 ? null : versionEdit.DeletedFiles;
				versionEdit.NewFiles = versionEdit.NewFiles.Count == 0 ? null : versionEdit.NewFiles;

				Print(versionEdit);

				comparator = versionEdit.Comparator ?? comparator;
				logNumber = versionEdit.LogNumber ?? logNumber;
				previousLogNumber = versionEdit.PreviousLogNumber ?? previousLogNumber;
				nextFileNumber = versionEdit.NextFileNumber ?? nextFileNumber;
				lastSequenceNumber = versionEdit.LastSequenceNumber ?? lastSequenceNumber;

				Log.Debug("------------------------------------------------------------");
			}

			// Clean files
			List<ulong> deletedFiles = new List<ulong>();
			foreach (var versionDeletedFile in finalVersion.DeletedFiles.Values)
			{
				deletedFiles.AddRange(versionDeletedFile);
			}

			foreach (var levelKvp in finalVersion.NewFiles)
			{
				foreach (var newFile in levelKvp.Value.ToArray())
				{
					if (deletedFiles.Contains(newFile.FileNumber)) levelKvp.Value.Remove(newFile);
				}
			}

			finalVersion.Comparator = comparator;
			finalVersion.LogNumber = logNumber;
			finalVersion.PreviousLogNumber = previousLogNumber;
			finalVersion.NextFileNumber = nextFileNumber;
			finalVersion.LastSequenceNumber = lastSequenceNumber;

			Log.Debug("============================================================");
			Print(finalVersion);
			Log.Debug("============================================================");
		}

		public static void Print(object obj)
		{
			if (!Log.IsDebugEnabled) return;

			var jsonSerializerSettings = new JsonSerializerSettings
			{
				PreserveReferencesHandling = PreserveReferencesHandling.Arrays,
				NullValueHandling = NullValueHandling.Ignore,
				Formatting = Formatting.Indented,
				Converters = {new ByteArrayConverter()}
			};

			string result = JsonConvert.SerializeObject(obj, jsonSerializerSettings);
			Log.Debug($"{result}");
		}

		[Test]
		public void LevelDbSearchLogTest()
		{
			// https://github.com/google/leveldb/blob/master/doc/log_format.md

			LogReader logReader = new LogReader(new FileInfo(@"TestWorld\000047.log"));
			logReader.Open();

			var result = logReader.Get(new byte[] {0xeb, 0xff, 0xff, 0xff, 0xf3, 0xff, 0xff, 0xff, 0x31});

			Assert.IsTrue(ReadOnlySpan<byte>.Empty != result.Data);
			Assert.AreEqual(new byte[] {0xA, 0x00, 0x00, 0x02, 0x05}, result.Data.Slice(0, 5).ToArray());
		}

		[Test]
		public void LevelDbReadLogTest()
		{
			// https://github.com/google/leveldb/blob/master/doc/log_format.md

			//var filestream = File.OpenRead(@"D:\Temp\My World\db\000028.log");

			LogReader logReader = new LogReader(new FileInfo(@"TestWorld\000047.log"));

			BytewiseComparator comparator = new BytewiseComparator();

			bool found = false;

			while (true)
			{
				Record record = logReader.ReadRecord();

				if (record.LogRecordType != LogRecordType.Full) break;

				Log.Debug($"{record.ToString()}");

				var datareader = new SpanReader(record.Data);

				long sequenceNumber = datareader.ReadInt64();
				long size = datareader.ReadInt32();

				while (!datareader.Eof)
				{
					byte recType = datareader.ReadByte();

					ulong v1 = datareader.ReadVarLong();
					var currentKey = datareader.Read(v1);

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
								v2 = datareader.ReadVarLong();
								currentVal = datareader.Read(v2);
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
		public void LevelDbWriteUserDataTest()
		{
			// Plan

			LogWriter writer = new LogWriter();
			var operations = new KeyValuePair<byte[], LogReader.ResultCacheEntry>[3];
			byte[] key = FillArrayWithRandomBytes(20);
			for (int i = 0; i < 3; i++)
			{
				var entry = new LogReader.ResultCacheEntry();
				entry.ResultState = ResultState.Exist;
				entry.Sequence = 10;
				entry.Data = FillArrayWithRandomBytes(32768); // 32KB is maz size for a block, not that it matters for this
				operations[i] = new KeyValuePair<byte[], LogReader.ResultCacheEntry>(key, entry);
			}

			// Do

			ReadOnlySpan<byte> result = writer.EncodeBatch(operations);

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
			writer.EncodeBlocks(stream, result);
			Assert.Less(0, stream.Length);
			stream.Position = 0;

			// Roundtrip test by making sure i can read blocks I've encoded myself.

			LogReader logReader = new LogReader(stream);
			logReader.Open();
			var cache = logReader._resultCache;
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

		private byte[] FillArrayWithRandomBytes(int size)
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