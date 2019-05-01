using System;
using System.Collections.Generic;
using System.IO;
using log4net;
using MiNET.LevelDB;
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
			var result = manifestReader.Get(new byte[] {0xf7, 0xff, 0xff, 0xff, 0xfd, 0xff, 0xff, 0xff, 0x2f, 0x05,});
			Assert.AreEqual(new byte[] {0x08, 0x01, 0x02, 0x0, 0x0}, result.Data.AsSpan(0, 5).ToArray());
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

				Log.Debug($"{record}");

				if (record.LogRecordType != LogRecordType.Full) break;

				VersionEdit versionEdit = new VersionEdit();

				var seek = new MemoryStream(record.Data);
				while (seek.Position < seek.Length)
				{
					LogTagType logTag = (LogTagType) seek.ReadVarint();
					switch (logTag)
					{
						case LogTagType.Comparator:
						{
							versionEdit.Comparator = seek.ReadLengthPrefixedString();
							break;
						}
						case LogTagType.LogNumber:
						{
							versionEdit.LogNumber = seek.ReadVarint();
							break;
						}
						case LogTagType.NextFileNumber:
						{
							versionEdit.NextFileNumber = seek.ReadVarint();
							break;
						}
						case LogTagType.LastSequence:
						{
							versionEdit.LastSequenceNumber = seek.ReadVarint();
							break;
						}
						case LogTagType.CompactPointer:
						{
							int level = (int) seek.ReadVarint();
							var key = seek.ReadLengthPrefixedBytes();
							versionEdit.CompactPointers[level] = key;
							break;
						}
						case LogTagType.DeletedFile:
						{
							int level = (int) seek.ReadVarint();
							ulong fileNumber = seek.ReadVarint();
							if (!versionEdit.DeletedFiles.ContainsKey(level)) versionEdit.DeletedFiles[level] = new List<ulong>();
							versionEdit.DeletedFiles[level].Add(fileNumber);
							if (!finalVersion.DeletedFiles.ContainsKey(level)) finalVersion.DeletedFiles[level] = new List<ulong>();
							finalVersion.DeletedFiles[level].Add(fileNumber);
							break;
						}
						case LogTagType.NewFile:
						{
							int level = (int) seek.ReadVarint();
							ulong fileNumber = seek.ReadVarint();
							ulong fileSize = seek.ReadVarint();
							var smallest = seek.ReadLengthPrefixedBytes();
							var largest = seek.ReadLengthPrefixedBytes();

							FileMetadata fileMetadata = new FileMetadata();
							fileMetadata.FileNumber = fileNumber;
							fileMetadata.FileSize = fileSize;
							fileMetadata.SmallestKey = smallest;
							fileMetadata.LargestKey = largest;
							if (!versionEdit.NewFiles.ContainsKey(level)) versionEdit.NewFiles[level] = new List<FileMetadata>();
							versionEdit.NewFiles[level].Add(fileMetadata);
							if (!finalVersion.NewFiles.ContainsKey(level)) finalVersion.NewFiles[level] = new List<FileMetadata>();
							finalVersion.NewFiles[level].Add(fileMetadata);
							break;
						}
						case LogTagType.PrevLogNumber:
						{
							versionEdit.PreviousLogNumber = seek.ReadVarint();
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

			var result = logReader.Get(new byte[] {0xeb, 0xff, 0xff, 0xff, 0xf3, 0xff, 0xff, 0xff, 0x31});

			Assert.NotNull(result.Data);
			Assert.AreEqual(new byte[] {0xA, 0x00, 0x00, 0x02, 0x05}, result.Data.AsSpan(0, 5).ToArray());
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

				Log.Debug($"{record}");

				var datareader = new BinaryReader(new MemoryStream(record.Data));

				long sequenceNumber = datareader.ReadInt64();
				long size = datareader.ReadInt32();

				while (datareader.BaseStream.Position < datareader.BaseStream.Length)
				{
					byte recType = datareader.ReadByte();

					ulong v1 = datareader.BaseStream.ReadVarint();
					byte[] currentKey = new byte[v1];
					datareader.Read(currentKey, 0, (int) v1);

					//CurrentKey = f5 ff ff ff eb ff ff ff 36

					if (comparator.Compare(new byte[] {0xf5, 0xff, 0xff, 0xff, 0xeb, 0xff, 0xff, 0xff, 0x36}, currentKey) == 0)
					{
						Assert.False(found);
						found = true;
					}

					ulong v2 = 0;
					byte[] currentVal = new byte[0];
					switch (recType)
					{
						case 1: // value
						{
							if (recType == 1)
							{
								v2 = datareader.BaseStream.ReadVarint();
								currentVal = new byte[v2];
								datareader.Read(currentVal, 0, (int) v2);
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
	}
}