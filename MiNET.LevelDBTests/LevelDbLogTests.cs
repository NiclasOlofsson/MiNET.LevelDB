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

		[Test]
		public void LevelDbSearchManifestTest()
		{
			var directory = @"D:\Temp\My World\db\";

			var currentStream = File.OpenText($@"{directory}CURRENT");
			string manifestFilename = currentStream.ReadLine();
			currentStream.Close();

			Log.Debug($"Reading manifest from {manifestFilename}");

			var fileStream = File.OpenRead($@"{directory}{manifestFilename}");

			ManifestReader manifestReader = new ManifestReader(new FileInfo($@"{directory}{manifestFilename}"), fileStream);
			byte[] result = manifestReader.Get(new byte[] {0xfe, 0xff, 0xff, 0xff, 0xf1, 0xff, 0xff, 0xff, 0x2d, 0x01, 0x06, 0x39, 0x00, 0x00, 0x00, 0x00, 0x00,});
			Assert.AreEqual(new byte[] {0x42, 0x0, 0x42, 0x0, 0x42}, result.AsSpan(0, 5).ToArray());
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
			var directory = @"D:\Temp\My World\db\";

			var currentStream = File.OpenText($@"{directory}CURRENT");
			string manifestFilename = currentStream.ReadLine();
			currentStream.Close();

			Log.Debug($"Reading manifest from {manifestFilename}");

			var fileStream = File.OpenRead($@"{directory}{manifestFilename}");
			ManifestReader manifestReader = new ManifestReader(new FileInfo($@"{directory}{manifestFilename}"), fileStream);

			string comparator = null;
			ulong? logNumber = null;
			ulong? previousLogNumber = null;
			ulong? nextFileNumber = null;
			ulong? lastSequenceNumber = null;

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
							InternalKey key = new InternalKey(seek.ReadLengthPrefixedBytes());
							versionEdit.CompactPointers[level] = key;
							break;
						}
						case LogTagType.DeletedFile:
						{
							int level = (int) seek.ReadVarint();
							ulong fileNumber = seek.ReadVarint();
							versionEdit.DeletedFiles[level] = fileNumber;
							break;
						}
						case LogTagType.NewFile:
						{
							int level = (int) seek.ReadVarint();
							ulong fileNumber = seek.ReadVarint();
							ulong fileSize = seek.ReadVarint();
							var smallest = new InternalKey(seek.ReadLengthPrefixedBytes());
							var largest = new InternalKey(seek.ReadLengthPrefixedBytes());

							FileMetadata fileMetadata = new FileMetadata();
							fileMetadata.FileNumber = fileNumber;
							fileMetadata.FileSize = fileSize;
							fileMetadata.SmallestKey = smallest;
							fileMetadata.LargestKey = largest;
							if (!versionEdit.NewFiles.ContainsKey(level)) versionEdit.NewFiles[level] = new List<FileMetadata>();
							versionEdit.NewFiles[level].Add(fileMetadata);
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

			VersionEdit finalVersion = new VersionEdit();
			finalVersion.Comparator = comparator;
			finalVersion.LogNumber = logNumber;
			finalVersion.PreviousLogNumber = previousLogNumber;
			finalVersion.NextFileNumber = nextFileNumber;
			finalVersion.LastSequenceNumber = lastSequenceNumber;
			finalVersion.CompactPointers = null;
			finalVersion.DeletedFiles = null;
			finalVersion.NewFiles = null;

			Log.Debug("============================================================");
			Print(finalVersion);
			Log.Debug("============================================================");
		}

		public static void Print(object obj)
		{
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

			var filestream = File.OpenRead(@"D:\Temp\World Saves PE\ExoGAHavAAA=\db\000341.log");

			LogReader logReader = new LogReader(filestream);
			byte[] result = logReader.Get(new byte[] {0xfc, 0xff, 0xff, 0xff, 0xf3, 0xff, 0xff, 0xff, 0x31,});

			Assert.NotNull(result);
			Assert.AreEqual(new byte[] {0xA, 0x00, 0x00, 0x02, 0x05}, result.AsSpan(0, 5).ToArray());
		}

		[Test]
		public void LevelDbReadLogTest()
		{
			// https://github.com/google/leveldb/blob/master/doc/log_format.md

			var filestream = File.OpenRead(@"D:\Temp\World Saves PE\ExoGAHavAAA=\db\000341.log");
			//var filestream = File.OpenRead(@"D:\Temp\My World\db\000028.log");

			LogReader logReader = new LogReader(filestream);

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

					// CurrentKey = fc ff ff ff f3 ff ff ff 31
					if (comparator.Compare(new byte[] {0xfc, 0xff, 0xff, 0xff, 0xf3, 0xff, 0xff, 0xff, 0x31,}, currentKey) == 0)
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

					Log.Debug($"RecType={recType}, Sequence={sequenceNumber}, Size={size}, v1={v1}, v2={v2}\nCurrentKey={currentKey.HexDump(currentKey.Length, false, false)}\nCurrentVal=\n{currentVal.HexDump(cutAfterFive: true)} ");
				}
			}

			Assert.True(found);
		}
	}
}