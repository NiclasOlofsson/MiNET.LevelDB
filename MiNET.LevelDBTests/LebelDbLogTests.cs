using System;
using System.IO;
using System.Text;
using log4net;
using MiNET.LevelDB;
using Newtonsoft.Json;
using NUnit.Framework;

namespace MiNET.LevelDBTests
{
	// https://github.com/basho/leveldb/wiki/mv-overview

	[TestFixture]
	public class LebelDbLogTests
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(LebelDbLogTests));

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

			var fileStream = File.OpenRead($@"{directory}{manifestFilename}");
			ManifestReader manifestReader = new ManifestReader(fileStream);

			string comparator = null;
			ulong? logNumber = null;
			ulong? previousLogNumber = null;
			ulong? nextFileNumber = null;
			ulong? lastSequenceNumber = null;

			while (true)
			{
				Record record = manifestReader.ReadRecord();

				if (record.LogRecordType != LogRecordType.Full) break;

				Log.Debug($"{record}");

				VersionEdit versionEdit = new VersionEdit();

				var seek = new BinaryReader(new MemoryStream(record.Data));
				while (seek.BaseStream.Position < seek.BaseStream.Length)
				{
					LogTagType logTag = (LogTagType) seek.BaseStream.ReadVarint();
					switch (logTag)
					{
						case LogTagType.Comparator:
						{
							versionEdit.Comparator = ReadLenghtPrefixedString(seek);
							break;
						}
						case LogTagType.LogNumber:
						{
							versionEdit.LogNumber = seek.BaseStream.ReadVarint();
							break;
						}
						case LogTagType.NextFileNumber:
						{
							versionEdit.NextFileNumber = seek.BaseStream.ReadVarint();
							break;
						}
						case LogTagType.LastSequence:
						{
							versionEdit.LastSequenceNumber = seek.BaseStream.ReadVarint();
							break;
						}
						case LogTagType.CompactPointer:
						{
							int level = (int) seek.BaseStream.ReadVarint();
							InternalKey key = new InternalKey(ReadLenghtPrefixedBytes(seek));
							versionEdit.CompactPointers[level] = key;
							break;
						}
						case LogTagType.DeletedFile:
						{
							int level = (int) seek.BaseStream.ReadVarint();
							ulong fileNumber = seek.BaseStream.ReadVarint();
							versionEdit.DeletedFiles[level] = fileNumber;
							break;
						}
						case LogTagType.NewFile:
						{
							int level = (int) seek.BaseStream.ReadVarint();
							ulong fileNumber = seek.BaseStream.ReadVarint();
							ulong fileSize = seek.BaseStream.ReadVarint();
							var smallest = new InternalKey(ReadLenghtPrefixedBytes(seek));
							var largest = new InternalKey(ReadLenghtPrefixedBytes(seek));

							FileMetadata fileMetadata = new FileMetadata();
							fileMetadata.FileNumber = fileNumber;
							fileMetadata.FileSize = fileSize;
							fileMetadata.SmallestKey = smallest;
							fileMetadata.LargestKey = largest;
							versionEdit.NewFiles[level] = fileMetadata;
							break;
						}
						case LogTagType.PrevLogNumber:
						{
							versionEdit.PreviousLogNumber = seek.BaseStream.ReadVarint();
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

				LogToFile("------------------------------------------------------------");
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

			LogToFile("============================================================");
			Print(finalVersion);
			LogToFile("============================================================");
		}

		public static string ReadLenghtPrefixedString(BinaryReader seek)
		{
			ulong length = seek.BaseStream.ReadVarint();
			string s = Encoding.UTF8.GetString(seek.ReadBytes((int) length));
			return s;
		}

		public static byte[] ReadLenghtPrefixedBytes(BinaryReader seek)
		{
			ulong size = seek.BaseStream.ReadVarint();
			return seek.ReadBytes((int) size);
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
			LogToFile($"{result}");
		}

		[Test]
		public void LevelDbReadLogTest()
		{
			// https://github.com/google/leveldb/blob/master/doc/log_format.md

			var filestream = File.OpenRead(@"D:\Temp\World Saves PE\ExoGAHavAAA=\db\000341.log");

			LogReader logReader = new LogReader(filestream);

			while (true)
			{
				Record record = logReader.ReadRecord();

				if (record.LogRecordType != LogRecordType.Full) break;

				Log.Debug($"{record}");

				//var datareader = new BinaryReader(new MemoryStream(record.Data));

				//long sequenceNumber = datareader.ReadInt64();
				//long size = datareader.ReadInt32();

				//while (datareader.BaseStream.Position < datareader.BaseStream.Length)
				//{
				//	byte recType = datareader.ReadByte();

				//	ulong v1 = datareader.BaseStream.ReadVarint();
				//	byte[] currentKey = new byte[v1];
				//	datareader.Read(currentKey, 0, (int) v1);

				//	ulong v2 = 0;
				//	byte[] currentVal = new byte[0];
				//	if (recType == 1)
				//	{
				//		v2 = datareader.BaseStream.ReadVarint();
				//		currentVal = new byte[v2];
				//		datareader.Read(currentVal, 0, (int) v2);
				//	}

				//	LogToFile($"RecType={recType}, Sequence={sequenceNumber}, Size={size}, v1={v1}, v2={v2}\nCurrentKey={currentKey.HexDump(currentKey.Length, false, false)}\nCurrentVal=\n{currentVal.HexDump(cutAfterFive: true)} ");
				//}
			}
		}

		private static void LogToFile(string s)
		{
			Log.Debug(s.TrimEnd());
		}
	}
}