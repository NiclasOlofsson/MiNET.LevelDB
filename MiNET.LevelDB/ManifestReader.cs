using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using log4net;
using Newtonsoft.Json;

namespace MiNET.LevelDB
{
	/// <summary>
	///     The first layer is the "manifest". Every table file has an entry in the manifest. The manifest entry tracks the
	///     first and last key contained in each table file. The manifest keeps the table file entries in one of seven sorted
	///     arrays. Each of the seven arrays represents one "level" of table files. A user request for a key causes leveldb to
	///     check each table file that overlaps the target key. leveldb searches each potential table file, level by level,
	///     until finding the first that yields an exact match for requested key.
	/// </summary>
	public class ManifestReader : LogReader
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(ManifestReader));
		private VersionEdit _versionEdit;
		private Dictionary<ulong, TableReader> _tableCache = new Dictionary<ulong, TableReader>();

		public ManifestReader(FileInfo file) : base(file)
		{
		}

		public new ResultStatus Get(Span<byte> key)
		{
			if (_versionEdit == null)
			{
				_versionEdit = ReadVersionEdit();
				Print(_versionEdit);

				foreach (var level in _versionEdit.NewFiles)
				{
					foreach (FileMetadata tbl in level.Value)
					{
						if (!_tableCache.TryGetValue(tbl.FileNumber, out var tableReader))
						{
							FileInfo f = new FileInfo(Path.Combine(_file.DirectoryName, $"{tbl.FileNumber:000000}.ldb"));
							tableReader = new TableReader(f);
							_tableCache.TryAdd(tbl.FileNumber, tableReader);
						}
					}
				}
			}

			if (!"leveldb.BytewiseComparator".Equals(_versionEdit.Comparator, StringComparison.InvariantCultureIgnoreCase))
				throw new Exception($"Found record, but contains invalid or unsupported comparator: {_versionEdit.Comparator}");

			BytewiseComparator comparator = new BytewiseComparator();

			foreach (var level in _versionEdit.NewFiles.OrderBy(kvp => kvp.Key)) // Search all levels for file with matching index
			{
				foreach (FileMetadata tbl in level.Value)
				{
					var smallestKey = tbl.SmallestKey.UserKey();
					var largestKey = tbl.LargestKey.UserKey();
					//if (smallestKey.Length == 0 || largestKey.Length == 0) continue;

					if (comparator.Compare(key, smallestKey) >= 0 && comparator.Compare(key, largestKey) <= 0)
					{
						Log.Debug($"Found table file for key in level {level.Key} in file={tbl.FileNumber}");

						if (!_tableCache.TryGetValue(tbl.FileNumber, out var tableReader))
						{
							FileInfo f = new FileInfo(Path.Combine(_file.DirectoryName, $"{tbl.FileNumber:000000}.ldb"));
							tableReader = new TableReader(f);
							_tableCache.TryAdd(tbl.FileNumber, tableReader);
						}

						var result = tableReader.Get(key);
						if (result.State == ResultState.Exist || result.State == ResultState.Deleted) return result;
					}
				}
			}

			return ResultStatus.NotFound;
		}


		public VersionEdit ReadVersionEdit()
		{
			Reset();

			string comparator = null;
			ulong? logNumber = null;
			ulong? previousLogNumber = null;
			ulong? nextFileNumber = null;
			ulong? lastSequenceNumber = null;

			VersionEdit finalVersion = new VersionEdit();

			while (true)
			{
				Record record = ReadRecord();

				if (record.LogRecordType != LogRecordType.Full) break;

				var seek = new MemoryStream(record.Data);

				VersionEdit versionEdit = new VersionEdit();

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
							var internalKey = seek.ReadLengthPrefixedBytes();
							versionEdit.CompactPointers[level] = internalKey;
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

				comparator = versionEdit.Comparator ?? comparator;
				logNumber = versionEdit.LogNumber ?? logNumber;
				previousLogNumber = versionEdit.PreviousLogNumber ?? previousLogNumber;
				nextFileNumber = versionEdit.NextFileNumber ?? nextFileNumber;
				lastSequenceNumber = versionEdit.LastSequenceNumber ?? lastSequenceNumber;
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

			return finalVersion;
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


		public void FindFileWithKey(Span<byte> key)
		{
		}
	}
}