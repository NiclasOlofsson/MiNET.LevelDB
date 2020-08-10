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
using System.IO.Compression;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using log4net;
using MiNET.LevelDB.Enumerate;
using MiNET.LevelDB.Utils;

[assembly: InternalsVisibleTo("MiNET.LevelDB.Tests")]

namespace MiNET.LevelDB
{
	public interface IDatabase : IDisposable
	{
		DirectoryInfo Directory { get; }
		Options Options { get; }

		void Delete(Span<byte> key);

		void Put(Span<byte> key, Span<byte> value);

		byte[] Get(Span<byte> key);

		List<string> GetDbKeysStartingWith(string startWith);

		void Open();

		void Close();

		bool IsClosed();
	}

	public class Options
	{
		public ulong MaxMemCacheSize { get; set; } = 4_000_000L; // 4Mb size before we rotate and compact
		public ulong MaxNumberOfLogFiles { get; set; } = 4;
		public ulong MaxTableFileSize { get; set; } = 2_000_000;
		public ulong LevelSizeBaseFactor { get; set; } = 10; // the size increase factor between L and L+1 => n^level
		public bool RetainAllFiles { get; set; } = false;
	}

	public class Database : IDatabase
	{
		private static readonly ILog Log = LogManager.GetLogger(typeof(Database));
		private Manifest _manifest;
		private MemCache _memCache;
		private MemCache _immutableMemCache;
		private ulong _logNumber;
		private LogWriter _log;
		private Statistics _statistics = new Statistics();
		private bool _createIfMissing;

		public DirectoryInfo Directory { get; private set; }
		public Options Options { get; }

		public static bool ParanoidMode { get; set; }

		private ReaderWriterLockSlim _dbLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
		private Task _compactTask = null;
		private Timer _compactTimer;

		public Database(DirectoryInfo dbDirectory, bool createIfMissing = false, Options options = null)
		{
			Directory = dbDirectory;
			_createIfMissing = createIfMissing;
			Options = options ?? new Options();
		}

		public void Delete(Span<byte> key)
		{
			if (_manifest == null) throw new InvalidOperationException("No manifest for database. Did you open it?");
			if (_memCache == null) throw new InvalidOperationException("No current memory cache for database. Did you open it?");

			var operation = new BatchOperation()
			{
				Key = key.ToArray(),
				Data = null,
				ResultState = ResultState.Deleted
			};

			PutInternal(operation);
		}

		public void Put(Span<byte> key, Span<byte> value)
		{
			if (_manifest == null) throw new InvalidOperationException("No manifest for database. Did you open it?");
			if (_memCache == null) throw new InvalidOperationException("No current memory cache for database. Did you open it?");

			var operation = new BatchOperation()
			{
				Key = key.ToArray(),
				Data = value.ToArray(),
				ResultState = ResultState.Exist
			};

			PutInternal(operation);
		}

		private void PutInternal(BatchOperation operation)
		{
			_dbLock.EnterWriteLock();

			try
			{
				MakeSurePutWorks();

				ulong sequenceNumber = _manifest.CurrentVersion.GetNextSequenceNumber();
				var batch = new WriteBatch
				{
					Sequence = sequenceNumber,
				};
				batch.Operations.Add(operation);

				// Write to LOG here.
				_log?.WriteData(batch.EncodeBatch());
				_memCache.Put(batch);
			}
			finally
			{
				_dbLock.ExitWriteLock();
			}
		}

		public byte[] Get(Span<byte> key)
		{
			_dbLock.EnterReadLock();

			try
			{
				if (_manifest == null) throw new InvalidOperationException("No manifest for database. Did you open it?");
				if (_memCache == null) throw new InvalidOperationException("No current memory cache for database. Did you open it?");

				ResultStatus result = _memCache.Get(key);
				if (result.State == ResultState.Deleted || result.State == ResultState.Exist)
				{
					if (result.Data == ReadOnlySpan<byte>.Empty) return null;
					return result.Data.ToArray();
				}
				MemCache imm = _immutableMemCache;
				if (imm != null)
				{
					result = imm.Get(key);
					if (result.State == ResultState.Deleted || result.State == ResultState.Exist)
					{
						if (result.Data == ReadOnlySpan<byte>.Empty) return null;
						return result.Data.ToArray();
					}
				}

				result = _manifest.Get(key);
				if (result.Data == ReadOnlySpan<byte>.Empty) return null;
				return result.Data.ToArray();
			}
			finally
			{
				_dbLock.ExitReadLock();
			}
		}

		public List<string> GetDbKeysStartingWith(string startWith)
		{
			throw new NotImplementedException();
		}

		public void Open()
		{
			if (_dbLock == null) throw new ObjectDisposedException("Database was closed and can not be reopened");

			_dbLock.EnterWriteLock();

			try
			{
				if (_manifest != null) throw new InvalidOperationException("Already had manifest for database. Did you already open it?");
				if (_memCache != null) throw new InvalidOperationException("Already had memory cache for database. Did you already open it?");

				if (Directory.Name.EndsWith(".mcworld"))
				{
					// Exported from MCPE. Unpack to temp

					Log.Debug($"Opening directory: {Directory.Name}");

					var originalFile = Directory;

					string newDirPath = Path.Combine(Path.GetTempPath(), Directory.Name);
					Directory = new DirectoryInfo(Path.Combine(newDirPath, "db"));
					if (!Directory.Exists || originalFile.LastWriteTimeUtc > Directory.LastWriteTimeUtc)
					{
						ZipFile.ExtractToDirectory(originalFile.FullName, newDirPath, true);
						Log.Warn($"Created new temp directory: {Directory.FullName}");
					}
					Directory.Refresh();
					Log.Warn($"Extracted bedrock world and set new DB directory to: {Directory.FullName}");
				}

				// Verify that directory exists
				if (!Directory.Exists)
				{
					Directory.Create();
					Directory.Refresh();
				}
				if (!File.Exists(GetCurrentFileName()))
				{
					if (!_createIfMissing)
					{
						var notFoundException = new DirectoryNotFoundException(Directory.FullName);
						Log.Error(notFoundException);
						throw notFoundException;
					}

					// Create new MANIFEST

					var manifest = new Manifest(Directory);
					manifest.CurrentVersion = new Version()
					{
						Comparator = "leveldb.BytewiseComparator",
						LogNumber = 1,
						PreviousLogNumber = 0,
						NextFileNumber = 2,
						LastSequenceNumber = 0
					};
					var manifestFileInfo = new FileInfo(GetManifestFileName(1));
					if (manifestFileInfo.Exists) throw new PanicException($"Trying to create database, but found existing MANIFEST file at {manifestFileInfo.FullName}. Aborting.");
					using var writer = new LogWriter(manifestFileInfo);
					manifest.Save(writer);
					manifest.Close();

					// Create new CURRENT text file and store manifest filename in it
					using StreamWriter current = File.CreateText(GetCurrentFileName());
					current.WriteLine(manifestFileInfo.Name);
					current.Close();

					// Done and created
				}

				Directory.Refresh(); // If this has been manipulated on the way, this is really needed.

				// Read Manifest into memory

				string manifestFilename = GetManifestFileNameFromCurrent();
				Log.Debug($"Reading manifest from {manifestFilename}");
				using (var reader = new LogReader(new FileInfo(manifestFilename)))
				{
					_manifest = new Manifest(Directory);
					_manifest.Load(reader);
				}

				// Read current log
				var logFile = new FileInfo(GetLogFileName(_manifest.CurrentVersion.LogNumber));
				Log.Debug($"Reading log from {logFile.FullName}");
				using (var reader = new LogReader(logFile))
				{
					_memCache = new MemCache();
					_memCache.Load(reader);
				}

				// Append mode
				_log = new LogWriter(logFile);
			}
			finally
			{
				_dbLock.ExitWriteLock();
			}

			// We do this on startup. It will rotate the log files and create
			// level 0 tables. However, we want to use into reusing the logs.
			CompactMemCache();
			CleanOldFiles();

			_compactTimer = new Timer(state => DoCompaction(), null, 300, 300);
		}

		public void Close()
		{
			if (_dbLock == null) return;

			_compactTask?.Wait();

			_dbLock?.EnterWriteLock();
			if (_dbLock == null) return;
			try
			{
				if (_compactTimer != null)
				{
					var timeFinishWaiter = new ManualResetEvent(false);
					_compactTimer.Dispose(timeFinishWaiter);
					timeFinishWaiter.WaitOne();
				}

				_memCache = null;
				_log?.Close();
				_log = null;
				_manifest?.Close();
				_manifest = null;
			}
			finally
			{
				var dbLock = _dbLock;
				_dbLock = null;
				dbLock.ExitWriteLock();
				dbLock?.Dispose();
			}
		}

		public bool IsClosed()
		{
			throw new NotImplementedException();
		}

		public void Dispose()
		{
			Close();
		}

		private string GetCurrentFileName()
		{
			return Path.Combine(Directory.FullName, "CURRENT");
		}

		private string GetLogFileName(ulong fileNumber)
		{
			return Path.Combine(Directory.FullName, $"{fileNumber:000000}.log");
		}

		private string GetTableFileName(ulong fileNumber)
		{
			return Path.Combine(Directory.FullName, $"{fileNumber:000000}.ldb");
		}

		private string GetManifestFileName(ulong fileNumber)
		{
			return Path.Combine(Directory.FullName, $"MANIFEST-{fileNumber:000000}");
		}

		private string GetManifestFileNameFromCurrent()
		{
			using StreamReader currentStream = File.OpenText(GetCurrentFileName());
			string manifestFileName = currentStream.ReadLine();
			if (string.IsNullOrEmpty(manifestFileName)) throw new FileNotFoundException("Missing content in CURRENT");

			manifestFileName = Path.Combine(Directory.FullName, manifestFileName);
			if (!File.Exists(manifestFileName)) throw new FileNotFoundException(manifestFileName);
			return manifestFileName;
		}

		private void MakeSurePutWorks()
		{
			if (!_dbLock.IsWriteLockHeld) throw new SynchronizationLockException("Expected caller to hold write lock");

			if (_memCache.GetEstimatedSize() < Options.MaxMemCacheSize) return; // All fine, carry on

			if (_immutableMemCache != null)
			{
				// still compacting
				//_compactReset.WaitOne();
			}
			else
			{
				// Rotate memcache and log

				Log.Debug($"Time to rotate memcache. Size={_memCache.GetEstimatedSize()} bytes");

				LogWriter oldLog = _log;

				ulong logNumber = _manifest.CurrentVersion.GetNewFileNumber();
				_log = new LogWriter(new FileInfo(GetLogFileName(logNumber)));
				_logNumber = logNumber;

				_immutableMemCache = _memCache;
				_memCache = new MemCache();

				oldLog.Close();

				_compactTask = new Task(CompactMemCache);
				_compactTask.Start();
				// Schedule compact
			}
		}

		private ManualResetEvent _compactReset = new ManualResetEvent(true);

		private void CompactMemCache()
		{
			Log.Debug($"Checking if we should compact");

			_dbLock.EnterWriteLock();

			try
			{
				//if (!force && _memCache.GetEstimatedSize() < Options.MaxMemCacheSize) return; // 4Mb

				if (_immutableMemCache == null) return;

				Log.Debug($"Compact kicking in");

				_compactReset.Reset();

				// Write immutable memcache to a level 0 table

				Version version = _manifest.CurrentVersion;
				ulong newFileNumber = version.GetNewFileNumber();

				var tableFileInfo = new FileInfo(GetTableFileName(newFileNumber));
				FileMetadata meta = WriteLevel0Table(_immutableMemCache, tableFileInfo);
				meta.FileNumber = newFileNumber;
				Level0Tables.Add(meta);

				// Update version data and commit new manifest (save)

				var newVersion = new Version(version);
				newVersion.LogNumber = _logNumber;
				newVersion.AddFile(0, meta);
				Manifest.Print(Log, newVersion);

				_immutableMemCache = null;

				// Update manifest with new version
				_manifest.CurrentVersion = newVersion;

				var manifestFileName = new FileInfo(GetManifestFileName(newVersion.GetNewFileNumber()));
				using (var writer = new LogWriter(manifestFileName))
				{
					_manifest.Save(writer);
				}

				using (StreamWriter currentStream = File.CreateText(GetCurrentFileName()))
				{
					currentStream.WriteLine(manifestFileName.Name);
					currentStream.Close();
				}

				CleanOldFiles();

				_compactTask = null;
				_compactReset.Set();
			}
			finally
			{
				_dbLock.ExitWriteLock();
			}
		}

		public List<FileMetadata> Level0Tables { get; private set; } = new List<FileMetadata>();
		public Version Version => _manifest.CurrentVersion;

		internal FileMetadata WriteLevel0Table(MemCache memCache, FileInfo tableFileInfo)
		{
			using FileStream fileStream = File.Create(tableFileInfo.FullName);
			var creator = new TableCreator(fileStream);

			byte[] smallestKey = null;
			byte[] largestKey = null;

			foreach (KeyValuePair<byte[], MemCache.ResultCacheEntry> entry in memCache._resultCache.OrderBy(kvp => kvp.Key, new BytewiseComparator()).ThenBy(kvp => kvp.Value.Sequence))
			{
				if (entry.Value.ResultState != ResultState.Exist && entry.Value.ResultState != ResultState.Deleted) continue;

				byte[] opAndSeq = BitConverter.GetBytes((ulong) entry.Value.Sequence << 8);
				opAndSeq[0] = (byte) (entry.Value.ResultState == ResultState.Exist ? 1 : 0);

				byte[] key = entry.Key.Concat(opAndSeq).ToArray();
				byte[] data = entry.Value.Data;

				smallestKey ??= key;
				largestKey = key;

				//if (Log.IsDebugEnabled)
				//{
				//	if (entry.Value.ResultState == ResultState.Deleted)
				//		Log.Warn($"Key:{key.ToHexString()} {entry.Value.Sequence}, {entry.Value.ResultState == ResultState.Exist}, size:{entry.Value.Data?.Length ?? 0}");
				//	else
				//Log.Debug($"Key: {entry.Key.ToHexString()} FullKey:{key.ToHexString()} Seq: {entry.Value.Sequence}, Exist: {entry.Value.ResultState == ResultState.Exist}");
				//}

				creator.Add(key, data);
			}

			creator.Finish();
			fileStream.Flush();
			fileStream.Close();
			tableFileInfo.Refresh();

			Log.Debug($"Size distinct:{memCache._resultCache.Distinct().Count()}");
			Log.Debug($"Wrote {memCache._resultCache.Count} values to {tableFileInfo.Name}");

			return new FileMetadata
			{
				FileNumber = 0, // Set in calling method
				FileSize = (ulong) tableFileInfo.Length,
				SmallestKey = smallestKey,
				LargestKey = largestKey,
				Table = new Table(tableFileInfo)
			};
		}

		private void DoCompaction()
		{
			if (!_dbLock.TryEnterWriteLock(0)) return;

			try
			{
				TryCompactLevel0();
				TryCompactLevelN();
			}
			catch (Exception e)
			{
				Log.Error(e);
				throw;
			}
			finally
			{
				_dbLock.ExitWriteLock();
			}
		}

		private void TryCompactLevel0()
		{
			Log.Debug($"Trying to do merge compact of level 0 to level 1..");

			if (!_dbLock.IsWriteLockHeld)
			{
				Log.Error("Panic!");
				throw new PanicException("Tried to compact without holding a write lock. Abort!");
			}

			Version version = _manifest.CurrentVersion;
			List<FileMetadata> level0FilesToCompact = version.GetFiles(0);

			if (level0FilesToCompact.Count < (int) Options.MaxNumberOfLogFiles)
			{
				Log.Debug($"No level 0 to 1 compact needed at this time.");
				return;
			}

			byte[] smallestKey = null;
			byte[] largestKey = null;
			foreach (FileMetadata metadata in level0FilesToCompact.OrderBy(f => f.SmallestKey, new InternalKeyComparator()))
			{
				smallestKey ??= metadata.SmallestKey;
				largestKey = metadata.LargestKey;
			}

			Log.Debug($"Smallest Key: {smallestKey.ToHexString()}, Largest Key: {largestKey.ToHexString()}");

			var filesToCompact = new List<FileMetadata>(level0FilesToCompact);
			// Add overlapping level 1 files
			List<FileMetadata> overlappingFiles = version.GetOverlappingFiles(1, smallestKey, largestKey);
			filesToCompact.AddRange(overlappingFiles);
			Log.Warn($"Overlapping files: {overlappingFiles.Count}");

			var files = new List<TableEnumerator>();
			foreach (FileMetadata metadata in filesToCompact.OrderBy(f => f.SmallestKey, new BytewiseComparator()))
			{
				metadata.Table ??= _manifest.GetTable(metadata.FileNumber);
				metadata.Table.Initialize();
				Log.Debug($"Add enumerator for {metadata.FileNumber}, idx len {metadata.Table._blockIndex.Length}");
				files.Add((TableEnumerator) metadata.Table.GetEnumerator());
			}

			var mergeEnumerator = new MergeEnumerator(files);
			int count = 0;

			var newFiles = new List<FileMetadata>();
			while (true)
			{
				FileMetadata newFileMeta = WriteMergedTable(version, mergeEnumerator, ref count);
				if (newFileMeta.SmallestKey == null) break;
				newFiles.Add(newFileMeta);
			}

			if (Log.IsDebugEnabled)
			{
				Log.Debug($"Compaction L0 * {level0FilesToCompact.Count} -> L1 * {newFiles.Count}");
				foreach (FileMetadata file in level0FilesToCompact)
				{
					Log.Debug($"Compacted L0@{file.FileNumber}");
				}
				foreach (FileMetadata file in overlappingFiles)
				{
					Log.Debug($"Compacted L1@{file.FileNumber}");
				}
				foreach (FileMetadata newFile in newFiles)
				{
					Log.Debug($"Created L1@{newFile.FileNumber}");
				}
			}

			var newVersion = new Version(version);
			newVersion.LogNumber = _logNumber;
			foreach (FileMetadata originalFile in filesToCompact)
			{
				newVersion.RemoveFile(overlappingFiles.Contains(originalFile) ? 1 : 0, originalFile.FileNumber);
			}

			foreach (var fileMetadata in newFiles)
			{
				newVersion.AddFile(1, fileMetadata);
			}

			Manifest.Print(Log, newVersion);

			// Update manifest with new version
			_manifest.CurrentVersion = newVersion;

			var manifestFileName = new FileInfo(GetManifestFileName(newVersion.GetNewFileNumber()));
			using (var writer = new LogWriter(manifestFileName))
			{
				_manifest.Save(writer);
			}

			using (StreamWriter currentStream = File.CreateText(GetCurrentFileName()))
			{
				currentStream.WriteLine(manifestFileName.Name);
				currentStream.Close();
			}

			CleanOldFiles();

			Log.Debug($"Done with merge compact of level 0 to level 1..");
		}

		private void TryCompactLevelN()
		{
			Log.Debug($"Trying to do merge compact of level n to level ..");

			if (!_dbLock.IsWriteLockHeld)
			{
				Log.Error("Panic!");
				throw new PanicException("Tried to compact without holding a write lock. Abort!");
			}

			Version version = _manifest.CurrentVersion;
			bool manifestIsDirty = false;
			foreach (KeyValuePair<int, List<FileMetadata>> kvp in version.Levels.Where(kvp => kvp.Key > 0).ToArray())
			{
				int level = kvp.Key;
				List<FileMetadata> files = kvp.Value;

				if (files.Count > Math.Pow(Options.LevelSizeBaseFactor, level))
				{
					manifestIsDirty = true;
					Log.Warn($"Want to compact level {level} with {files.Count} files.");

					while (true)
					{
						FileMetadata fileToCompact = GetNextFileToCompactForLevel(files, version.GetCompactPointer(level));

						if (fileToCompact == null) break; // Nah, shouldn't happen, right.

						if (files.Count <= Math.Pow(Options.LevelSizeBaseFactor, level)) break;

						List<FileMetadata> overlappingFiles = version.GetOverlappingFiles(level + 1, fileToCompact.SmallestKey, fileToCompact.LargestKey);
						if (overlappingFiles.Count == 0)
						{
							// Level+1 has no files overlapping. So we can basically just move this file up the level.
							Log.Warn($"Want to compact level {level}, file {fileToCompact.FileNumber} and level {level + 1} had No overlapping files. Simple move.");
							version.RemoveFile(level, fileToCompact.FileNumber);
							version.AddFile(level + 1, fileToCompact);
							version.SetCompactPointer(level, fileToCompact.LargestKey);
						}
						else
						{
							Log.Warn($"Want to compact level {level}, file {fileToCompact.FileNumber} and level {level + 1} had {overlappingFiles.Count} overlapping files");
							var mergeFiles = new List<FileMetadata>(overlappingFiles);
							mergeFiles.Add(fileToCompact);

							var tableEnumerators = new List<TableEnumerator>();
							foreach (FileMetadata metadata in mergeFiles.OrderBy(f => f.SmallestKey, new BytewiseComparator()))
							{
								metadata.Table ??= _manifest.GetTable(metadata.FileNumber);
								metadata.Table.Initialize();
								Log.Debug($"Add enumerator for {metadata.FileNumber}, idx len {metadata.Table._blockIndex.Length}");
								tableEnumerators.Add((TableEnumerator) metadata.Table.GetEnumerator());
							}

							var mergeEnumerator = new MergeEnumerator(tableEnumerators);
							var newFiles = new List<FileMetadata>();
							int count = 0;
							while (true)
							{
								FileMetadata newFileMeta = WriteMergedTable(version, mergeEnumerator, ref count);
								if (newFileMeta.SmallestKey == null) break;
								version.SetCompactPointer(level, newFileMeta.LargestKey);
								newFiles.Add(newFileMeta);
							}

							foreach (FileMetadata originalFile in mergeFiles)
							{
								version.RemoveFile(overlappingFiles.Contains(originalFile) ? level + 1 : level, originalFile.FileNumber);
							}

							foreach (var fileMetadata in newFiles)
							{
								version.AddFile(level + 1, fileMetadata);
							}
						}
					}

					//break;
				}
			}

			if (manifestIsDirty)
			{
				var manifestFileName = new FileInfo(GetManifestFileName(_manifest.CurrentVersion.GetNewFileNumber()));
				using (var writer = new LogWriter(manifestFileName))
				{
					_manifest.Save(writer);
				}

				using (StreamWriter currentStream = File.CreateText(GetCurrentFileName()))
				{
					currentStream.WriteLine(manifestFileName.Name);
					currentStream.Close();
				}

				Manifest.Print(Log, version);

				CleanOldFiles();
			}

			Log.Debug($"Done merge compact of level n to level ..");
		}

		public FileMetadata GetNextFileToCompactForLevel(List<FileMetadata> files, byte[] lastCompactionKey)
		{
			var comparator = new InternalKeyComparator();

			foreach (FileMetadata file in files)
			{
				if (comparator.Compare(file.SmallestKey, lastCompactionKey) >= 0) return file;
			}

			return files.FirstOrDefault();
		}

		private FileMetadata WriteMergedTable(Version version, MergeEnumerator mergeEnumerator, ref int count)
		{
			var newFileMeta = new FileMetadata {FileNumber = version.GetNewFileNumber()};
			var newFileInfo = new FileInfo(GetTableFileName(newFileMeta.FileNumber));
			using FileStream newFileStream = newFileInfo.Create();
			var creator = new TableCreator(newFileStream);

			byte[] smallestKey = null;
			byte[] largestKey = null;

			ReadOnlyMemory<byte> prevKey = null;
			while (mergeEnumerator.MoveNext())
			{
				BlockEntry entry = mergeEnumerator.Current;

				count++;
				ReadOnlyMemory<byte> key = entry.Key;
				if (prevKey.Length != 0)
				{
					if (prevKey.Span.UserKey().SequenceEqual(key.Span.UserKey()))
					{
						Log.Warn($"Duplicate keys - Prev Key: {prevKey.ToHexString()}, Key: {key.ToHexString()}");
						continue;
					}
				}

				prevKey = key;

				creator.Add(key.Span, entry.Data.Span);
				smallestKey ??= key.ToArray();
				largestKey = key.ToArray();

				if (creator.CurrentSize > Options.MaxTableFileSize) break;
			}

			creator.Finish();

			newFileMeta.SmallestKey = smallestKey;
			newFileMeta.LargestKey = largestKey;
			newFileInfo.Refresh();
			newFileMeta.FileSize = (ulong) newFileInfo.Length;

			return newFileMeta;
		}

		internal void CleanOldFiles()
		{
			if (Options.RetainAllFiles) return;

			Directory.Refresh();
			Version version = _manifest.CurrentVersion;

			foreach (FileInfo file in Directory.GetFiles())
			{
				switch (file.Name)
				{
					case { } s when s.EndsWith(".log"):
					{
						ulong number = ulong.Parse(s.Replace(".log", ""));
						if (number < version.LogNumber) file.Delete();
						break;
					}
					case { } s when s.EndsWith(".ldb"):
					{
						ulong number = ulong.Parse(s.Replace(".ldb", ""));
						if (number >= version.NextFileNumber) continue;

						FileMetadata meta = version.Levels.FirstOrDefault(kvp => kvp.Value.Any(f => f.FileNumber == number)).Value?.FirstOrDefault(f => f.FileNumber == number);
						if (meta == null)
						{
							file.Delete();
						}
						break;
					}
					case { } s when s.StartsWith("MANIFEST-"):
					{
						ulong number = ulong.Parse(s.Replace("MANIFEST-", ""));
						string currentName = new FileInfo(GetManifestFileNameFromCurrent()).Name;
						ulong currentNumber = ulong.Parse(currentName.Replace("MANIFEST-", ""));
						if (number < currentNumber) file.Delete();
						break;
					}
				}
			}
		}
	}

	public class PanicException : Exception
	{
		public PanicException(string message) : base(message)
		{
		}
	}

	internal class BatchOperation
	{
		public byte[] Key { get; set; }
		public byte[] Data { get; set; }
		public ResultState ResultState { get; set; } = ResultState.Undefined;
	}

	internal class WriteBatch
	{
		public ulong Sequence { get; set; }
		public List<BatchOperation> Operations = new List<BatchOperation>();

		internal ReadOnlySpan<byte> EncodeBatch()
		{
			if (Operations.Count == 0) throw new ArgumentException("Zero size batch", nameof(Operations));

			long maxSize = 0;
			maxSize += 8; // sequence
			maxSize += 4 * Operations.Count; // count
			foreach (BatchOperation entry in Operations)
			{
				maxSize += 1; // op code
				maxSize += 10; // varint max
				maxSize += entry.Key.Length;
				if (entry.ResultState == ResultState.Exist)
				{
					maxSize += 10; // varint max
					maxSize += entry.Data?.Length ?? 0;
				}
			}

			Span<byte> data = new byte[maxSize]; // big enough to contain all data regardless of size

			var writer = new SpanWriter(data);

			// write sequence
			writer.Write((ulong) Sequence);
			// write operations count
			writer.Write((uint) Operations.Count);

			foreach (var operation in Operations)
			{
				byte[] key = operation.Key;
				// write op type (byte)
				writer.Write(operation.ResultState == ResultState.Exist ? (byte) OperationType.Value : (byte) OperationType.Delete);
				// write key
				writer.WriteLengthPrefixed(key);

				if (operation.ResultState == ResultState.Exist)
				{
					// write data
					writer.WriteLengthPrefixed(operation.Data);
				}
			}

			return data.Slice(0, writer.Position);
		}
	}


	public class Statistics
	{
		public int QuerySuccesses { get; set; }
		public int QueryFailes { get; set; }
		public int TableCacheHits { get; set; }
	}
}