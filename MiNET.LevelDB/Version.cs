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
using System.Linq;
using log4net.Core;
using MiNET.LevelDB.Utils;

namespace MiNET.LevelDB
{
	public class Version
	{
		public string Comparator { get; set; }
		public ulong LogNumber { get; set; }
		public ulong PreviousLogNumber { get; set; }
		public ulong NextFileNumber { get; set; } // Global file number counter. For all files it seems
		public ulong LastSequenceNumber { get; set; }

		public Dictionary<int, byte[]> CompactPointers { get; set; } = new Dictionary<int, byte[]>();
		public Dictionary<int, List<ulong>> DeletedFiles { get; set; } = new Dictionary<int, List<ulong>>();
		public Dictionary<int, List<FileMetadata>> Levels { get; set; } = new Dictionary<int, List<FileMetadata>>();

		private object _seqLock = new object();

		public ulong GetNextSequenceNumber()
		{
			lock (_seqLock)
			{
				return LastSequenceNumber++;
			}
		}

		public ulong GetNewFileNumber()
		{
			lock (_seqLock)
			{
				return NextFileNumber++;
			}
		}

		public byte[] GetCompactPointer(int level)
		{
			if (!CompactPointers.ContainsKey(level)) return null;

			return CompactPointers[level];
		}

		public void SetCompactPointer(int level, byte[] key)
		{
			CompactPointers[level] = key;
		}

		public void RemoveCompactPointer(int level)
		{
			CompactPointers.Remove(level);
		}

		public List<FileMetadata> GetFiles(int level)
		{
			if (!Levels.ContainsKey(level)) Levels[level] = new List<FileMetadata>();

			return new List<FileMetadata>(Levels[level]);
		}

		public List<FileMetadata> GetOverlappingFiles(int level, byte[] smallestKey, byte[] largestKey)
		{
			if (!Levels.ContainsKey(level)) return new List<FileMetadata>();

			var overlappingFiles = new List<FileMetadata>();
			var comparator = new BytewiseComparator();
			foreach (FileMetadata metadata in GetFiles(level))
			{
				if (comparator.Compare(metadata.SmallestKey, largestKey) <= 0 && comparator.Compare(metadata.LargestKey, smallestKey) >= 0)
				{
					overlappingFiles.Add(metadata);
				}
			}

			return overlappingFiles;
		}


		public void AddFile(int level, FileMetadata meta)
		{
			if (!Levels.ContainsKey(level)) Levels[level] = new List<FileMetadata>();

			Levels[level].Add(meta);
		}

		public void RemoveFile(int level, ulong fileNumber)
		{
			List<FileMetadata> levelFiles = GetFiles(level);
			FileMetadata file = levelFiles.FirstOrDefault(f => f.FileNumber == fileNumber);
			if (file == null) throw new Exception($"Expected to find file {fileNumber} in level {level}, but did not");

			Levels[level].Remove(file);
			file.Table?.Dispose();

			if (!DeletedFiles.ContainsKey(level)) DeletedFiles[level] = new List<ulong>();
			DeletedFiles[level].Add(file.FileNumber);
		}

		public Version()
		{
		}

		/// <summary>
		///     Shallow copy constructor.
		/// </summary>
		/// <param name="original"></param>
		public Version(Version original)
		{
			Comparator = original.Comparator;
			LogNumber = original.LogNumber;
			PreviousLogNumber = original.PreviousLogNumber;
			NextFileNumber = original.NextFileNumber;
			LastSequenceNumber = original.LastSequenceNumber;
			CompactPointers = new Dictionary<int, byte[]>(original.CompactPointers);
			DeletedFiles = new Dictionary<int, List<ulong>>(original.DeletedFiles);
			Levels = new Dictionary<int, List<FileMetadata>>(original.Levels);
		}
	}
}