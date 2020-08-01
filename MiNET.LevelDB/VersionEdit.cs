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

using System.Collections.Generic;
using System.Linq;

namespace MiNET.LevelDB
{
	public class VersionEdit
	{
		public string Comparator { get; set; }
		public ulong LogNumber { get; set; }
		public ulong PreviousLogNumber { get; set; }
		public ulong NextFileNumber { get; set; } // Global file number counter. For all files it seems
		public ulong LastSequenceNumber { get; set; }

		public Dictionary<int, byte[]> CompactPointers { get; set; } = new Dictionary<int, byte[]>();
		public Dictionary<int, List<ulong>> DeletedFiles { get; set; } = new Dictionary<int, List<ulong>>();
		public Dictionary<int, List<FileMetadata>> NewFiles { get; set; } = new Dictionary<int, List<FileMetadata>>();

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

		public void AddNewFile(int level, FileMetadata meta)
		{
			if (!NewFiles.ContainsKey(level)) NewFiles[level] = new List<FileMetadata>();

			NewFiles[level].Add(meta);
		}

		public void AddDeletedFile(int level, ulong fileNumber)
		{
			if (!DeletedFiles.ContainsKey(level)) DeletedFiles[level] = new List<ulong>();
			if (level > 0)
			{
				List<FileMetadata> newFiles = NewFiles[level - 1];
				FileMetadata file = newFiles.FirstOrDefault(f => f.FileNumber == fileNumber);
				if (file != null) newFiles.Remove(file);
			}

			{
				List<FileMetadata> newFiles = NewFiles[level];
				FileMetadata file = newFiles.FirstOrDefault(f => f.FileNumber == fileNumber);
				if (file != null) newFiles.Remove(file);
			}

			DeletedFiles[level].Add(fileNumber);
		}

		public VersionEdit()
		{
		}

		/// <summary>
		///     Shallow copy constructor.
		/// </summary>
		/// <param name="original"></param>
		public VersionEdit(VersionEdit original)
		{
			Comparator = original.Comparator;
			LogNumber = original.LogNumber;
			PreviousLogNumber = original.PreviousLogNumber;
			NextFileNumber = original.NextFileNumber;
			LastSequenceNumber = original.LastSequenceNumber;
			CompactPointers = new Dictionary<int, byte[]>(original.CompactPointers);
			DeletedFiles = new Dictionary<int, List<ulong>>(original.DeletedFiles);
			NewFiles = new Dictionary<int, List<FileMetadata>>(original.NewFiles);
		}
	}
}