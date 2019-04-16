using System.IO;

namespace MiNET.LevelDB
{
	abstract class LevelDbFactory
	{
		/**
		 * Loads/creates a (new) database, located at the given File path.
		 * @param dbFolder The root path of the database folder.
		 * @return An object with database controls as specified in {@link ILevelDB}.
		 */
		public abstract ILDb LoadLevelDb(DirectoryInfo dbFolder);
	}
}