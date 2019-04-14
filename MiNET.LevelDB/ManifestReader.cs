using System.IO;

namespace MiNET.LevelDB
{
	public class ManifestReader : LogReader
	{
		public ManifestReader(Stream manifestStream) : base(manifestStream)
		{
		}


	}
}