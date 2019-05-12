using System;
using System.Linq;
using System.Text;

namespace MiNET.LevelDB.Utils
{
	public class LevelDbUtils
	{
		public static string HexDump(byte[] bytes, int bytesPerLine = 16, bool printLineCount = false, bool printText = true, bool cutAfterFive = false)
		{
			StringBuilder sb = new StringBuilder();
			for (int line = 0; line < bytes.Length; line += bytesPerLine)
			{
				if (cutAfterFive && line >= bytesPerLine*5)
				{
					sb.AppendLine(".. output cut after 5 lines");
					break;
				}

				byte[] lineBytes = bytes.Skip(line).Take(bytesPerLine).ToArray();
				if (printLineCount) sb.AppendFormat("{0:x8} ", line);
				sb.Append(string.Join(" ", lineBytes.Select(b => b.ToString("x2"))
						.ToArray())
					.PadRight(bytesPerLine*3));
				if (printText)
				{
					sb.Append(" ");
					sb.Append(new string(lineBytes.Select(b => b < 32 ? '.' : (char) b)
						.ToArray()));
				}
				if (bytesPerLine < bytes.Length)
					sb.AppendLine();
			}

			return sb.ToString();
		}
	}

	public static class LevelDbHelpers
	{
		public static Span<byte> UserKey(this Span<byte> fullKey)
		{
			return fullKey.Slice(0, fullKey.Length - 8);
		}

		public static Span<byte> UserKey(this byte[] fullKey)
		{
			return fullKey.AsSpan(0, fullKey.Length - 8);
		}

		public static string ToHexString(this Span<byte> bytes)
		{
			return bytes.ToArray().HexDump(bytes.Length, cutAfterFive: true, printText: false, printLineCount: false);
		}

		public static string ToHexString(this ReadOnlySpan<byte> bytes)
		{
			return bytes.HexDump(bytes.Length, cutAfterFive: true, printText: false, printLineCount: false);
		}

		public static string HexDump(this byte[] value, int bytesPerLine = 16, bool printLineCount = false, bool printText = true, bool cutAfterFive = false)
		{
			return LevelDbUtils.HexDump(value, bytesPerLine, printLineCount, printText, cutAfterFive);
		}

		public static string HexDump(this ReadOnlySpan<byte> value, int bytesPerLine = 16, bool printLineCount = false, bool printText = true, bool cutAfterFive = false)
		{
			return LevelDbUtils.HexDump(value.ToArray(), bytesPerLine, printLineCount, printText, cutAfterFive);
		}
	}
}