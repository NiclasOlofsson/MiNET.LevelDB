using System;
using System.IO;
using System.Linq;
using System.Text;

namespace MiNET.LevelDB
{
	public class PrintUtils
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

		public static ulong ReadVarint(Stream sliceInput)
		{
			ulong result = 0;
			for (int shift = 0; shift <= 63; shift += 7)
			{
				ulong b = (ulong) sliceInput.ReadByte();

				// add the lower 7 bits to the result
				result |= ((b & 0x7f) << shift);

				// if high bit is not set, this is the last byte in the number
				if ((b & 0x80) == 0)
				{
					return result;
				}
			}
			throw new Exception("last byte of variable length int has high bit set");
		}
	}

	public static class PrintHelpers
	{
		public static string HexDump(this byte[] value, int bytesPerLine = 16, bool printLineCount = false, bool printText = true, bool cutAfterFive = false)
		{
			return PrintUtils.HexDump(value, bytesPerLine, printLineCount, printText, cutAfterFive);
		}

		public static ulong ReadVarint(this Stream sliceInput)
		{
			return PrintUtils.ReadVarint(sliceInput);
		}
	}
}