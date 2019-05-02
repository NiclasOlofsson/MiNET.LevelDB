namespace MiNET.LevelDB
{
	public enum ResultState
	{
		Undefined,
		Exist,
		Deleted,
		NotFound
	}

	public struct ResultStatus
	{
		public ResultState State { get; }
		public byte[] Data { get; }

		public ResultStatus(ResultState state, byte[] data = null)
		{
			State = state;
			Data = data;
		}

		public static ResultStatus NotFound => new ResultStatus(ResultState.NotFound);
		public static ResultStatus Deleted => new ResultStatus(ResultState.Deleted);
	}
}