namespace MiNET.LevelDB
{
	public enum ResultState
	{
		Undefined,
		Exist,
		Deleted,
		NotFound
	}

	public ref struct ResultStatus
	{
		public ResultStatus(ResultState state, byte[] data = null)
		{
			State = state;
			Data = data;
		}

		public ResultState State { get; }

		public byte[] Data { get; }

		public static ResultStatus NotFound => new ResultStatus(ResultState.NotFound);
		public static ResultStatus Deleted => new ResultStatus(ResultState.Deleted);
	}
}