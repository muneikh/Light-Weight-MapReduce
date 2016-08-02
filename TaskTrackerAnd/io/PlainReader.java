package mrlite.io;

public class PlainReader extends Reader{
	@Override
	public String read()
	{
		return (String) getRecord();
	}
}
