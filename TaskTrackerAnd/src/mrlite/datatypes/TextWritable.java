package mrlite.datatypes;

public class TextWritable  extends Writable{
	
	public TextWritable(String parseInt) {
		// TODO Auto-generated constructor stub
		data= parseInt;
	}
	@Override
	public TextWritable parse (String inp)
	{
		data=inp;
		return new TextWritable(inp);
	}

	public String getData()
	{
		return (String) data;
	}
}