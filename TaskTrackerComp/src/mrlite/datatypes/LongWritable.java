package mrlite.datatypes;

public class LongWritable  extends Writable{
	
	public LongWritable(Long parseInt) {
		// TODO Auto-generated constructor stub
		data= parseInt;
	}
	@Override
	public LongWritable parse (String inp)
	{
		data=Long.parseLong(inp);
		return new LongWritable((Long)data);
	}

	public Long getData()
	{
		return (Long) data;
	}
}