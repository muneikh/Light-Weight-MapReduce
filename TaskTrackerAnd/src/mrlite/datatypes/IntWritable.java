package mrlite.datatypes;

public class IntWritable extends Writable{
	
	public IntWritable(Integer parseInt) {
		// TODO Auto-generated constructor stub
		data= parseInt;
	}
	@Override
	public IntWritable parse (String inp)
	{
		data=Integer.parseInt(inp);
		return new IntWritable((Integer)data);
	}

	public Integer getData()
	{
		return (Integer) data;
				
	}
}
