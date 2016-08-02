package mrlite.mapred;
import mrlite.io.Reader;
import mrlite.io.PlainReader;
import mrlite.io.Writer;
import mrlite.io.PlainWriter;
public abstract class MapRed {

	///reader for mapper
	///reader for reducer
	public Reader mapperReader=null;
	public Reader reducerReader=null;
	public Writer mapperWriter=null;
	public Writer reducerWriter=null;
	
	public abstract String map(String inp,String arg);
	public abstract String reduce(String inp);
	public void init()
	{
		mapperReader  = new PlainReader();
		reducerReader = new PlainReader();
		mapperWriter  = new PlainWriter();
		reducerWriter = new PlainWriter();
	}
/*	
	final public void initMapReader(String startBlock,long startOffset,String endBlock,long endOffset, int bufferSize , String delimiter)
	{
		
	}
	
	final public void initMapWriter()
	{
		
	}
	
	final public void initReduceReader(String startBlock,long startOffset,String endBlock,long endOffset, int bufferSize , String delimiter)
	{
		
	}
	
	final public void initReduceWriter()
	{
		
	}

*/	
}
