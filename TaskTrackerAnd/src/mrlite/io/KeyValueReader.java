package mrlite.io;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.StringTokenizer;

import mrlite.datatypes.LongWritable;
import mrlite.datatypes.Writable;

public class KeyValueReader<K extends Writable,V extends Writable> extends Reader{
	public String keyValueDelimiter;
	//public String recordDelimiter;
	public int sorting;
	public String valueValueDelimiter;
	public String keyKeyDelimiter;
	public ArrayList<RandomAccessFile> InputFileArray;
	public ArrayList<StringTokenizer>  buffers;
	public int bufferSizeEach;
	public Class<K> classOfKey;
	public Class<V> classOfValue;
	K getNewKeyInstance() throws InstantiationException, IllegalAccessException
	{
		return (K) classOfKey.newInstance();
	}
	
	V getNewValueInstance() throws InstantiationException, IllegalAccessException
	{
		return (V) classOfValue.newInstance();
	}
	
	public KeyValueReader(Class<K> classK,Class<V> classV )
	{
		classOfKey=classK;
		classOfValue=classV;
		
	}
	
	
	public boolean FillKeyValBuffer(int index) throws IOException
	{
		byte [] buff= new byte [bufferSizeEach];
		//if (InputFileArray.null)
		int readFile=InputFileArray.get(index).read(buff);
		int x=readFile-1;
		long ptr=InputFileArray.get(index).getFilePointer();
		for (;x>=0;x--)
		{
			if (buff[x]==delimiter.getBytes()[0])
			{
				break;
			}
			
			ptr --;
		}
		InputFileArray.get(index).seek(ptr);
		
		String retBuff= new String(buff,0, x+1);
		buffers.set(index, new StringTokenizer(retBuff));
		buff=null;
		return true;
			
		
	}
	String []record =null;
	public KeyValuePair<K, V> mergeRead()
	{
		KeyValuePair<K, V> pairToRet=null;
		
		for (int i=0 ;i<buffers.size();i++)
		{
			if (record[i]==null) record[i] = buffers.get(i).nextToken(delimiter);
			try {
				if (record==null )
				{
					if (InputFileArray.get(i).getFilePointer()<InputFileArray.get(i).length()) 
					{
						try {
							FillKeyValBuffer(i);
						} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						}
					}
					else
					{
						
						continue;
					}
				}
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			String []keyValue= record[i].split(keyValueDelimiter);
			String key=keyValue[0];
			String val=keyValue[1];
			String [] keyList= key.split(keyKeyDelimiter);
			String [] valList= val.split(valueValueDelimiter);
			ArrayList<K> kList= new ArrayList<K>();
			ArrayList<V> vList= new ArrayList<V>();
			for (int j=0 ;j<keyList.length;j++)
			{
				try {
					K k= getNewKeyInstance() ;
					k.parse(keyList[j]);
					kList.add(k);
					
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
			for (int j=0 ;j<valList.length;j++)
			{
				try {
					V v= getNewValueInstance() ;
					v.parse(keyList[j]);
					vList.add(v);
					
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		
			KeyValuePair<K,V> thisKVP = new KeyValuePair<K, V>();
			thisKVP.key	  =kList;
			thisKVP.value =vList;			
			
			if (pairToRet == null)
			{
				pairToRet=thisKVP;
				record[i]=null;
			}
			else if(pairToRet.compareTo(thisKVP)>0 )
			{
				pairToRet=thisKVP;
				record[i]=null;
			}
			else if(pairToRet.compareTo(thisKVP)==0 )
			{
				pairToRet.value.addAll(thisKVP.value);
				record[i]=null;
			}
			
				
		};

		
		
		return pairToRet;
	}
	@Override
	public KeyValuePair<K,V> read()
	{
		return mergeRead();
	}
	@Override
	public void init()
	{
		InputFileArray= new ArrayList<RandomAccessFile>();
		buffers= new ArrayList<StringTokenizer>();
		for (int i=0;i<blockList.size()-1;i++)
		{
			try {
				RandomAccessFile raf= new RandomAccessFile(blockList.get(i).name, "r");
				InputFileArray.add(raf);
				StringTokenizer strTok= new StringTokenizer("");
				buffers.add(strTok);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		record=new String[InputFileArray.size()];
	}
}
