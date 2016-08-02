package mrlite.io;


import java.util.ArrayList;

public class KeyValuePair <K,V> implements Comparable 
{
	public ArrayList<K> key;
	public ArrayList<V> value;
	public String keyValueDelimiter;
	public String recordDelimiter;
	public String valueValueDelimiter;
	public String keyKeyDelimiter;
	@Override
	public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
	

		
		for (int i=0; i<key.size();i++)
		{
			if (((Comparable)key.get(i)).compareTo((Comparable)((KeyValuePair)arg0).key.get(i))>0)
			{
				return 1;
				
			}
			else
			if (((Comparable)key.get(i)).compareTo((Comparable)((KeyValuePair)arg0).key.get(i))<0)
			{
				
				return -1;
			}
			
					
		}

		return 0;
		
	
	}
	
	@Override
	public
	String toString()
	{
		String keyStr="";
		for (int i=0; i<key.size()-1;i++)
		{
			keyStr+=key.get(i).toString()+keyKeyDelimiter;
		}
		keyStr+=key.get(key.size()-1).toString()+keyValueDelimiter;
		for (int i=0; i<value.size()-1;i++)
		{
			keyStr+=value.get(i).toString()+valueValueDelimiter;
		}
		keyStr+=key.get(key.size()-1).toString()+ recordDelimiter;
		return keyStr;
	}
	
}
