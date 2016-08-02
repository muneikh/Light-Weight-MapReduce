package mrlite.datatypes;

public abstract class Writable implements Comparable{
	public Comparable data;
	
	public Writable()
	{
		
	}
	public Writable(Comparable parseObj) {
		// TODO Auto-generated constructor stub
		data= parseObj;
	}
	 abstract  public Writable parse (String inp);

	@Override
	public int compareTo(Object arg0) {
		// TODO Auto-generated method stub
		return data.compareTo(arg0);
	}
	public Object getData()
	{
		return data;
	}
	
	@Override
	public String toString()
	{
		return data.toString();
	}
}
