package shared;

import java.io.Serializable;
import java.util.ArrayList;

public class Message  implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 114119192163L;
	public int taskType=0;//both
	public String taskName="";//both
	public String className="";//both
	public int taskSize=0;
	public int jarSize=0;
	public String inputPath="";//map
	public String inputFile="";//map
	public String arg="";//map
	public String startBlock="";//map
	public int startOff=0;		//map
	public String endBlock="";	//map
	public int endOff=0;		//map
	public String reducerAddr="";//map
	public ArrayList<String> ListOfMaps;//red
	public int noOfMaps=0;		//red
	public Message(){
		
		ListOfMaps= new ArrayList<String>();
	}
}
