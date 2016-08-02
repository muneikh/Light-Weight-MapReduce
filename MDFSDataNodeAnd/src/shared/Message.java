package shared;
import java.io.Serializable;
public class Message implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 824810981188122L;

	public int action;
	public String Directory;
	public String Filename;
	public String blockname;
	public long blocksize;
	public Message(){}
}
