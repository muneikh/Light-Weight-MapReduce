package shared;

import java.io.Serializable;

public class HeartbeatMassages  implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -9177357856823024980L;
	public long cpuAvaliable;
	public long memoryAvaliable;
	public long storageAvailable;
	public boolean isCharging;
	public int chargingAmount;
	public byte type;
	public HeartbeatMassages()
	{
		
		cpuAvaliable=Integer.MAX_VALUE;
		memoryAvaliable=Integer.MAX_VALUE;
		storageAvailable=Integer.MAX_VALUE;
		isCharging=true;
		chargingAmount=Integer.MAX_VALUE;
		type=0;
	}
	
}
