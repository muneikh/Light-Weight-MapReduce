package shared;

import java.io.IOException;
import java.io.RandomAccessFile;

public class NodeStateBuilder {

	static public long readMemoryAvail() {
	/*	MemoryInfo mi = new MemoryInfo();
		ActivityManager activityManager = (ActivityManager) getSystemService(ACTIVITY_SERVICE);
		activityManager.getMemoryInfo(mi);
		long availableMegs = mi.availMem / 1048576L;
		return availableMegs;*/
		return Long.MAX_VALUE;
	}
	
	static int batteryLevel() {

		int level=-1;
		level=Integer.MAX_VALUE;
		/*BroadcastReceiver batteryLevelReceiver = new BroadcastReceiver() {
			public void onReceive(Context context, Intent intent) {
				context.unregisterReceiver(this);

				

				int rawlevel = intent.getIntExtra(BatteryManager.EXTRA_LEVEL,
						-1);
				int scale = intent.getIntExtra(BatteryManager.EXTRA_SCALE, -1);
				//int plug = intent.getIntExtra(BatteryManager.EXTRA_PLUGGED, 0);

				//int level = -1;

				if (rawlevel >= 0 && scale > 0) {
					level = (rawlevel * 100) / scale;
				}
				return level;
				String str3 = Integer.toString(level);

				 Log.e("Battery Level Percent", str3);

			}*/
		return level;
		};
		
		
		static public long readCPUAvail() {
			try {
				RandomAccessFile reader = new RandomAccessFile("/proc/stat", "r");
				String load = reader.readLine();
				String[] toks = load.split(" ");
				long idle1 = Long.parseLong(toks[5]);
				long cpu1 = Long.parseLong(toks[2]) + Long.parseLong(toks[3])
						+ Long.parseLong(toks[4]) + Long.parseLong(toks[6])
						+ Long.parseLong(toks[7]) + Long.parseLong(toks[8]);
				return  idle1;
			/*	try {
					Thread.sleep(360);
				} catch (Exception e) {
				}
				reader.seek(0);
				load = reader.readLine();
				reader.close();
				toks = load.split(" ");
				long idle2 = Long.parseLong(toks[5]);
				long cpu2 = Long.parseLong(toks[2]) + Long.parseLong(toks[3])
						+ Long.parseLong(toks[4]) + Long.parseLong(toks[6])
						+ Long.parseLong(toks[7]) + Long.parseLong(toks[8]);
				return (float) (cpu2 - cpu1) / ((cpu2 + idle2) - (cpu1 + idle1));*/
			} catch (IOException ex) {
				return Long.MAX_VALUE;
				//ex.printStackTrace();
			}
			//return Long.MAX_VALUE;
		}
		
		static public String getStats()
		{
			//HeartbeatMassages hms=new HeartbeatMassages();
			//hms.chargingAmount=batteryLevel();
			//hms.cpuAvaliable=readCPUAvail();
			//hms.memoryAvaliable=readMemoryAvail();
			//hms.isCharging=true;
		//	hms.storageAvailable=1000000000;
			String attr="";
			String res="";
			res +="1;";
			attr+=Long.toHexString(readCPUAvail());
			while (attr.length()<16)attr="0"+attr;
			res+=attr+";";
			attr="";
			attr+=Long.toHexString(readMemoryAvail());
			while (attr.length()<16)attr="0"+attr;
			res+=attr+";";
			attr="";
			attr+=Long.toHexString(1000000000);
			while (attr.length()<16)attr="0"+attr;
			res+=attr+";";

			attr="";
			attr+=Long.toHexString(batteryLevel());
			while (attr.length()<8)attr="0"+attr;
			res+=attr+";";
			
			res+="1;";
			return res;
		}
		
}
