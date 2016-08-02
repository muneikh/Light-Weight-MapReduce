package shared;

import java.io.IOException;
import java.io.RandomAccessFile;

import android.app.ActivityManager;
import android.app.ActivityManager.MemoryInfo;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.os.BatteryManager;
import android.util.Log;
import android.app.Activity;
import android.os.Bundle;
public class NodeStateBuilder extends Activity {
	static long AvailableMemory=20;
 public long readMemoryAvail(Activity act) {

				MemoryInfo mi = new MemoryInfo();
				ActivityManager activityManager = (ActivityManager) act.getSystemService(Context.ACTIVITY_SERVICE);

				activityManager.getMemoryInfo(mi);
				AvailableMemory = mi.availMem / 1048576L;
		//return availableMegs;
		//return Long.MAX_VALUE;

		IntentFilter filter = new IntentFilter();
        //filter.addAction(SendBroadcast.BROADCAST_ACTION);
        //registerReceiver(batteryLevelReceiver, filter);

		return 0;
	}
	
	static int BatteryLevel=10;
	static int batteryLevel() {

		
	//	level=Integer.MAX_VALUE;
		//BroadcastReceiver batteryLevelReceiver = new BroadcastReceiver() {
			
		//	@Override
		//	public void onReceive(Context context, Intent intent) {
				int level=-1;
				// TODO Auto-generated method stub
				int rawlevel = (new Intent()).getIntExtra(BatteryManager.EXTRA_LEVEL,
						-1);
				int scale = (new Intent()).getIntExtra(BatteryManager.EXTRA_SCALE, -1);
				//int plug = intent.getIntExtra(BatteryManager.EXTRA_PLUGGED, 0);

				//int level = -1;

				if (rawlevel >= 0 && scale > 0) {
					BatteryLevel=level = (rawlevel * 100) / scale;
				}
				
				String str3 = Integer.toString(level);

				 Log.e("Battery Level Percent", str3);
		//	}
	//	};		
		
		return 0;
		}
		
		
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
		
		 public String getStats(Activity act)
		{
			//HeartbeatMassages hms=new HeartbeatMassages();
			//hms.chargingAmount=batteryLevel();
			//hms.cpuAvaliable=readCPUAvail();
			//hms.memoryAvaliable=readMemoryAvail(act);
			//hms.isCharging=true;
			//hms.storageAvailable=1000000000;
			//hms.chargingAmount=BatteryLevel;
			//hms.memoryAvaliable=AvailableMemory;
			
			
			//String res="";
			//res +="2;";
			//res+=Long.toString(readCPUAvail())+";";
			//res+=Long.toString(readMemoryAvail(act))+";";
			//res+=Long.toString(1000000000)+";";
			//res+=Long.toString(BatteryLevel)+";";
			//res+=Boolean.toString(true)+";";
			//return res;
			
			String attr="";
			String res="";
			res +="2;";
			attr+=Long.toHexString(readCPUAvail());
			while (attr.length()<16)attr="0"+attr;
			res+=attr+";";
			attr="";
			attr+=Long.toHexString(readMemoryAvail(act));
			while (attr.length()<16)attr="0"+attr;
			res+=attr+";";
			attr="";
			attr+=Long.toHexString(1000000000);
			while (attr.length()<16)attr="0"+attr;
			res+=attr+";";

			attr="";
			attr+=Long.toHexString(BatteryLevel);
			while (attr.length()<8)attr="0"+attr;
			res+=attr+";";
			
			res+="1;";
			
			return res;
		}
		
}
