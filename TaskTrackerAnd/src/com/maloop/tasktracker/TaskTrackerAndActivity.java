/*
 * job PlainApp.dex PlainApp Dirx/Zohair.txt  output1
 */

package com.maloop.tasktracker;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import shared.Message;
import shared.NodeStateBuilder;
//import shared2.zoh;
import mrlite.io.Reader;
import mrlite.mapred.*;

import dalvik.system.DexClassLoader;

import android.app.Activity;
import android.os.Bundle;
import android.os.Looper;
import android.util.Log;

public class TaskTrackerAndActivity extends Activity {
	/** Called when the activity is first created. */
	static int blockSize = 200;
	static Socket TaskNodeSocket = null;
	static ServerSocket ReducerSocket = null;
	static private Socket HeartBeatSocket;
	static String JobTrackerIp = "192.168.43.6";

	static class HeartBeatThread extends Thread {
		Activity activity;

		HeartBeatThread(Activity act) {
			activity = act;
		}

		public void run() {
			Looper.prepare();
			OutputStream os = null;
			try {
				os = HeartBeatSocket.getOutputStream();
			} catch (IOException e) {
				// TODO Auto-generated catch block

				e.printStackTrace();
				return;

			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			while (true) {
				try {
					Thread.sleep(1000);
					os.write((new NodeStateBuilder()).getStats(activity)
							.getBytes());

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

	}

	static class Mappers extends Thread {
		public static int closed = 0;
		public static int total = 0;
		public Socket socket;
		public Reader reader;
		public String copyTo;

		public Mappers(Socket s, Reader r, String c) {
			socket = s;
			reader = r;
			copyTo = c;
		}

		public void run() {

			try {

				reader.remoteCopy(socket, copyTo, 10485760 / 4);
				closed++;
				System.out.println("thread ends: "+copyTo);
				// synchronized (lock) {
				// if (closed==total) lock.notify();
				// }

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	static int buffSize = 5242880 / 2;

	@Override
	public void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.main);
		Message taskMsg = null;
		String root = "/sdcard/MDFS/";

		try {

			int port = 9998;
			TaskNodeSocket = new Socket(JobTrackerIp, port);
			HeartBeatSocket = new Socket(JobTrackerIp, port + 1);
			HeartBeatThread hbt = new HeartBeatThread(this);
			hbt.start();
			InputStream is = TaskNodeSocket.getInputStream();
			ObjectInputStream oib = new ObjectInputStream(is);
			OutputStream out = TaskNodeSocket.getOutputStream();

			taskMsg = (Message) oib.readObject();
			if (taskMsg.taskType == 1){
				
				ReducerSocket = new ServerSocket(6000);
			}
			// System.out.println("here");
			byte[] buffer = new byte[200];
			int len;
			File mdfs = new File(root);
			if (!mdfs.isDirectory())
				mdfs.mkdirs();
			String jarPath = root + taskMsg.taskName;
			String MapInpStr = "";
			System.out.println(jarPath);
			FileOutputStream fos = new FileOutputStream(jarPath);
			BufferedInputStream is2 = new BufferedInputStream(is);
			while ((len = is.read(buffer)) != -1) {
				// String str= new String(buffer);
				// System.out.print(str);
				// System.out.println(len);
				fos.write(buffer, 0, len);

				if (len < 200)
					break;
			}

			fos.close();

			File jarFile = new File(jarPath);

			Thread.sleep(100);
			FileInputStream fis = new FileInputStream(jarPath);
			byte[] JarData = new byte[(int) jarFile.length()];
			fis.read(JarData);
			System.out.println("here");
			// Class<?> cl= (new Loader()).DefineClass(null, JarData, 0,
			// (int)jarFile.length());
			DexClassLoader classLoader = new DexClassLoader(jarPath, root,
					null, getClass().getClassLoader());
			Class<?> cl = classLoader.loadClass(taskMsg.className);
			MapRed z = (MapRed) cl.newInstance();

			if (taskMsg.taskType == 0) {
				System.out.println("mapper");
				String inpPath = root + taskMsg.inputPath;
				String stBlock = inpPath + taskMsg.startBlock;
				String edBlock = null;
				if (taskMsg.endBlock != null)
					edBlock = inpPath + taskMsg.endBlock;
				long stOffset = taskMsg.startOff;
				long edOffset = taskMsg.endOff;
				// String buffSizeStr="52428800";
				// int buffSize=Integer.parseInt(buffSizeStr)/2;
				z.init();

				z.mapperReader.addBlock(stBlock, stOffset,
						(new File(stBlock)).length());
				z.mapperReader.addBlock(edBlock, 0, edOffset);
				z.mapperReader.setBufferSize(buffSize);
				z.mapperReader.setDelimiter("\n");
				z.mapperReader.init();
				String writeTo = inpPath + taskMsg.startBlock + ".mapout";
				z.mapperWriter.setFile(writeTo);
				z.mapperWriter.setBufferSize(buffSize);
				/*
				 * byte[] finalMapInp = new byte[taskMsg.taskSize]; int sread =
				 * 0; int length = taskMsg.inputFile.length(); if (length > 0) {
				 * if (taskMsg.startBlock.equals(taskMsg.endBlock) == false) {
				 * int len1; FileInputStream block1 = new
				 * FileInputStream(inpPath + taskMsg.startBlock);
				 * block1.skip((int) taskMsg.startOff); sread = len1 =
				 * block1.read(finalMapInp); FileInputStream block2 = new
				 * FileInputStream(inpPath + taskMsg.endBlock); sread +=
				 * block2.read(finalMapInp, len1, taskMsg.endOff); } else {
				 * FileInputStream block1 = new FileInputStream(inpPath +
				 * taskMsg.startBlock); block1.skip((int) taskMsg.startOff);
				 * sread = block1.read(finalMapInp, 0, taskMsg.endOff -
				 * taskMsg.startOff); } MapInpStr = new String(finalMapInp, 0,
				 * sread); finalMapInp=null; // System.out.print(MapInpStr); //
				 * //mapper//// }
				 */
				String MapOutput = "hitlar=1\nAdolf=3\n";
				// AddNodeThread th=new AddNodeThread();
				// th.start();
				z.mapperWriter.setDelimiter("\n");
				MapOutput = z.map(MapInpStr, taskMsg.arg);
				z.mapperWriter.flush();
				// //mapper////
				System.out.println("map done");
				Socket redsoc = new Socket();
				// System.out.println(taskMsg.reducerAddr);
				 SocketAddress addr = new
				 InetSocketAddress(taskMsg.reducerAddr, 6000);/////////// TO
				// OPEN
				//SocketAddress addr = new InetSocketAddress("10.0.2.2", 5000);
				System.out.println(taskMsg.reducerAddr);
				int tries=4;
				
				for (int t=0;t<tries;t++)
				{
					if (redsoc.isConnected()==false)
					{
						redsoc.connect(addr,2000);
					}
					else break;
				}
				System.out.println(taskMsg.reducerAddr);
				z.mapperWriter.remoteSend(redsoc, 1048576);
				System.out.println(taskMsg.reducerAddr);
				// OutputStream mapout = redsoc.getOutputStream();
				// mapout.write(MapOutput.getBytes());
				System.out.println("done");
				redsoc.close();
			} else {

				System.out.println("reducer  " + taskMsg.noOfMaps);
				// int totalread = 0;
				// ArrayList<byte[]> bytesArr = new ArrayList<byte[]>();
				Mappers[] mapThreads = new Mappers[taskMsg.noOfMaps];
				Mappers.total = taskMsg.noOfMaps;
				z.init();
				String[] filename = taskMsg.inputFile.split("/");
				z.reducerReader.setDelimiter("\n");
				z.reducerReader.setBufferSize(buffSize);
				// z.reducerReader.setDelimiter("\n");
				
				
				for (int i = 0; i < taskMsg.noOfMaps; i++) {
					System.out.println("ACEPTED PIE");
					
					Socket sock = ReducerSocket.accept();
					System.out.println("ACEPTED PIE2");
					
					String[] starr = taskMsg.inputFile.split("/");
					String Dir = "";
					for (int x = 0; x < starr.length - 1; x++) {
						Dir += starr[i];
					}
					(new File(Dir)).mkdirs();
					mapThreads[i] = new Mappers(sock, z.reducerReader, root
							+ filename[filename.length - 1] + ".map" + i);
					mapThreads[i].start();
					/*
					 * InputStream sis = sock.getInputStream(); byte[] buff=new
					 * byte[taskMsg.taskSize];
					 * //System.out.println(taskMsg.taskSize); int buffreaded=0;
					 * byte[] buff2=null; while((len=sis.read(buff))!=-1) {
					 * totalread+=len; buffreaded+=len; buff2= new byte[len];
					 * //bytesArr.add(buff); System.arraycopy(buff, 0, buff2, 0,
					 * buff2.length); bytesArr.add(buff2); String otpt=new
					 * String(buff2); if(i==1)System.out.println(otpt); }
					 * buff=null; buff2=null; System.out.println("got");
					 */
				}
				for (int i = 0; i < taskMsg.noOfMaps; i++) {
					mapThreads[i].join();
				}
				
				Log.e("ERROR", "HERE REACH");
				// synchronized (lock) {
				// lock.wait();
				// }
				z.reducerReader.addBlock(null, 0, 0);

				z.reducerReader.init();
				z.reducerWriter.setDelimiter("\n");

				z.reducerWriter.setFile(root + filename[filename.length - 1]);
				/*
				 * byte[] finalinput = new byte[totalread]; for (int i = 0, pos
				 * = 0; i < bytesArr.size(); i++) {
				 * System.arraycopy(bytesArr.get(i), 0, finalinput, pos,
				 * bytesArr.get(i).length); pos += bytesArr.get(i).length; }
				 */
				String RedInputStr = null;
				// System.out.print(RedInputStr);
				// /////reduce call///////////
				System.out.println("done");
				// z.Hello();
				String RedOutputStr = "hello";
				RedOutputStr = z.reduce(RedInputStr);
				// ////////////reduce call////////////
				System.out.println("done");
				// OutputStream out2 = TaskNodeSocket.getOutputStream();
				// out2.write(RedOutputStr.getBytes());
				z.reducerWriter.flush();
				Socket toJT = new Socket();
				SocketAddress addr=new InetSocketAddress(JobTrackerIp, 9998 + 10);
				int tries=4;
				for(int t=0;t<tries;t++)
				{
					if (toJT.isConnected()==false)
					{
						toJT.connect(addr, 2000);
					}
					else break;
				}
				z.reducerWriter.remoteSend(toJT, 1048576);
				toJT.close();
				// out2.flush();
				Thread.sleep(4000);
				ReducerSocket.close();
			}

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}