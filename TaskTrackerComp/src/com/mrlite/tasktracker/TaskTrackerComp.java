package com.mrlite.tasktracker;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;
import java.util.ArrayList;

import mrlite.io.Reader;
import mrlite.io.Writer;
import mrlite.mapred.MapRed;

import shared.Message;
import shared.NodeStateBuilder;

//import dalvik.system.DexClassLoader;
//import android.app.Activity;
//import android.os.Bundle;

public class TaskTrackerComp {
	/** Called when the activity is first created. */
	static int blockSize = 13107200 * 2;
	static Socket TaskNodeSocket = null;
	static ServerSocket ReducerSocket = null;
	
	static public class Loader extends ClassLoader {
		public Loader() {
		}

		public Class<?> DefineClass(String name, byte[] buff, int off, int len) {
			return defineClass(name, buff, off, len);
		}
	}

	static String MapInpStr = null;
	private static Socket HeartBeatSocket;
	private static Object lock = new Object();

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
				System.out.println("thread start");
				reader.remoteCopy(socket, copyTo, 10485760 / 4);
				closed++;
				System.out.println("thread ends");
				// synchronized (lock) {
				// if (closed==total) lock.notify();
				// }

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	static class AddNodeThread extends Thread {

		public int noOfNode = 0;

		public void run() {
			try {
				Thread.sleep(1000);
				MapInpStr = null;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	static class HeartBeatThread extends Thread {

		HeartBeatThread() {
		}

		public void run() {
			OutputStream os = null;
			try {
				// os = new
				// ObjectOutputStream(HeartBeatSocket.getOutputStream());
				os = HeartBeatSocket.getOutputStream();
			} catch (IOException e) {
				// TODO Auto-generated catch block

				e.printStackTrace();
				return;

			}
			while (true) {
				try {
					Thread.sleep(1000);
					os.write(NodeStateBuilder.getStats().getBytes());

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

	// static String nameNodeIP="192.168.1.101";
	static String nameNodeIP = "127.0.0.1";

	public static void main(String[] arg) throws IOException {
		// super.onCreate(savedInstanceState);
		// setContentView(R.layout.main);
		File confFile = new File("tasktrackerConfig");
		FileInputStream conf = new FileInputStream(confFile);
		byte[] readConf = new byte[(int) confFile.length()];
		conf.read(readConf);
		String[] readConfArr = (new String(readConf).split("\n"));

		String root = readConfArr[1];
		nameNodeIP = readConfArr[0];
		String buffSizeStr = "52428800";
		int buffSize = Integer.parseInt(buffSizeStr) / 2;

		Message taskMsg = null;
		// String root = "/home/hadoop/wokspace/FYP1-98/MDFSDataNodeComp/MDFS/";
		// String root = "/home/zohair/workspace/MDFSDataNodeComp/MDFS";

		if (arg.length > 0)
			root = root + arg[0] + "/";

		int port = 9998;
		TaskNodeSocket = new Socket(nameNodeIP, port);
		HeartBeatSocket = new Socket(nameNodeIP, port + 1);
		InputStream is = TaskNodeSocket.getInputStream();
		HeartBeatThread hbt = new HeartBeatThread();
		hbt.start();
		while (true) {
			try {
				ObjectInputStream oib = new ObjectInputStream(is);
				// OutputStream out = TaskNodeSocket.getOutputStream();
				taskMsg = (Message) oib.readObject();
				if (taskMsg.taskType == 1)
					ReducerSocket = new ServerSocket(6000);
				// System.out.println("here");
				byte[] buffer = new byte[500];
				int len;
				File mdfs = new File(root);
				if (!mdfs.isDirectory())
					mdfs.mkdirs();
				String jarPath = root + taskMsg.taskName;

				System.out.println(jarPath);
				// (new File(jarPath)).createNewFile();
				FileOutputStream fos = new FileOutputStream(jarPath);
				// BufferedInputStream is2 = new BufferedInputStream(is);
				while ((len = is.read(buffer)) != -1) {

					fos.write(buffer, 0, len);

					if (len < 500)
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
				// DexClassLoader classLoader = new DexClassLoader(
				// jarPath, "/sdcard", null, getClass().getClassLoader());
				// Class<?> cl = classLoader.loadClass(taskMsg.className);

				// Class<?> cl= (new Loader()).DefineClass(taskMsg.className,
				// JarData, 0, (int)JarData.length);

				URLClassLoader clazzLoader;
				Class<?> clazz;
				// try {
				// ClassLoaderUtil.addFile(filePath);
				String jarPath2 = jarPath;
				URL url = new URL("jar:" + (new File(jarPath2)).toURL() + "!/");
				clazzLoader = new URLClassLoader(new URL[] { url });
				clazz = clazzLoader.loadClass(taskMsg.className);

				MapRed z = (MapRed) clazz.newInstance();

				// zoh z=
				// (zoh)cl.newInstance();

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

					z.mapperReader.addBlock(stBlock, stOffset, (new File(
							stBlock)).length());
					z.mapperReader.addBlock(edBlock, 0, edOffset);
					z.mapperReader.setBufferSize(buffSize);
					z.mapperReader.setDelimiter("\n");
					z.mapperReader.init();
					String writeTo = inpPath + taskMsg.startBlock + ".mapout";
					z.mapperWriter.setFile(writeTo);
					z.mapperWriter.setBufferSize(buffSize);
					/*
					 * byte[] finalMapInp = new byte[taskMsg.taskSize]; int
					 * sread = 0; int length = taskMsg.inputFile.length(); if
					 * (length > 0) { if
					 * (taskMsg.startBlock.equals(taskMsg.endBlock) == false) {
					 * int len1; FileInputStream block1 = new
					 * FileInputStream(inpPath + taskMsg.startBlock);
					 * block1.skip((int) taskMsg.startOff); sread = len1 =
					 * block1.read(finalMapInp); FileInputStream block2 = new
					 * FileInputStream(inpPath + taskMsg.endBlock); sread +=
					 * block2.read(finalMapInp, len1, taskMsg.endOff); } else {
					 * FileInputStream block1 = new FileInputStream(inpPath +
					 * taskMsg.startBlock); block1.skip((int) taskMsg.startOff);
					 * sread = block1.read(finalMapInp, 0, taskMsg.endOff -
					 * taskMsg.startOff); } MapInpStr = new String(finalMapInp,
					 * 0, sread); finalMapInp=null; //
					 * System.out.print(MapInpStr); // //mapper//// }
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
					SocketAddress addr = new InetSocketAddress(
							taskMsg.reducerAddr, 6000);
					System.out.println(taskMsg.reducerAddr);
					redsoc.connect(addr, 0);
					System.out.println(taskMsg.reducerAddr);
					z.mapperWriter.remoteSend(redsoc, 10485760);
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
					z.reducerReader.setDelimiter("\n");
					z.reducerReader.setBufferSize(buffSize);
					// z.reducerReader.setDelimiter("\n");
					for (int i = 0; i < taskMsg.noOfMaps; i++) {
						Socket sock = ReducerSocket.accept();
						String[] starr = taskMsg.inputFile.split("/");
						String Dir = "";
						for (int x = 0; x < starr.length - 1; x++) {
							Dir += starr[x];
						}
						(new File(Dir)).mkdirs();
						mapThreads[i] = new Mappers(sock, z.reducerReader,
								taskMsg.inputFile + ".map" + i);
						mapThreads[i].start();
						/*
						 * InputStream sis = sock.getInputStream(); byte[]
						 * buff=new byte[taskMsg.taskSize];
						 * //System.out.println(taskMsg.taskSize); int
						 * buffreaded=0; byte[] buff2=null;
						 * while((len=sis.read(buff))!=-1) { totalread+=len;
						 * buffreaded+=len; buff2= new byte[len];
						 * //bytesArr.add(buff); System.arraycopy(buff, 0,
						 * buff2, 0, buff2.length); bytesArr.add(buff2); String
						 * otpt=new String(buff2);
						 * if(i==1)System.out.println(otpt); } buff=null;
						 * buff2=null; System.out.println("got");
						 */
					}
					for (int i = 0; i < taskMsg.noOfMaps; i++) {
						mapThreads[i].join();
					}
					// synchronized (lock) {
					// lock.wait();
					// }
					z.reducerReader.addBlock(null, 0, 0);

					z.reducerReader.init();
					z.reducerWriter.setDelimiter("\n");

					z.reducerWriter.setFile(taskMsg.inputFile);
					/*
					 * byte[] finalinput = new byte[totalread]; for (int i = 0,
					 * pos = 0; i < bytesArr.size(); i++) {
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
					SocketAddress addr= new InetSocketAddress(nameNodeIP, 9998 + 10);
					Socket toJT = new Socket();
					
					toJT.connect(addr);
					z.reducerWriter.remoteSend(toJT, 10485760);
					toJT.close();
					// out2.flush();
					Thread.sleep(4000);
					ReducerSocket.close();
				}

				// TaskNodeSocket.close();

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
}
