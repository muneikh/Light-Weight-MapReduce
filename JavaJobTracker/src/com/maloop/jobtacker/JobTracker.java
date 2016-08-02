package com.maloop.jobtacker;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ClassLoader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.PriorityQueue;
import java.util.concurrent.ArrayBlockingQueue;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import java.io.OutputStream;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import shared.HeartbeatMassages;
import shared.Message;

import shared2.zoh;

public class JobTracker {

	static String xmlPath = "/home/zohair/workspace/MDFSNameNode/MDFSNameNode/FilesDB.xml";
	static Document doc;
	static ServerSocket jobNodePort = null;
	static ServerSocket heartbeatPort = null;

	public static class TaskTrackers {
		public String nodeIp;
		public int port;
		public Socket socket;
		public Socket heartbeatSocket;
		public long cpuAvaliable;
		public long memoryAvaliable;
		public long storageAvailable;
		public byte isCharging;
		public int chargingAmount;
		public byte type;
		public int secondsAfterLastHeartbeat;

		public TaskTrackers(String ip, int p, Socket sock, Socket hbSock) {
			nodeIp = ip;
			port = p;
			socket = sock;
			heartbeatSocket = hbSock;
		}

		@Override
		public boolean equals(Object rhs) {
			return (((TaskTrackers) rhs).nodeIp.equals(this.nodeIp) && port == ((TaskTrackers) rhs).port);

		}
	}

	static ArrayList<TaskTrackers> taskNodes;
	static ArrayList<HeartBeatThread> listHBThreads;

	// /public static class TaskTrackerCompareable implements Comparable<T>

	static class Block {

		String blockName;
		long length;
		long firstDelimiter;

		public Block(String bName, long blkLength, long fdelimiter) {
			blockName = bName;
			length = blkLength;
			firstDelimiter = fdelimiter;

		}

		@Override
		public boolean equals(Object rhs) {
			return (this.blockName.equals(((Block) rhs).blockName));
		}

	}

	static Hashtable<TaskTrackers, ArrayList<Block>> nodesHasBlocks;
	static Hashtable<Block, ArrayList<TaskTrackers>> blocksAtNodes;

	public static class Split {

		public String startBlock = "";
		public int startOff = 0;
		public String endBlock = "";
		public int endOff = 0;
		public int tasksize = 0;

		public Split() {
		}
	}

	static class HeartBeatThread extends Thread {
		int index;
		String a = "a";
		String b = "z";

		public HeartBeatThread(int i) {

			// TODO Auto-generated constructor stub
			index = i;
		}

		public void run() {
			long lStartTime = new Date().getTime();
			HeartbeatMassages hmsg = null;
			// ObjectInputStream os=null;
			InputStream is = null;
			try {
				// os = new
				// ObjectInputStream(taskNodes.get(index).heartbeatSocket.getInputStream());
				is = taskNodes.get(index).heartbeatSocket.getInputStream();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				System.out.println("no");
				e1.printStackTrace();
				// return;
			}
			while (true) {

				try {

					// System.out.println("hello");
					// if(os.available()<=0)continue;

					// hmsg=(HeartbeatMassages) os.readObject();
					byte[] buff = new byte[64];

					int len = is.read(buff);
					String[] msgs = (new String(buff, 0, len)).split(";");

					long lEndTime = new Date().getTime();
					int timeDiff = (int) ((lEndTime - lStartTime) / 1000);
					lStartTime = lEndTime;
					// System.out.println(hmsg.cpuAvaliable);
					// System.out.println(hmsg.memoryAvaliable);
					// System.out.println(hmsg.chargingAmount);
					taskNodes.get(index).type = Byte.parseByte(msgs[0]);
					//System.out.println(taskNodes.get(index).type);
					taskNodes.get(index).cpuAvaliable = Long.parseLong(msgs[1],16);
					taskNodes.get(index).memoryAvaliable = Long
							.parseLong(msgs[2],16);
					taskNodes.get(index).storageAvailable = Long
							.parseLong(msgs[3],16);
					taskNodes.get(index).chargingAmount = Integer
							.parseInt(msgs[4],16);
					taskNodes.get(index).isCharging = Byte
							.parseByte(msgs[5]);
					taskNodes.get(index).secondsAfterLastHeartbeat = timeDiff;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}

	}

	static class AddNodeThread extends Thread // //// Tread for Accepting new
												// Connections
	{

		public int noOfNode = 0;

		public void run() {

			while (true) {
				Socket sock = null;
				try {

					TaskTrackers d = new TaskTrackers("", 0,
							jobNodePort.accept(), heartbeatPort.accept());
					String str = d.socket.getInetAddress().toString();
					String[] str2 = str.split("/");

					d.nodeIp = str2[1];
					d.port = d.socket.getPort();

					taskNodes.add(d);
					HeartBeatThread hbt = new HeartBeatThread(
							taskNodes.size() - 1);
					listHBThreads.add(hbt);
					hbt.start();
					System.out.println("accepted:" + taskNodes.size());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}

		public AddNodeThread() {

		}
	}

	static public class Loader extends ClassLoader {
		public Loader() {
		}

		public Class<?> DefineClass(String name, byte[] buff, int off, int len) {
			return defineClass(name, buff, off, len);
		}
	}

	public static void main(String[] arg) throws IOException,
			InterruptedException, SAXException, ParserConfigurationException {

		File confFile = new File("jobtrackerConfig");
		FileInputStream conf = new FileInputStream(confFile);
		byte[] readConf = new byte[(int) confFile.length()];
		conf.read(readConf);
		String readConfStr = new String(readConf);
		String[] readConfArr = readConfStr.split("\n");

		xmlPath = readConfArr[0];
		try {

			// zoh z=
			// (zoh)cl.newInstance();
			// z.Hello();

			int port = 9998;
			jobNodePort = new ServerSocket(port); // ////
			heartbeatPort = new ServerSocket(port + 1);
			taskNodes = new ArrayList<TaskTrackers>(); //
			listHBThreads = new ArrayList<HeartBeatThread>();
			blocksAtNodes = new Hashtable<Block, ArrayList<TaskTrackers>>();
			File fXmlFile = new File(xmlPath); //
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory
					.newInstance(); //
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder(); // //initializing
																		// sockets
																		// and
																		// Document
			doc = dBuilder.parse(fXmlFile);
			//
			XPath xpath = XPathFactory.newInstance().newXPath();

			NodeList blockList = (NodeList) xpath.evaluate("//block", doc,
					XPathConstants.NODESET);
			for (int i = 0; i < blockList.getLength(); i++) {
				Element blk = (Element) blockList.item(i);
				Long blkLength = Long.parseLong(blk.getAttribute("length"));
				Long firstDelim = Long.parseLong(blk
						.getAttribute("firstWhiteSpaceAt"));
				String blkName = blk.getAttribute("name");
				Block blkToHash = new Block(blkName, blkLength, firstDelim);

				NodeList blockAt = (NodeList) blk.getChildNodes();
				for (int j = 0; j < blockAt.getLength(); j++) {
					Element tt = (Element) blockAt.item(j);
					String ip = tt.getAttribute("ip");
					int prt = Integer.parseInt(tt.getAttribute("port"));
					TaskTrackers ttToHash = new TaskTrackers(ip, prt, null,
							null);
					ArrayList<TaskTrackers> ttArr = null;
					if (blocksAtNodes.containsKey(blkToHash)) {

						if ((ttArr = blocksAtNodes.get(blkToHash)) != null) {
							ttArr.add(ttToHash);
						} else {

							ttArr = new ArrayList<TaskTrackers>();
							ttArr.add(ttToHash);
							blocksAtNodes.put(blkToHash, ttArr);
						}

					} else {

						ttArr = new ArrayList<TaskTrackers>();
						ttArr.add(ttToHash);
						blocksAtNodes.put(blkToHash, ttArr);
					}

				}
			}

		} catch (XPathExpressionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String inp = null;
		AddNodeThread addnode = new AddNodeThread(); //
		addnode.start();
		int size = 0;
		System.out.println("start");
		while (size < 3) {
			size = taskNodes.size();
			Thread.sleep(100);
		}
		;// /// we set minimum 3 slave node needed to run the system. (this
			// minimum can be increase or decrease)
		// System.out.println("start");

		while (true) // //user inputs starts
		{

			try {

				inp = br.readLine();
				if (inp.equals("quit"))
					break;
				String[] inparr = inp.split(" ");
				String query = "/root/";

				if (inparr.length == 6) {
					if (inparr[0].equals("job"))
						;
					{
						startJob(inparr[1], inparr[2], inparr[3], inparr[4],
								inparr[5]);
					}
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParserConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private static void startJob(String jarPath, String className,
			String inputPath, String arg, String outputPath)
			throws SAXException, IOException, ParserConfigurationException {
		// TODO Auto-generated method stub
		File fXmlFile = new File(xmlPath); //
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance(); //
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder(); // //initializing
																	// sockets
		//long x=1111111111111111111l;															// and
				// Document
		FileInputStream readFile=new FileInputStream(jarPath);
		byte[] readFileByte=new byte[100000];
		int lenRead=readFile.read(readFileByte);
		String temp= new String(readFileByte,0,lenRead);		
		String []jarPaths= temp.split("\n");
		doc = dBuilder.parse(fXmlFile);

		StopWatch sw = new StopWatch();
		sw.start();

		String[] str2 = inputPath.split("/");
		String query = "/root";

		boolean hasInput = true;
		// Element e=null,e2=null;
		String Directory = "";
		ArrayList<Split> SplitList = new ArrayList<Split>();
		if (inputPath.length() == 0)
			hasInput = false;
		try {
			if (hasInput) {
				for (int i = 0; i < str2.length - 1; i++) {
					query = query + "/" + str2[i];
					Directory += str2[i] + "/";

				}

				query = query + "/file[@name=\"" + str2[str2.length - 1]
						+ "\"]";
				XPath xpath = XPathFactory.newInstance().newXPath();

				Element n = (Element) xpath.evaluate(query,
						doc.getDocumentElement(), XPathConstants.NODE);
				NodeList nl = n.getChildNodes();

				String startBlock = "";
				String endBlock = "";
				int startoff, endoff;
				int tasksize = 0;
				for (int i = 0; i < nl.getLength(); i++) {
					Element ele = (Element) nl.item(i);
					tasksize = Integer.parseInt(ele.getAttribute("length"));
					Split s = new Split();
					if (i == 0) {
						s.startOff = 0;

					} else {
						String val = ele.getAttribute("firstWhiteSpaceAt");
						s.startOff = Integer.parseInt(val) + 1;
						tasksize -= s.startOff;
					}
					s.startBlock = ele.getAttribute("name");
					if (i == nl.getLength() - 1) {
						s.endBlock = ele.getAttribute("name");
						String val = ele.getAttribute("length");
						s.endOff = Integer.parseInt(val);
					} else {
						Element ele2 = (Element) nl.item(i + 1);
						s.endBlock = ele2.getAttribute("name");

						String val = ele2.getAttribute("firstWhiteSpaceAt");
						tasksize += Integer.parseInt(ele2
								.getAttribute("firstWhiteSpaceAt")) + 5;
						s.endOff = Integer.parseInt(val);
					}

					s.tasksize = tasksize + 10;
					SplitList.add(s);
				}
			}

			File jarFile1 = new File(jarPaths[0]);
			File jarFile2 = new File(jarPaths[1]);
			FileInputStream fis1 = new FileInputStream(jarFile1);
			FileInputStream fis2 = new FileInputStream(jarFile2);
			byte[][] jarData = new byte[2][];
			jarData[0]=new byte[(int)jarFile1.length()];
			jarData[1]=new byte[(int)jarFile2.length()];
			fis1.read(jarData[0]);
			fis2.read(jarData[1]);
			Message taskMsg = new Message();
			taskMsg.taskType = 1;
			taskMsg.taskSize = 100000000;
			
			
			taskMsg.className = className;
			
			taskMsg.inputPath = Directory;
			taskMsg.inputFile = inputPath;
			taskMsg.arg = arg;
			String redIp = taskNodes.get(taskNodes.size() - 1).nodeIp;
			taskMsg.reducerAddr = redIp;

			taskMsg.noOfMaps = taskNodes.size() - 1;

			for (int i = 0; i < taskNodes.size() - 1; i++) {
				taskMsg.ListOfMaps.add(taskNodes.get(i).nodeIp);
			}
			String[] jarName = jarPaths[taskNodes.get(taskNodes.size() - 1).type-1].split("/");
			taskMsg.taskName = jarName[jarName.length - 1];
			taskMsg.jarSize = (int) jarData[taskNodes.get(taskNodes.size() - 1).type-1].length;
			ObjectOutputStream obo = new ObjectOutputStream(
					taskNodes.get(taskNodes.size() - 1).socket
							.getOutputStream());
			obo.writeObject(taskMsg);
			obo.flush();
			OutputStream out2 = taskNodes.get(taskNodes.size() - 1).socket
					.getOutputStream();
			Thread.sleep(10);
			out2.write(jarData[taskNodes.get(taskNodes.size() - 1).type-1]);
			out2.flush();

			taskMsg.taskType = 0;
			int distriSize = 0;
			if (hasInput)
				distriSize = SplitList.size();
			else
				distriSize = taskNodes.size() - 1;
			SplitList.get(SplitList.size() - 1).endBlock = null;
			for (int i = 0; i < distriSize; i++) {
				ObjectOutputStream obo2 = new ObjectOutputStream(
						taskNodes.get(i).socket.getOutputStream());
				if (hasInput) {
					taskMsg.startBlock = SplitList.get(i).startBlock;
					taskMsg.startOff = SplitList.get(i).startOff;
					taskMsg.endBlock = SplitList.get(i).endBlock;
					taskMsg.endOff = SplitList.get(i).endOff;
					taskMsg.taskSize = SplitList.get(i).tasksize;
				}
				jarName = jarPaths[taskNodes.get(i).type-1].split("/");
				taskMsg.taskName = jarName[jarName.length - 1];
				taskMsg.jarSize = (int) jarData[taskNodes.get(i).type-1].length;
				obo2.writeObject(taskMsg);
				obo2.flush();
				OutputStream out = taskNodes.get(i).socket.getOutputStream();
				Thread.sleep(10);
				out.write(jarData[taskNodes.get(i).type-1]);
				out.flush();
				int a = 1;
			}
			InputStream is = taskNodes.get(taskNodes.size() - 1).socket
					.getInputStream();
			byte[] buffer = new byte[10485760 / 4];
			int len;
			FileOutputStream fos = new FileOutputStream(outputPath);
			BufferedInputStream is2 = new BufferedInputStream(is);
			boolean stopped = false;
			ServerSocket ftrasn = new ServerSocket(9998 + 10);
			Socket red = ftrasn.accept();
			InputStream ris = red.getInputStream();
			while (ris.available() >= 0 && (len = ris.read(buffer)) != -1) {
				if (stopped == false) {
					sw.stop();
					stopped = true;
				}
				// String str= new String(buffer,0, len);
				// System.out.print(str);
				fos.write(buffer, 0, len);

			}
			red.close();
			ftrasn.close();

			fos.close();

			System.out.println("elapsed time in milliseconds: "
					+ sw.getElapsedTime());

		} catch (XPathExpressionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
