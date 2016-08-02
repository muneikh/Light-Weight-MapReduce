package com.maloop.namenode;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.Console;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.Object;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.*;
import javax.xml.xpath.XPathExpression;

import org.w3c.dom.*;
import org.w3c.dom.xpath.*;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import shared.Message;

import com.sun.imageio.plugins.common.InputStreamAdapter;
import com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl;
public class NameNode {

public static class DataNodeInfo{
	public String nodeIp;
	public int port;
	public Socket socket;
	public DataNodeInfo(String ip,int p, Socket sock){
		nodeIp=ip;
		port=p;
		socket=sock;
	}
}



static ArrayList<DataNodeInfo> dataNodes; ///// LIst of All DataNodes(slave)
static ServerSocket NameNodePort;
static int blockSize=13107200*4; /// a blocks size
static Document doc; //// Xml Document


static class AddNodeThread extends Thread   ////// Tread for Accepting new Connections
{
	
	public int noOfNode=0;
	public void run() {
		
		while (true){
			Socket sock=null;
			try {
			
				DataNodeInfo d= new DataNodeInfo("",0,NameNodePort.accept());
				String str=d.socket.getInetAddress().toString();
				String[] str2=str.split("/");
				
				d.nodeIp=str2[1];
				d.port=d.socket.getPort();
				dataNodes.add(d);
				System.out.println("accepted:"+dataNodes.size());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			}
    }
	public AddNodeThread()
	{
		
		
	}
}

/*For deleting a Slave Node from the System*/
 static void deleteDataNode(int ind)throws XPathExpressionException, TransformerException 
{
	String chk= "/root//at[ip=\""+dataNodes.get(ind).nodeIp+
								"\" and port=\""+Integer.toString(dataNodes.get(ind).port)+"\"]";
	XPath xpath = XPathFactory.newInstance().newXPath();
	NodeList nl=(NodeList) xpath.evaluate(chk,doc.getDocumentElement(),XPathConstants.NODESET);
	if (nl==null)return;
	Element e=null;
	ArrayList<Element> toDelete = new ArrayList<Element>();
	while((e=(Element) nl.item(ind))!=null)
	{
		toDelete.add(e);
	}
	
	for (int i=0;i<toDelete.size();i++)
	{
		Element p= (Element) toDelete.get(i).getParentNode();
		p.removeChild(toDelete.get(i));
		
	}
}

 
 /* below Function is for pushing A file to File system Parameters are 
 'path'= path of file to push. 
 'to' = Location in the file system where file will be push.
 'copies' = level of redundancy;
 */
static int pushFile(String path,String to,int copies) throws  XPathExpressionException, TransformerException, InterruptedException, IOException
{
	if (dataNodes.isEmpty()) return 0;                                           //// 
	File fl= new File(path);													   //
	FileInputStream is=null;													   //	
																				   //	
		is = new FileInputStream(fl);                                              //
																	   //
	int flSize=(int) fl.length();												   //	
	int numOfBlocks=flSize/blockSize +((flSize%blockSize>0)?1:0);                  //////// breaking files into blocks
	byte [][] buff= new byte[numOfBlocks][] ;                                      //
	long [] blockOffsetTable=new long [numOfBlocks];//
	
	long blkplus=0;
	for(int i=0;i<numOfBlocks;i++,blkplus+=blockSize)
	{
		blockOffsetTable[i]=blkplus;
	}
	
/*	int x=0;                                                                       //
	for (;x<flSize/blockSize;x++)                                                  //
	{                                                                              //
		buff[x]=new byte[blockSize];                                                     //
		try {                                                                      //
			is.read(buff[x],0,blockSize);                                          //  
		} catch (IOException e1) {                                                 //
			// TODO Auto-generated catch block                                     //
			e1.printStackTrace();                                                  //
		}                                                                          // 
	}                                                                            ////
	if(flSize%blockSize>0)
	{
		buff[x]=new byte[flSize%blockSize];
		try {
			is.read(buff[x],0,flSize%blockSize);
			//buff[x][flSize%blockSize]=26;
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	System.out.println(buff.length);
*/
	String[] str=path.split("/");    ////for the file name at last index of str
	String[] str2=to.split("/");     //// for separating folder names (to put on xml)
	String chk="/root";
	
	
	
	boolean flg=false;
	Element e=null,e2=null;
	String strchk ="/root/"+to+"/file[@name=\""+str[str.length-1]+"\"]"; 
	XPath xpath = XPathFactory.newInstance().newXPath();
	flg=(Boolean) xpath.evaluate(strchk,doc.getDocumentElement(),XPathConstants.BOOLEAN);
	if (flg==true)
	{
		return -1;
	}
	for(int i=0;i<str2.length;i++)                                                            ////
	{																							//
		chk=chk+"/"+str2[i];																	//
																								//
																								//
																								//
		flg=(Boolean) xpath.evaluate(chk,doc.getDocumentElement(),XPathConstants.BOOLEAN);		//	
		if (flg==false)																			//
		{																						//
																								/////// for adding file info in the xml
			NodeList nl=null;																	//
			if (i>0)																			//
				nl=doc.getElementsByTagName(str2[i-1]);											//		
			else																				//
				nl=doc.getElementsByTagName("root");											//
			e=(Element) nl.item(0);																//
			e2=doc.createElement(str2[i]);														//
			e.appendChild(e2);																	//
																								//
		}																					//////	
		flg =(Boolean) xpath.evaluate(chk,doc.getDocumentElement(),XPathConstants.BOOLEAN);
		
		
	}
	e=(Element)xpath.evaluate(chk,doc.getDocumentElement(),XPathConstants.NODE);
	String delimiter="\n";
	e2=doc.createElement("file");
	Attr at=doc.createAttribute("name"); 
	at.setNodeValue(str[str.length-1]);
	e2.setAttribute("name", str[str.length-1]);
	e2.setAttribute("recordDelimiter", delimiter);
	e2.setAttribute("size", Long.toString(fl.length()));
	
	e.appendChild(e2);
	
	//for(int k=0;k<copies;k++)//// loop for making redundancies
	//{
		int dnsize=dataNodes.size();
		System.out.println("no. of datanodes"+ dataNodes.size());
		for (int i=0,j=0;i<numOfBlocks;i++,j=(j+1)%dnsize) /// loop for sending blocks to slave nodes and adding their location info in xml
		{
		
		
			for (int k=0,l=i;k<copies;k++,l=(l+1)%numOfBlocks) /// loop for sending blocks to slave nodes and adding their location info in xml
			{
					Thread.sleep(50);
					Socket s=null;
					
					OutputStream out=dataNodes.get(j).socket.getOutputStream();
		
		 
					Message msg= new Message();// message for making the slave node ready to receive the block
					msg.action=0; // order to receive the block (se also Message.java in package 'shared')
		
					msg.Filename=str[str.length-1];
					msg.blockname=msg.Filename+".mpart"+l;
					//msg.blocksize=buff[l].length;
					if (l<numOfBlocks-1)msg.blocksize=blockSize;
					else
						msg.blocksize=fl.length()-blockOffsetTable[l];
					Element e3=null;
					String st=chk+"/file[@name=\""+msg.Filename+"\"]/block[@name=\""+msg.blockname+"\"]";////
					e3=(Element)xpath.evaluate(st,doc.getDocumentElement(),XPathConstants.NODE);		   //	
					boolean need=false;																	   //
																							   //
					if (e3==null)																		   //
					{																				   //
						e3= doc.createElement("block");                                                    //
						need=true;                                                                         ////// Putting all blocks info and its locations in xml file 
					}                                                                                  //
					e3.setAttribute("name", msg.blockname);                                                //
					e3.setAttribute("offset", Integer.toString(l*blockSize));                              // 
					e3.setAttribute("length", Long.toString(msg.blocksize));
		
					int firstw=0;
					RandomAccessFile blkdelimoff= new RandomAccessFile(fl,"r");
					blkdelimoff.seek(blockOffsetTable[l]);
					while((blkdelimoff.read()!=(int)delimiter.getBytes()[0]))
					{
						firstw++;
					}
					e3.setAttribute("firstWhiteSpaceAt", Integer.toString(firstw));//
					String atstr="primery";
					if (k>0)atstr="at";
					Element e4=doc.createElement(atstr);                                                    //
					e4.setAttribute("ip", dataNodes.get(j).nodeIp);                                        //
					e4.setAttribute("port", Integer.toString(dataNodes.get(j).port));                      //
					e3.appendChild(e4);                                                                 /////
					if (need)e2.appendChild(e3);
					msg.Directory=to;
					try {
						//Thread.currentThread();
						Thread.sleep(100);  ////Necessary when serialized object is sent many times in loop 
			
						ObjectOutputStream oos=new ObjectOutputStream (dataNodes.get(j).socket.getOutputStream());  //
						
						oos.writeObject(msg);								  ///// sending order message			
						oos.flush();
						
						BufferedOutputStream bout=new BufferedOutputStream(out);//
						int off=0;
						int len =500;
						int sent=0;
			//do{
						//System.out.println("lenth of block;"+buff[l].length);
				//if((buff[l].length-off)>=500){
						
						RandomAccessFile raf=new RandomAccessFile(fl, "r");
						raf.seek(blockOffsetTable[l]);
						Socket fts=NameNodeFTPort.accept();
						byte []rafbuff=new byte[50000000];
						while(!(raf.getFilePointer()>=fl.length()) && !(l<numOfBlocks-1 && raf.getFilePointer()>=blockOffsetTable[l+1]))
						{
							if(l<numOfBlocks-1 && raf.getFilePointer()>=blockOffsetTable[l+1]) break;
							long startPtr=raf.getFilePointer();
							System.out.println("le "+1);
							int rafread=raf.read(rafbuff);
							System.out.println("raf "+rafread);
							if(l<numOfBlocks-1 && raf.getFilePointer()>=blockOffsetTable[l+1])
							{
							rafread=(int) (blockOffsetTable[l+1]-startPtr);
							}
							else
							if(raf.getFilePointer()>=fl.length()) 
							{
								rafread=(int) (fl.length()-startPtr);
							}
							
							fts.getOutputStream().write(rafbuff,0,rafread);//,off,500);off+=500;
							fts.getOutputStream().flush();
						}
						
						fts.close();
						raf.close();
					//}
					//else if(buff[l].length-1!=off)bout.write(buff[l],off,buff[l].length-off);
						out.flush();///// sending block	
					//	
						//dataNodes.get(j).socket.shutdownOutput();
						//out.close();
						//out.write(0);
						//out.flush();
			//}
			//while((buff[l].length>off));
			
			//out.flush();  		
						Thread.sleep(10);//
					} catch (IOException e1) {
				//		deleteDataNode(j);
				//		j--;
				//		dnsize=dataNodes.size();
			// TODO Auto-generated catch block
						e1.printStackTrace();
					}
	}
		
	//}
	}
	NodeList list = doc.getElementsByTagName("root");					////
	Node node = list.item(0);											  //	
	DOMSource source = new DOMSource(node);                               //
	File xmlF=new File(xmlPath);                                    //
	StreamResult result = new StreamResult(xmlF);                         //////  to save changes in Xml File
	TransformerFactory tFactory = TransformerFactory.newInstance();       //
    Transformer transformer = tFactory.newTransformer();                  //
	transformer.transform(source, result);                              ////
	System.out.println("Created!");
	return 0;
	
}
/*make Directory in the File system. (just required to be added in xml other nodes will 
 				create it when the first file is written in this directory)*/
static int makeDir(String to) throws XPathExpressionException, TransformerException
{
	String[] str2=to.split("/");
	String chk="/root";
	
	
	
	boolean flg=false;
	Element e=null,e2=null;
	String strchk ="/root";
	if (to!="")
		strchk ="/root/"+to; 
	XPath xpath = XPathFactory.newInstance().newXPath();
	flg=(Boolean) xpath.evaluate(strchk,doc.getDocumentElement(),XPathConstants.BOOLEAN);
	if (flg==true)
	{
		return -1;
	}
	for(int i=0;i<str2.length;i++)															/////
	{                                                                                          //
		chk=chk+"/"+str2[i];                                                                   //
		                                                                                       //  
		                                                                                       //
		                                                                                       //
		flg=(Boolean) xpath.evaluate(chk,doc.getDocumentElement(),XPathConstants.BOOLEAN);     // 
		if (flg==false)                                                                        //  
		{                                                                                      ////// adding each folder one by one on xml
			System.out.println(chk+"  "+flg);                                                  //
			NodeList nl=null;                                                                  //
			if (i>0)																		   //	
				nl=doc.getElementsByTagName(str2[i-1]);                                        //
			else                                                                               //
				nl=doc.getElementsByTagName("root");                                           // 
			e=(Element) nl.item(0);                                                            //
			e2=doc.createElement(str2[i]);                                                     //
			e.appendChild(e2);                                                              /////
			
		}
		flg =(Boolean) xpath.evaluate(chk,doc.getDocumentElement(),XPathConstants.BOOLEAN);
		System.out.println(flg);
		
	}
	NodeList list = doc.getElementsByTagName("root");					////
	Node node = list.item(0);											  //	
	DOMSource source = new DOMSource(node);                               //
	File xmlF=new File(xmlPath);                                    //
	StreamResult result = new StreamResult(xmlF);                         //////  to save changes in Xml File
	TransformerFactory tFactory = TransformerFactory.newInstance();       //
    Transformer transformer = tFactory.newTransformer();                  //
	transformer.transform(source, result);                              ////
	
	return 0;
}

static int indexOfDataNode(String ip,int port)
{
	for (int i=0 ;i<dataNodes.size();i++)
	{
		if (dataNodes.get(i).nodeIp==ip && dataNodes.get(i).port==port)
			return i;
	}
	return -1;
}

static int delFile(String to) throws  XPathExpressionException, TransformerException, IOException
{
	if (dataNodes.isEmpty()) return 0;

	String[] str2=to.split("/");
	String chk="/root";
	
	
	
	boolean flg=false;
	Element e=null,e2=null;
	for(int i=0;i<str2.length-1;i++)
	{
		chk=chk+"/"+str2[i];
		
		
	}
	String Dir="";
	for (int i=0;i<str2.length-1;i++)
	{
		Dir+=str2[i]+"/";
	}
	chk=chk+"/file[@name=\""+str2[str2.length-1]+"\"]";									////
	XPath xpath = XPathFactory.newInstance().newXPath();								  //
	Node n =(Node) xpath.evaluate(chk,doc.getDocumentElement(),XPathConstants.NODE);      //
	if (n==null)																		  //// getting the node of File specified, from xml 
	{                                                                                     //
		return -1;																		  //
	}																					////
	
	NodeList nl=n.getChildNodes();   ///// getting info of all blocks associated with the file specified
	
	int i=0;
	ArrayList<Integer> toDelete = new ArrayList<Integer>();
	while((e=(Element) nl.item(i))!=null) ///loop for traversing each block of the file in xml(<block> tag)
	{
		int j=0;
		NodeList el=e.getChildNodes();
		while((e2=(Element) el.item(j))!=null) //////loop for traversing each location of a block of the file in xml (<at> tag)
		{
			
			
			String ip=e2.getAttribute("ip");								////
			int port= Integer.parseInt(e2.getAttribute("port"));			  //
			int ind = indexOfDataNode(ip,port);								  //
			OutputStream out=dataNodes.get(ind).socket.getOutputStream();	  //
			try{															  //
																		      //
			ObjectOutputStream oos= new ObjectOutputStream(out);			  ////// order to delete the block to the location
			Message msg= new Message();										  //
			msg.action=2;													  //						
			msg.Filename=str2[str2.length-1];                                 //
			msg.blockname=e.getAttribute("name");							  //
			msg.Directory=Dir;												  //
			oos.writeObject(msg);											////
			}catch(IOException e1)
			{
				toDelete.add(ind);
				e1.printStackTrace();
			}
			j++;
		}
		i++;
	}
//for (int x=0;x<toDelete.size();x++)
	//{
	//	deleteDataNode(toDelete.get(x));
//	}
	System.out.println("Deleted!");
	Node p=n.getParentNode();											
	p.removeChild(n);
	NodeList list = doc.getElementsByTagName("root");					////
	Node node = list.item(0);											  //	
	DOMSource source = new DOMSource(node);                               //
	File xmlF=new File(xmlPath);                                    //
	StreamResult result = new StreamResult(xmlF);                         //////  to save changes in Xml File
	TransformerFactory tFactory = TransformerFactory.newInstance();       //
    Transformer transformer = tFactory.newTransformer();                  //
	transformer.transform(source, result);                              ////
	
	return 0;
	
}

/*
 * below function to retrieve the files blocks from the slave nodes and gather the data in string
 */

static String fetchFile(String to) throws  XPathExpressionException, TransformerException, IOException
{
	if (dataNodes.isEmpty()) return null;

	String[] str2=to.split("/");                            
	String chk="/root";								
	
	boolean flg=false;
	Element e=null,e2=null;
	for(int i=0;i<str2.length-1;i++)
	{
		chk=chk+"/"+str2[i];
		
		
	}
	String Dir="";
	for (int i=0;i<str2.length-1;i++)
	{
		Dir+=str2[i]+"/";
	}
	chk=chk+"/file[@name=\""+str2[str2.length-1]+"\"]";
	XPath xpath = XPathFactory.newInstance().newXPath();
	
	System.out.println(chk);
	flg=(Boolean) xpath.evaluate(chk,doc.getDocumentElement(),XPathConstants.BOOLEAN); ///Checking if file exist
	if (flg==false)
	{
		System.out.println(flg);
		return null;
		
	}
	Node n =(Node) xpath.evaluate(chk,doc.getDocumentElement(),XPathConstants.NODE);
	System.out.println(n.getNodeName());
	NodeList nl=n.getChildNodes();
	
	int i=0;
	String str="";
	
	while((e=(Element) nl.item(i))!=null) ///loop for traversing each block of the file in xml(<block> tag)
	{
		int j=0;
		NodeList el=e.getChildNodes();
		while((e2=(Element) el.item(j))!=null)//////loop for traversing each location of a block of the file in xml (<at> tag)
		{
			
			
			String ip=e2.getAttribute("ip");											////
			int port= Integer.parseInt(e2.getAttribute("port"));						  //
			int ind = indexOfDataNode(ip,port);											  //	
			InputStream is=dataNodes.get(ind).socket.getInputStream();					  //
			OutputStream out=dataNodes.get(ind).socket.getOutputStream();				  //
			try {																		  //
																					      //
			ObjectOutputStream oos= new ObjectOutputStream(out);						  //	
			Message msg= new Message();												      ////// ordering and Fetching the block 
			msg.action=1;																  //
			msg.Filename=str2[str2.length-1];											  //	
			msg.blockname=e.getAttribute("name");										  //
			msg.Directory=Dir;														      //
			oos.writeObject(msg);														  //
			byte[] bf= new byte[blockSize];													  //
			int r;																		  //
			r = is.read(bf);															  //
			String s=new String(bf,0,r);												  //
			str+=s;																		////
			} catch (IOException e1) {
				deleteDataNode(ind);
				j--;
				el=e.getChildNodes();
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			j++;
		}
		i++;
	}
	return str;
}
/*
 * getting list of files and folders in the specified directory (simply fetching data from xml)
 * */
static String ls(String to) throws XPathExpressionException	
{
	String chk="/root";
	if (to!="")
		chk="/root"+"/"+to;
	XPath xpath = XPathFactory.newInstance().newXPath();
	Node n =(Node) xpath.evaluate(chk,doc.getDocumentElement(),XPathConstants.NODE);
	
	NodeList nl=n.getChildNodes();
	
	int i=0;
	String str="";
	Element e;
	while((e=(Element) nl.item(i))!=null)
	{
		String str2="";
		String type="";
		if (e.getNodeName()=="file")
		{
			type= "FILE";
			str2=e.getAttribute("name");
		}
		else
		{
			type= "DIRECTORY";
			str2=e.getNodeName();
		}
		str+=str2+"\t"+type+"\n";
		i++;
	}
	
	return str;
	
}

/*
 * Renaming a File in the FS 
 * (Just need to rename in xml because renaming the blocks is never needed in each slave node) 
 */
static int rename(String to, String newName) throws XPathExpressionException, TransformerException
{
	String chk="/root";
	String schk="";
	if (to!="")
	{
		String[] sbuff=to.split("/");
		for (int i=0; i<sbuff.length-1;i++)
			chk=chk+"/"+sbuff[i];
		schk=chk+"/file[@name=\""+newName+"\"]";
		chk+="/file[@name=\""+sbuff[sbuff.length-1]+"\"]";
		
	}
	XPath xpath = XPathFactory.newInstance().newXPath();
	if((Boolean) xpath.evaluate(schk,doc.getDocumentElement(),XPathConstants.BOOLEAN)==true)
			{
				return -2;
			}
	
	
	
		Element n =(Element) xpath.evaluate(chk,doc.getDocumentElement(),XPathConstants.NODE);
		if (n==null){
			return -1;
		}
		
		n.setAttribute("name", newName);
	
		NodeList list = doc.getElementsByTagName("root");					////
		Node node = list.item(0);											  //	
		DOMSource source = new DOMSource(node);                               //
		File xmlF=new File(xmlPath);                                    //
		StreamResult result = new StreamResult(xmlF);                         //////  to save changes in Xml File
		TransformerFactory tFactory = TransformerFactory.newInstance();       //
	    Transformer transformer = tFactory.newTransformer();                  //
		transformer.transform(source, result);                              ////
		System.out.println("Renamed!");
	return 0;
	
	
}
static String xmlPath="FileDB.xml";
static ServerSocket NameNodeFTPort=null;
public static void main(String [] argv) throws ParserConfigurationException, SAXException, IOException, XPathExpressionException, TransformerException, InterruptedException
	{
	File confFile = new File("namenodeConfig");
	FileInputStream conf=new FileInputStream(confFile);
	byte[] readConf=new byte[(int)confFile.length()];
	conf.read(readConf);
	String readConfStr=new String(readConf);
	String[] readConfArr=readConfStr.split("\n");
	
	xmlPath=readConfArr[0];
		
		NameNodePort= new ServerSocket(9900);											//////
		NameNodeFTPort= new ServerSocket(9910);
		dataNodes= new ArrayList<DataNodeInfo>();											//
		File fXmlFile = new File(xmlPath);											//
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();			//
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();							////initializing sockets and Document
		doc = dBuilder.parse(fXmlFile);														//	
		XPath xpath = XPathFactory.newInstance().newXPath();								//
																							//
		AddNodeThread addnode =new AddNodeThread();											//	
		addnode.start();//////
		System.out.println("start");	
		int size=0;
		while(size<2){size=dataNodes.size();Thread.sleep(100);};///// we set minimum 3 slave node needed to run the system. (this minimum can be increase or decrease)
		//System.out.println("start");
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String inp;
		while(1==1) ////user inputs starts
		{
		
		inp=br.readLine();
		String [] inparr =inp.split(" ");  
		if (inparr[0].equals("push"))
		{
			if (inparr.length==5)
			{
				blockSize=Integer.parseInt(inparr[4]);
				pushFile(inparr[1],inparr[2],Integer.parseInt(inparr[3]));
			}
			
		}
		else if (inparr[0].equals("cat"))
		{
			if (inparr.length==2)
			System.out.println(fetchFile(inparr[1]));
		}
		else if (inparr[0].equals("ls"))
		{
			if (inparr.length==2)
			System.out.println(ls(inparr[1]));
		}
		else if (inparr[0].equals("mkdir"))
		{
			if (inparr.length==2)
			makeDir(inparr[1]);
		}
		else if (inparr[0].equals("rename"))
		{
			if (inparr.length==3)
			rename(inparr[1],inparr[2]);
		}
		else if (inparr[0].equals("delete"))
		{
			if (inparr.length==2)
			delFile(inparr[1]);
		}
		System.out.println("end");
		}
		
		
	}
}
