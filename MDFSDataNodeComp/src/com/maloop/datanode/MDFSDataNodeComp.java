package com.maloop.datanode;

import java.net.Socket;

import java.io.*;

import java.net.UnknownHostException;

import shared.Message;

public class MDFSDataNodeComp {
	/** Called when the activity is first created. */
	static Socket nodeSocket;
	static int blocksize = 500000;
	static String nameNodeIp="192.168.1.101";
	static public void main(String[] args) {
		// super.onCreate(savedInstanceState);
		// setContentView(R.layout.main);
		try {


			//String root = "MDFS/";
			
			File confFile = new File("datanodeConfig");
			FileInputStream conf=new FileInputStream(confFile);
			byte[] readConf=new byte[(int)confFile.length()];
			conf.read(readConf);
			String[] readConfArr=(new String(readConf).split("\n"));
			
			String root=readConfArr[1];
			nameNodeIp=readConfArr[0];
			nodeSocket = new Socket(nameNodeIp, 9900);
			if (args.length>0)
			root+=args[0]+"/";
			(new File(root)).mkdir();

			System.out.println(root);
			OutputStream os = nodeSocket.getOutputStream();
			
			while (true) {
			//	Thread.sleep(100);
				InputStream is = nodeSocket.getInputStream();
				System.out.println(root);
				
				System.out.println(root);
				Message msg = new Message();
				//try{
					ObjectInputStream ois = new ObjectInputStream(
							nodeSocket.getInputStream());
				msg = (Message) ois.readObject();
				//}
				//catch (EOFException e)
				//{
					//continue;
				//}
				if (msg.action == 0) {
			//		byte[] buff = new byte[(int)msg.blocksize];

					(new File(root + msg.Directory)).mkdirs();
					File fl = new File(root + msg.Directory + msg.blockname);
					FileOutputStream fos = new FileOutputStream(fl);
					int len = 0,read=0,rate=1000;
					byte[] buffb = new byte[50000];
					System.out.println("here1");
					//BufferedInputStream bis = new BufferedInputStream(is);
					System.out.println("Block Size: "+ msg.blocksize);
					int lim=(int)(msg.blocksize/rate);
					int rem =(int)(msg.blocksize%rate);
					//for (int i=0; i<lim;i++) {
						//if (len==0) break;
						//if (buffb[0]==0) break;
					Socket fts= new Socket(nameNodeIp,9910);
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
						for(;;){
							len=fts.getInputStream().read(buffb);
							System.out.println(len);
							//buff[i]=buffb[0];
							if (len<0)break;
							fos.write(buffb, 0, len);
							fos.flush();
							read+=len;
						}
						fts.close();
						//fos.write(buff, 0, buff.length);
						//if (read>=blocksize)break;
						//if (len < 500)
						//	break;
				//	}
				//	if (rem>0)
				//	{
				//		len=is.read(buffb);
				//		fos.write(buffb, 0, len);
				//	}
					System.out.println("here2");
					fos.close();
				} else if (msg.action == 1) {

					File fl = new File(root + msg.Directory + msg.blockname);

					FileInputStream fis = new FileInputStream(fl);
					byte[] buf = new byte[(int) fl.length()];
					int r = fis.read(buf);
					os.write(buf);
					fis.close();
				} else if (msg.action == 2) {

					File fl = new File(root + msg.Directory + msg.blockname);
					System.out.println(msg.Directory + msg.blockname
							+ fl.delete());
				}
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
		} //catch (InterruptedException e) {
			// TODO Auto-generated catch block
		//	e.printStackTrace();
	//	}

	}
}