package com.maloop.datanode;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import android.app.Activity;
import android.os.Bundle;
import java.io.*;

import java.net.UnknownHostException;

import shared.Message;

public class MDFSDataNodeAndActivity extends Activity {
    /** Called when the activity is first created. */
	static Socket nodeSocket; 
	static int blocksize=200;
	static String NameNodeIp="192.168.43.6";
    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.main);
        try {
			nodeSocket=new Socket(NameNodeIp,9900);
		
			String root="/sdcard/MDFS/";
		(new File(root)).mkdir();
		
		
			System.out.println(root);
		OutputStream os = nodeSocket.getOutputStream();
		while(1==1){
			InputStream is = nodeSocket.getInputStream();
			ObjectInputStream ois= new ObjectInputStream(is);
			Message msg= new Message();
			
			msg=(Message) ois.readObject();
			if (msg.action==0)
			{
				
				
				(new File(root+msg.Directory)).mkdirs();
				File fl = new File(root + msg.Directory + msg.blockname);
				FileOutputStream fos = new FileOutputStream(fl,false);
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
				Socket fts= new Socket();
				SocketAddress addr=new InetSocketAddress(NameNodeIp,9910);
				int tries=20000;
				for (int t=0; t<tries;t++)
				{
					if (fts.isConnected()==false)
					{
						fts.connect(addr, 2000);
					}
					else break;
				}
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

				fos.close();
		
			}
			else if (msg.action==1)
			{
				
				File fl=new File(root+msg.Directory+msg.blockname);
				
				FileInputStream fis =new FileInputStream(fl);
				byte[] buf= new byte[(int) fl.length()];
				int r=fis.read(buf);
				os.write(buf);
				fis.close();
			}
			else if (msg.action==2)
			{
				
				File fl=new File(root+msg.Directory+msg.blockname);
				System.out.println(msg.Directory+msg.blockname+fl.delete());
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
		}
		
    }
}