package mrlite.io;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public abstract class Writer {

	
	public BufferedOutputStream bufferedWriter;
	//public FileOutputStream outputStream;
	public String writeTo;
	public int bufferSize;
	public String delimiter;
	public void write(Object record) throws IOException
	{
		
		bufferedWriter.write(((String)record
				+
				delimiter).getBytes());
		
	}
	
	public void flush() throws IOException
	{
		bufferedWriter.flush();
	}
	
	public void remoteSend(Socket socket ,int buffSize) throws IOException
	{
		try{
		bufferedWriter.close();
		}catch(Exception e)
		{
			
		}
		System.out.println("writer closed");
		FileInputStream fis=new FileInputStream (writeTo);
		
		int len=0;
		OutputStream os= socket.getOutputStream();
		//BufferedOutputStream bos=new BufferedOutputStream(os,buffSize*3);
		byte[] buffer= new byte[buffSize];
		while ((len=fis.read(buffer))>=0)
		{
	//		System.out.println(len);

			os.write(buffer,0,len);
			os.flush();
		}
		os.flush();
		
	}
	
	public void setFile(String name) throws FileNotFoundException
	{
		
		writeTo=name;
		bufferedWriter =new BufferedOutputStream(new FileOutputStream(writeTo));
		
	}
	public void setDelimiter(String delim)
	{
		delimiter=delim;
	}
	public void setBufferSize(int buffSize)
	{
		bufferSize=buffSize;
	}
	
}
