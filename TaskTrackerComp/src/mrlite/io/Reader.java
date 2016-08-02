package mrlite.io;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.StringTokenizer;
public abstract class Reader {
	
	
	
	
	String startBlock;
	long startOffset;
	String endBlock;
	long endOffset;
	String delimiter;
	String lastRecord=null;
	int currentRecordPointer;
	long startseek;
	long endseek;
	String[] buffer;
	String spill;
	int bufferSize;
	RandomAccessFile startFile;
	RandomAccessFile endFile;
	protected class BlockFile
	{
		public String name;
		public long startOffset;
		public long endOffset;
		public String lastRecord;
		public BlockFile(String n, long so,long eo)
		{
			name=n;
			startOffset=so;
			endOffset=eo;
		}
	}
	ArrayList<BlockFile> blockList;
	StringTokenizer tokenizer;
	public void addBlock(String n, long so,long eo)
	{
		
		blockList.add(new BlockFile(n, so, eo));
	}
	int currentBlock=0;
	
	public void remoteCopy(Socket socket,String copyTo,int buffSize) throws IOException
	{
		InputStream is=socket.getInputStream();
		int len=0;
		FileOutputStream fos= new FileOutputStream(copyTo); 
		byte[] buffer= new byte[buffSize];
		while ((len=is.read(buffer))>=0)
		{
			//System.out.println(len);
			
			fos.write(buffer,0,len);
		}
		fos.close();
		long totalSize=(new File(copyTo)).length();
		BlockFile bf = new BlockFile(copyTo, 0, totalSize-1);
		blockList.add(bf);
		
	}
	
	protected boolean fillBuffer()
	{
		if (blockList== null)
		{
			return false;
		}
		currentRecordPointer=0;
		try {
			if (currentBlock>=
					blockList.size()-2 && 
					startFile.getFilePointer()>endseek)
			{
				return false;
			}
			byte [] buff=new byte[bufferSize];
			int numread=startFile.read(buff);
			
			int minus=0;
			for (int i=numread-1;i>=0;i--,minus++)
			{
				
				
				if (buff[i]==
						delimiter.getBytes()[0]) break;
			}
			
			startFile.seek(startFile.getFilePointer()-minus);
			String lastrec=null;
			if(startFile.getFilePointer()>endseek)
			{
				lastrec=blockList.get(currentBlock).lastRecord;
				currentBlock++;
				if(currentBlock<blockList.size()-2)
				{
					BlockFile bf=blockList.get(currentBlock);
					startFile=new RandomAccessFile(bf.name,"r");
					startFile.seek(bf.startOffset);
					endseek=bf.endOffset;
				}
			}

		
			if(numread-minus<0) return false;
			
			//buffer=(new String(buff,0,numread-minus)).split(delimiter);
			spill=new String(buff,0,numread-minus);
			if(lastrec!=null)
			{
				if(spill!=null)
					spill+=lastrec;
				else
					spill=lastrec;
			}
			tokenizer= new StringTokenizer(spill);
			spill=null;
			return true;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			
			e.printStackTrace();
			return false;
		}
		
		
		
		
		
		
	}
	
	
	
	public void init()
	{
		currentRecordPointer=0;
		byte delim=delimiter.getBytes()[0];
	try {
				
	for(int i=0;i<blockList.size()-1;i++)
	{
					
		startBlock=blockList.get(i).name;
		endBlock=blockList.get(i+1).name;
		startOffset=blockList.get(i).startOffset;
		endOffset=blockList.get(i+1).endOffset;

			startFile=new RandomAccessFile(startBlock, "r");
			
			if(endBlock!=null)
			{
				endFile=new RandomAccessFile(endBlock, "r");
			}
			
			startFile.seek(startFile.length()-1);
			int del=0;
			while(true)
			{
				del= startFile.read();
				if (del==delim ) break;
				startFile.seek(startFile.getFilePointer()-2);
			}
			blockList.get(i).endOffset=endseek=startFile.getFilePointer()-1;
			if(endBlock!=null)
			{
				byte [] lastrecbyte=new byte[(int) (startFile.length()-startFile.getFilePointer())];
				startFile.read(lastrecbyte);
				byte [] lastrecbyte2=new byte[(int) (endOffset+1)]; 
				endFile.read(lastrecbyte2);
				blockList.get(i).lastRecord=lastRecord=(new String(lastrecbyte))+ (new String(lastrecbyte2));
			@SuppressWarnings("unused")
			int a=0;
			}
			startFile.seek(startOffset);
			startseek=startOffset;
			startFile.seek(blockList.get(0).startOffset);
			endseek=blockList.get(0).endOffset;
		}
		
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}

	public Object getRecord()
	{
	/*	
		String rec="";
		while(rec=="" || rec=="\r"){
		if (buffer==null || currentRecordPointer>=buffer.length)
			
		{
			if(fillBuffer()==false)
			{
				String ret=lastRecord;
				lastRecord=null;
				return ret;
			}
		}
		
			rec=buffer[currentRecordPointer];
			buffer[currentRecordPointer]=null;
			currentRecordPointer++;
		
			if(rec!=null)
			{
			
				return rec;
			}
	}
		
			return rec;
		
	*/
	String rec=null;
	do
	{
	if(tokenizer.hasMoreTokens()==false)
	{
		if(fillBuffer()==false)
	
		{
			
			return null;	
		}
	}
	
	rec=tokenizer.nextToken(delimiter);
	//System.out.println("very good");
	}
	while(rec==null || rec=="");
	return rec;
	}
	
	public Object read()
	{
		return getRecord();
	}

	
	public Reader()
	{
		blockList=new ArrayList<BlockFile>();
		tokenizer= new StringTokenizer("");
	}
	
	public void  setBufferSize(int size)
	{
		bufferSize=size;
	}
	
	public void  setDelimiter(String delim)
	{
		delimiter=delim;
	}
	public void setblocks(String stblk,long stoffset,String edblk,long edoffset )
	{
		startBlock=stblk;
		startOffset=stoffset;
		endBlock=edblk;
		endOffset=edoffset;
	}
	
}
