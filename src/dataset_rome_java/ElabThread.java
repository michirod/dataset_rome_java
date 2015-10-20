package dataset_rome_java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.*;

public class ElabThread extends Thread {
	
	public final static String outputFileNamePrefix = "node_";
	public final static String outputFileNameExtension = ".csv";
	protected LinkedList<WorkingThread> threadQueue;
	protected LinkedList<File> fileQueue;
	protected HashMap<Integer, PrintWriter> writerMap;
	protected HashMap<Integer, SortedSet<Contact>> contactsMap;
	protected final Lock lock = new ReentrantLock();
	protected final Semaphore sem = new Semaphore(0);
	protected int stop;
	protected boolean pipelined;
	protected boolean sortByPeer;
	
	public ElabThread(boolean pipelined, boolean sortByPeer)
	{
		if (pipelined)
			this.threadQueue = new LinkedList<>();
		else
			this.fileQueue = new LinkedList<>();
		this.writerMap = new HashMap<>();
		this.contactsMap = new HashMap<>();
		this.stop = 0;
		this.pipelined = pipelined;
		this.sortByPeer = sortByPeer;
		this.setName("ElabThread");
	}
	
	public synchronized void setStop(int stop)
	{
		lock.lock();
		this.stop = stop;
		lock.unlock();
	}
	
	public synchronized void insertThread(WorkingThread thread)
	{
		lock.lock();
		threadQueue.add(thread);
		sem.release();
		lock.unlock();
		
	}
	
	public synchronized void insertFile(File file)
	{
		if (! fileQueue.isEmpty())
		{
			if (fileQueue.getLast().getName().compareTo(file.getName()) < 0)
			{	
				fileQueue.addLast(file);
			}
		}
		else
			fileQueue.addLast(file);
	}
	
	protected File getCurrentFile() throws InterruptedException
	{
		File f;
		if (pipelined)
		{
			WorkingThread thread = null;
			sem.acquire();
			lock.lock();
			thread = threadQueue.poll();
			lock.unlock();
			f = new File(thread.getOutputFilename());
		}
		else
		{
			f = fileQueue.removeFirst();
		}
		return f;
	}
	
	
	
	@Override 
	public void run()
	{
		File f = null;
		BufferedReader reader = null;
		int count = 0;
		while (stop == 0 || count < stop)
		{
			try {
				
				f = getCurrentFile();
				reader = new BufferedReader(new FileReader(f));
				String line;
				PrintWriter writer = null;
				int nodeId = 0;
				while ((line = reader.readLine()) != null)
				{
					if (line.startsWith("NODE_HEADER"))
					{
						nodeId = Integer.parseInt(line.substring(12));
						writer = writerMap.get(nodeId);
						if ( writer == null)
						{
							writer = new PrintWriter(Main.folder + outputFileNamePrefix + String.format("%03d", nodeId) + outputFileNameExtension);
							writerMap.put(nodeId, writer);
							if (this.sortByPeer)
							{
								contactsMap.put(nodeId, new TreeSet<>(new Comparator<Contact>() {

									@Override
									public int compare(Contact o1, Contact o2) {
										int[] ns1 = o1.getNodesId();
										int[] ns2 = o2.getNodesId();
										if (ns1[0] == ns2[0])
										{
											if (ns1[1] == ns2[1])
												return o1.compareTo(o2);
											else return ns1[1] - ns2[1];
										}
										else if (ns1[1] == ns2[1])
										{
											if (ns1[0] == ns2[0])
												return o1.compareTo(o2);
											else return ns1[0] - ns2[0];
										}
										else if (ns1[0] == ns2[1])
										{
											if (ns1[1] == ns2[0])
												return o1.compareTo(o2);
											else return ns1[1] - ns2[0];
										}
										else if (ns1[1] == ns2[0])
										{
											if (ns1[0] == ns2[1])
												return o1.compareTo(o2);
											else return ns1[0] - ns2[1];
										}
										else
											return o1.compareTo(o2);
									}
								}));
							}
						}
						continue;
					}
					else if (line.startsWith("POSITIONS_HEADER"))
					{
						while ( ! (line = reader.readLine()).startsWith("CONTACTS_HEADER"));
						continue;
					}
					if (line == null || line.equals(""))
						continue;
					Contact c = Contact.parseContact(line);
					if (this.sortByPeer)
						contactsMap.get(nodeId).add(c);
					else
						writer.println(c.toString(nodeId));	
				}		
				count++;		
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (this.sortByPeer)
		{
			PrintWriter writer = null;
			Contact prev = null;
			long interval = -1;
			for (int node : writerMap.keySet())
			{
				writer = writerMap.get(node);
				for (Contact c : contactsMap.get(node))
				{
					if (prev != null)
					{
						if (prev.getPeer(node) == c.getPeer(node))
						{
							interval = c.getStartTime().getEpochSecond() - prev.getEndTime().getEpochSecond();
						}
						else
							interval = -1;
					}
					writer.print(c.toString(node));
					writer.print(";");
					if (interval > 0)
					{
						writer.print(interval);
					}
					writer.println();
					prev = c;
				}
				writer.close();
			}
		}
		else
		{
			for (PrintWriter p : writerMap.values())
				p.close();
		}
	}

	protected void lineOperation(PrintWriter writer, BufferedReader reader, String line) throws IOException
	{
		int nodeId = -1;
		if (line.startsWith("NODE_HEADER"))
		{
			nodeId = Integer.parseInt(line.substring(12));
			writer = writerMap.get(nodeId);
			if ( writer == null)
			{
				writer = new PrintWriter(Main.folder + outputFileNamePrefix + String.format("%03d", nodeId) + outputFileNameExtension);
				writerMap.put(nodeId, writer);
			}
			return;
		}
		else if (line.startsWith("POSITIONS_HEADER"))
		{
			while ( ! (line = reader.readLine()).startsWith("CONTACTS_HEADER"));
			return;
		}
		if (line == null || line.equals(""))
			return;
		Contact c = Contact.parseContact(line);
		writer.println(c.toString(nodeId));	
	}	
}


