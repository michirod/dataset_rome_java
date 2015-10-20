package dataset_rome_java;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TreeSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class WorkingThread extends Thread implements Comparable<WorkingThread>{
	
	private TreeSet<Position> positions;
	private Nodes nodes;
	private Instant day;
	private String dayString;
	private WorkingThread nextThread;
	private boolean first;
	private boolean last;
	private boolean endFirstScan = false;
	private boolean endFirstBound = false;
	private final Lock lock = new ReentrantLock();
	private Condition firstScan = lock.newCondition();
	private Condition firstBound = lock.newCondition();
	private Condition nextThreadActive = lock.newCondition();
	private ContactsManager man;
	private String outputFilename;
	private ThreadManager tMan;
	
	public WorkingThread(Instant day, ThreadManager tMan, int id) {
		this.positions = new TreeSet<Position>();
		this.nodes = new Nodes();
		this.day = day;
		this.nextThread = null;
		OffsetDateTime dt = OffsetDateTime.ofInstant(day, Main.zoneOffset);
		dayString = DateTimeFormatter.ofPattern("uuuu-MM-dd_").format(dt);
		//dayString = ZonedDate.from(day).format(DateTimeFormatter.ofPattern("yyyy-MM-dd_"));
		//dayString = new SimpleDateFormat("yyyy-MM-dd_").format(day);
		this.outputFilename = Main.folder + dayString + Main.output_fileName;
		this.tMan = tMan;
		this.setName("WorkingThread-" + id);
	}
	
	public String getOutputFilename()
	{
		return this.outputFilename;
	}
	
	public void setFirst()
	{
		this.first = true;
	}
	public void setLast()
	{
		this.last = true;
	}
	
	public Instant getDay()
	{
		return day;
	}
	
	public void setNextThread(WorkingThread thread)
	{
		this.lock.lock();
		this.nextThread = thread;
		this.nextThreadActive.signal();
		this.lock.unlock();
	}
	
	public void putPosition(Position p)
	{
		int size = this.positions.size();
		boolean result = this.positions.add(p);
		int newSize = this.positions.size();
		if (result == false)
		{
			
		}
		if (size + 1 != newSize)
		{
			
		}
	}
	
	@Override 
	public void run()
	{
		System.out.println("Thread " + this.getName() + ": thread start");
		for(Position p : positions)
			nodes.insertPosition(p);
		
		man = new ContactsManager(nodes);
		man.calculateContacts();
		
		this.lock.lock();
		endFirstScan = true;
		System.out.println("Thread " + this.getName() + ": finished first scan");
		if (!first)
		{
			firstScan.signal();
			System.out.println("Thread " + this.getName() + ": signaled previous thread");
			while (endFirstBound == false)
				try {
					firstBound.await();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		tMan.signalThreadPool();
		this.lock.unlock();
		if (!last)
		{
			if (this.getPriority() == MAX_PRIORITY)
			{
				this.setPriority(NORM_PRIORITY + 1);
				nextThread.setPriority(MAX_PRIORITY);
			}

			System.out.println("Thread " + this.getName() + ": waiting for next thread");
			nextThread.lock.lock();
			while (nextThread == null)
				try {
					this.nextThreadActive.await();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			while (nextThread.endFirstScan == false)
				try {
					nextThread.firstScan.await();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.exit(1);
				}
			nextThread.lock.unlock();
			System.out.println("Thread " + this.getName() + ": Starting merging");

			man.mergeWithNext(nextThread.man);
			nextThread.lock.lock();
			nextThread.endFirstBound = true;
			nextThread.firstBound.signal();
			nextThread.lock.unlock();
		}
		
		PrintStream writer = null;
		try {
			writer = new PrintStream(outputFilename);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		nodes.printNodes(writer);
		writer.close();
		tMan.signalElabThread(this);
		System.out.println("Thread " + this.getName() + ": thread end");
	}

	@Override
	public int compareTo(WorkingThread o) {
		// TODO Auto-generated method stub
		return day.compareTo(o.getDay());
	}


}
