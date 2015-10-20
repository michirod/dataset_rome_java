package dataset_rome_java;

import java.time.Instant;
import java.time.LocalDate;
import java.util.TreeMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ThreadManager {
	TreeMap<Instant, WorkingThread> threadMap;
	Instant current;
	Instant previous;
	WorkingThread curThread, prevThread;
	ElabThread elabThread;
	int runningThreads;
	boolean overlap;
	private final Lock lock = new ReentrantLock();
	private final Condition runningThreadsCond = lock.newCondition();
	
	public ThreadManager()
	{
		this.threadMap = new TreeMap<>();
		this.current = null;
		this.overlap = false;
		this.elabThread = new ElabThread(true, true);
		this.elabThread.start();
		this.runningThreads = 0;
	}
	
	public WorkingThread createThread(Instant day)
	{
		WorkingThread t = new WorkingThread(day, this, threadMap.size() + 1);
		threadMap.put(day, t);
		return t;
	}
	public void signalThreadPool()
	{
		lock.lock();
		runningThreads--;
		runningThreadsCond.signal();
		lock.unlock();
	}
	
	public void signalElabThread(WorkingThread thread)
	{
		elabThread.insertThread(thread);
	}
	
	public void insertPosition(Position p) 
	{
		if (current == null)
		{
			current = p.getDate();
			curThread = createThread(current);
			curThread.setFirst();
			curThread.setPriority(Thread.MAX_PRIORITY);
		}
		else if (! LocalDate.from(p.getDate().atOffset(Main.zoneOffset)).isEqual(LocalDate.from(current.atOffset(Main.zoneOffset))))
		{
			overlap = true;
			previous = current;
			prevThread = curThread;
			current = p.getDate();
			curThread = createThread(current);
			prevThread.setNextThread(curThread);
		}
		threadMap.get(current).putPosition(p);
		if (overlap)
		{
			if (p.getDate().isAfter(current.plusSeconds(Main.TEMP_GRANULARITY))){
				overlap = false;
				startWorkingThread(prevThread);
			}
			if (overlap)
				prevThread.putPosition(new Position(p.getNode(), p.getDate(), p.getCoordinate()));
		}
		
	}
	
	private void startWorkingThread(WorkingThread prevThread) 
	{
		lock.lock();
		while (runningThreads + 1 > 4)
		{
			try {
				runningThreadsCond.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		runningThreads++;
		lock.unlock();
		prevThread.start();
	}

	public void notifyLast() 
	{
		curThread.setLast();
		curThread.start();
		elabThread.setStop(threadMap.size());
		joinThreads();
	}

	private void joinThreads()
	{
		try {
			for(Thread t : threadMap.values())
			{
				t.join();			
			}
			elabThread.join();	
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public synchronized void checkForBoundContacts(ContactsManager manPrev, ContactsManager manNext)
	{
		manPrev.mergeWithNext(manNext);
	}



}
