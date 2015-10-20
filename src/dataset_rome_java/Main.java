package dataset_rome_java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.StringTokenizer;
import java.util.TreeMap;


public class Main {

	public static String folder = "/home/michele/DTN/DATASETS/taxi/";
	public static String input_fileName = folder + "taxi_february.txt";
	public static String output_fileName = "taxi_cont.txt";
	public static int TEMP_GRANULARITY = 300;
	public static int SPAT_GRANULARITY = 50;
	public static ZoneOffset zoneOffset = null;
	
	public static void main(String[] args) {
		//checkInputFileOrder();
		//mainMain();
		threadedMain();
		//startElabThread();
	}
	
	private static void threadedMain() 
	{
		BufferedReader reader = null;
		String line = "";
		ThreadManager threadManager = new ThreadManager();
		
		try {
			reader = new BufferedReader(new FileReader(input_fileName));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			int count = 0;
			while ((line = reader.readLine()) != null)
			{
				count ++;
				Position p = parse_taxi_roma(line, count);
				if (p != null)
				{
					threadManager.insertPosition(p);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		threadManager.notifyLast();
		
		System.out.println("END OF PROCESSING");
		
	}
	
	public static void mainMain()
	{
		BufferedReader reader = null;
		String line = "";
		
		Nodes nodes = new Nodes();
		
		try {
			reader = new BufferedReader(new FileReader(input_fileName));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			int count = 0;
			while ((line = reader.readLine()) != null)
			{
				count ++;
				Position p = parse_taxi_roma(line, count);
				if (p != null)
					nodes.insertPosition(p);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println(nodes.printNode(89));
		
		// Find contacts
		ContactsManager man = new ContactsManager(nodes);
		man.calculateContacts();
		
		PrintStream writer = null;
		try {
			writer = new PrintStream(folder + output_fileName);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		nodes.printNodes(writer);
	}
	
	public static Position parse_taxi_roma(String line, int lineNum)
	{
		try {
			StringTokenizer tok = new StringTokenizer(line, ";");
			String id = tok.nextToken();
			if (Integer.parseInt(id) > 500) return null;
			String date = tok.nextToken();
			tok.nextToken("(");
			String lat = tok.nextToken("( ");
			String lon = tok.nextToken(" )");
			String dateSec;
			if (date.contains("."))
			{
				tok = new StringTokenizer(date, ".+");
				dateSec = tok.nextToken();
				tok.nextToken();
				dateSec += "+" + tok.nextToken();
			}
			else
			{
				dateSec = date;
			}
			//System.out.println(id + ' ' + dateSec + ' ' + lat + ' ' + lon);
			Instant d = null;
			d = ZonedDateTime.parse(dateSec, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssX")).toInstant();
			if (zoneOffset == null)
				zoneOffset = ZonedDateTime.parse(dateSec, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssX")).getOffset();
			//d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssX").parse(dateSec);
			return new Position(id, d, lat, lon);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error at line " + lineNum);
			System.out.println(line);
			System.exit(1);
		}
		return null;
	}
	
	public static void checkInputFileOrder()
	{
		BufferedReader reader = null;
		String line = "";
		Position cur = null, prev;

		try {
			reader = new BufferedReader(new FileReader(input_fileName));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			int count = 0;
			while ((line = reader.readLine()) != null)
			{
				count ++;
				prev = cur;
				while ((cur = parse_taxi_roma(line, count)) == null);
				if (prev != null)
				{
					if (cur.compareTo(prev) < 0)
					{
						System.out.println("Ordering Error");
						System.out.println(prev);
						System.out.println(cur);
						System.exit(1);
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Input ordered correctly");
	}
	
	private static File[] getFiles(String folder, String pattern, String nomeFile)
	{
		File dir = new File(folder);
		FilenameFilter filter = new FilenameFilter() {
			
			@Override
			public boolean accept(File dir , String name) {
				return name.matches(pattern + nomeFile);
			}
		};
		File[] files = dir.listFiles(filter);
		return files;
	}
	
	public static TreeMap<LocalDate, File> getOrderedFiles(String folder, String nomeFile)
	{
		TreeMap<LocalDate, File> result = new TreeMap<>();
		String pattern = "\\d{4}-\\d{2}-\\d{2}_";
		File[] files = getFiles(folder, pattern, nomeFile);
		for (File f : files)
		{
			StringTokenizer tok = new StringTokenizer(f.getName(), "_");
			LocalDate date = LocalDate.parse(tok.nextToken());
			result.put(date, f);
		}
		return result;
	}
	
	public static void startElabThread()
	{
		ElabThread elabThread = new ElabThread(false, true);
		TreeMap<LocalDate, File> filesMap = getOrderedFiles(Main.folder, Main.output_fileName);
		File[] files = filesMap.values().toArray(new File[0]);
		for (File f : files)
		{
			elabThread.insertFile(f);
		}
		elabThread.setStop(files.length);
		elabThread.start();
		try {
			elabThread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
