package dataset_rome_java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.TreeSet;

public class OrderThread {
	public final static String nodeOutputFileName = "_node_ordered.csv";
	protected int curNode;
	protected File curFile;
	protected TreeSet<Contact> contacts;
	
	public OrderThread(int curNode, File curFile)
	{
		this.curFile = curFile;
		this.curNode = curNode;
		
	}
	
	
	public void run()
	{
		PrintWriter writer = null;
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(curFile));
			String line;
			writer = new PrintWriter(Main.folder + String.format("%03d", curNode) + nodeOutputFileName);
			while ((line = reader.readLine()) != null)
			{
				

			}			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (writer != null)
				writer.close();
			if (reader != null)
				try {
					reader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
}
