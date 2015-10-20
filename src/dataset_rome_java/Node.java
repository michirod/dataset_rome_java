package dataset_rome_java;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.time.Instant;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

public class Node implements Comparable<Node>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int id;
	private TreeMap<Instant, Position> positions;
	private TreeMap<Node, TreeSet<Contact>> contacts;
	
	public Node(int id)
	{
		this.id = id;
		positions = new TreeMap<Instant, Position>();
		contacts = new TreeMap<Node, TreeSet<Contact>>();
	}
	
	public int getId()
	{
		return this.id;
	}
	
	public synchronized void addPosition(Position p)
	{
		
		positions.put(p.getDate(), p);
		p.addNodeRef(this);
	}
	
	public TreeMap<Instant, Position> getPositions()
	{
		return positions;
	}
	
	public Position getCoordinateAtTime(Instant time)
	{
		return positions.get(positions.floorKey(time));
	}
	
	public synchronized void addContact(Contact c)
	{
		if (c.getPositions(this) != null)
		{
			Node peer = c.getPeer(this);
			if ( ! contacts.containsKey(peer))
				contacts.put(peer, new TreeSet<Contact>());
			contacts.get(peer).add(c);
		}
	}
	
	public synchronized void removeContact(Contact c)
	{
		contacts.get(c.getPeer(this)).remove(c);
	}

	@Override
	public int compareTo(Node o) {
		return this.id - o.getId();
	}
	
	public void writePositions(PrintStream writer)
	{
		for(Position p : positions.values())
		{
			writer.println(p.toString());
		}
	}
	
	public void writeContacts(PrintStream writer)
	{
		for (TreeSet<Contact> set : contacts.values())
		{
			for (Contact c : set)
			{
				writer.println(c.toString());
			}
		}
	}
	
	public void writeNode(PrintStream writer)
	{
		writer.println("NODE_HEADER " + id);
		writer.println("POSITIONS_HEADER");
		writePositions(writer);
		writer.println("CONTACTS_HEADER");
		writeContacts(writer);
		
	}
	
	public static Node readNode(BufferedReader reader)
	{
		if (reader != null)
		{
			try {
				Node node = parseId(reader);
				parsePositionsAndContacts(reader, node);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}

	private static Node parseId(BufferedReader reader) throws Exception {
		String line = reader.readLine();
		if (line == null)
			throw new Exception("Malformed File");
		StringTokenizer tok = new StringTokenizer(line, " ");
		if ( ! tok.nextToken().equals("NODE_HEADER"))
			throw new Exception("Malformed File");
		Node node = new Node(Integer.parseInt(tok.nextToken()));
		return node;
	}
	
	private static void parsePositionsAndContacts(BufferedReader reader, Node node) throws Exception {
		String line = reader.readLine();
		if (line == null)
			throw new Exception("Malformed File");
		if (line.equals("POSITIONS_HEADER"));
		{
			while ((line = reader.readLine()) != null)
			{
				if (line.equals("CONTACTS_HEADER"))
				{
					parseContacts(reader, node);
					break;
				}
				node.addPosition(Position.parse(line));			
			}
				
		}

	}

	private static void parseContacts(BufferedReader reader, Node node) throws IOException {
		String line;
		while ((line = reader.readLine()) != null)
		{
			node.addContact(Contact.parseContact(line));
		}
		
	}

	public synchronized void writeNodeBin(ObjectOutputStream outputStream) throws IOException {
		outputStream.writeObject(this);
	}

}
