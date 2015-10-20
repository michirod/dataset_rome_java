package dataset_rome_java;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.TreeSet;

public class Contact implements Comparable<Contact>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private HashMap<Node, TreeSet<Position>> nodes;
	private TreeSet<Position> positions1;
	private TreeSet<Position> positions2;
	private Duration duration;
	private Instant startTime;
	private Instant endTime;
	
	public Contact(Instant startTime, Instant endTime)
	{
		this.startTime = startTime;
		this.endTime = endTime;
	}
	
	public Contact(Node node1, Node node2)
	{
		this.positions1 = new TreeSet<Position>();
		this.positions2 = new TreeSet<Position>();
		this.nodes = new HashMap<Node, TreeSet<Position>>(2);
		nodes.put(node1, positions1);
		nodes.put(node2, positions2);
		this.startTime = null;
		this.endTime = null;
	}
	
	public TreeSet<Position> getPositions(Node n)
	{
		return nodes.get(n);
	}
	
	public TreeSet<Position> getPositions(int num)
	{
		if (num == 1)
			return positions1;
		else if (num == 2)
			return positions2;
		else return null;
	}

	public Node getPeer(Node n)
	{
		for (Node temp : nodes.keySet())
		{
			if ( ! temp.equals(n))
				return temp;
		}
		return null;
	}
	
	public int getPeer(int n)
	{
		for (Node temp : nodes.keySet())
		{
			if (temp.getId() != n)
				return temp.getId();
		}
		return 0;
	}
	
	public int[] getNodesId()
	{
		int[] result = new int[2];
		int i = 0;
		for (Node temp : nodes.keySet())
		{
			result[i] = temp.getId();
			i++;
		}
		return result;
		
	}
	

	public Duration getDuration() {
		return duration;
	}



	public Instant getStartTime() {
		return startTime;
	}



	public Instant getEndTime() {
		return endTime;
	}



	public synchronized void addPosition(Position p)
	{
		if (startTime == null)
		{
			Node n = p.getNodeRef();
			TreeSet<Position> pos = nodes.get(n);
			pos.add(p);
			startTime = p.getDate();
			endTime = p.getDate();
			updateDuration();
		}
		else
		{
			nodes.get(p.getNodeRef()).add(p);
			if (startTime.isAfter(p.getDate()))
			{
				startTime = p.getDate();
				updateDuration();
			}
			if (endTime.isBefore(p.getDate()))
			{
				endTime = p.getDate();
				updateDuration();
			}
		}
	}

	private void updateDuration()
	{
		duration = Duration.between(startTime, endTime);
	}

	@Override
	public int compareTo(Contact o) {
		// TODO Auto-generated method stub
		return startTime.compareTo(o.getStartTime());
	}
	
	public boolean startsBefore(Instant date)
	{
		return startTime.isBefore(date);
	}
	public boolean startsAfter(Instant date)
	{
		return startTime.isAfter(date);
	}
	public boolean endsBefore(Instant date)
	{
		return endTime.isBefore(date);
	}
	public boolean endsAfter(Instant date)
	{
		return endTime.isAfter(date);
	}
	
	@Override
	public String toString()
	{
		StringBuilder string = new StringBuilder();
		Object[] ns = nodes.keySet().toArray();
		string.append(((Node)ns[0]).getId());
		string.append(';');
		string.append(((Node)ns[1]).getId());
		string.append(';');
		string.append(startTime);
		string.append(';');
		string.append(endTime);
		string.append(';');
		string.append(duration.getSeconds());
		return string.toString();
	}
	
	public static Contact parseContact(String line)
	{
		Contact result = null;
		if (line == null || line.equals(""))
			return result;
		StringTokenizer tok = new StringTokenizer(line, ";");
		Node node1 = new Node(Integer.parseInt(tok.nextToken()));
		Node node2 = new Node(Integer.parseInt(tok.nextToken()));
		Instant startTime = Instant.parse(tok.nextToken());
		Instant endTime = Instant.parse(tok.nextToken());
		Contact contact = new Contact(node1, node2);
		contact.startTime = startTime;
		contact.endTime = endTime;
		contact.updateDuration();
		return contact;
	}

	public String toString(int nodeId) {
		StringBuilder string = new StringBuilder();
		Object[] ns = nodes.keySet().toArray();
		int node1, node2;
		if (((Node)ns[0]).getId() == nodeId)
		{
			node1 = ((Node)ns[0]).getId();
			node2 = ((Node)ns[1]).getId();
		}
		else 
		{
			node1 = ((Node)ns[1]).getId();
			node2 = ((Node)ns[0]).getId();
		}
		string.append(node1);
		string.append(';');
		string.append(node2);
		string.append(';');
		string.append(startTime);
		string.append(';');
		string.append(endTime);
		string.append(';');
		string.append(duration.getSeconds());
		return string.toString();
	}
}
