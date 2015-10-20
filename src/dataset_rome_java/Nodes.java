package dataset_rome_java;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.util.TreeMap;

public class Nodes {

	private TreeMap<Integer, Node> nodes;
	
	public TreeMap<Integer, Node> getNodes() {
		return nodes;
	}
	
	public Node getNode(int id)
	{
		return nodes.get(id);
	}

	public Nodes()
	{
		this.nodes = new TreeMap<Integer, Node>();
	}
	
	public synchronized void insertPosition(Position p)
	{
		int id = p.getNode();
		Node n = nodes.get(id);
		if (n == null)
		{
			n = new Node(id);
			nodes.put(id, n);
		}
		n.addPosition(p);
		//p.addNodeRef(n);
	}
	
	public void printNodes(PrintStream writer)
	{
		for (Node n : nodes.values())
		{
			n.writeNode(writer);
		}
	}
	
	public void printNodesBin(ObjectOutputStream outputStream) throws IOException
	{
		for (Node n : nodes.values())
		{
			n.writeNodeBin(outputStream);
		}
	}
	
}
