package dataset_rome_java;

import java.io.Serializable;
import java.time.Instant;
import java.util.StringTokenizer;

import com.vividsolutions.jts.geom.Coordinate;


public class Position implements Comparable<Position>, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int node;
	private Node nodeRef;
	private Instant date;
	private Coordinate coordinate;
	
	public Position(int node, Instant date, Coordinate coordinate)
	{
		this.node = node;
		this.date = date;
		this.coordinate = coordinate;
	}

	public Position(String node, Instant date, String lat, String lon)
	{
		int n = 0;
		Instant d = null;
		Coordinate c = null;
		n = Integer.parseInt(node);
		d = date;
		c = new Coordinate(Double.parseDouble(lat), Double.parseDouble(lon));
		this.node = n;
		this.date = d;
		this.coordinate = c;
	}

	public int getNode() {
		return node;
	}

	public Instant getDate() {
		return date;
	}

	public Coordinate getCoordinate() {
		return coordinate;
	}
	
	@Override
	public String toString()
	{
		return "" + node + ';' + date.toString() + ';' + coordinate.toString();
	}
	
	public static Position parse(String line)
	{
		StringTokenizer tok = new StringTokenizer(line, ";");
		int id = Integer.parseInt(tok.nextToken());
		Instant date = Instant.parse(tok.nextToken());
		double lat = Double.parseDouble(tok.nextToken("();, "));
		double lon = Double.parseDouble(tok.nextToken("();, "));
		Coordinate coo = new Coordinate(lat, lon);
		return new Position(id, date, coo);
	}


	@Override
	public int compareTo(Position o) {
		// TODO Auto-generated method stub
		int result = date.compareTo(o.getDate());
		if (result == 0)
		{
			return this.node - o.node;
		}
		return result;
	}

	public void addNodeRef(Node n) {
		// TODO Auto-generated method stub
		this.nodeRef = n;
	}

	public Node getNodeRef() {
		// TODO Auto-generated method stub
		return nodeRef;
	}
	
}
