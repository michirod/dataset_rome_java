package dataset_rome_java;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;

import javax.vecmath.Point2d;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.GeodeticCalculator;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.operation.TransformException;

import java.util.HashSet;
import java.util.Iterator;
import java.util.HashMap;

public class ContactsManager {
	
	private HashMap<HashSet<Node>, TreeSet<Contact>> contacts;
	private Nodes nodes;
	private Instant firstInstant;
	private GeodeticCalculator calc;
	
	public ContactsManager(Nodes nodes)
	{
		this.nodes = nodes;
		this.contacts = new HashMap<HashSet<Node>, TreeSet<Contact>>();
		this.firstInstant = null;
		this.calc = new GeodeticCalculator(DefaultGeographicCRS.WGS84);
	}
	
	public Set<HashSet<Node>> getPairs()
	{
		return contacts.keySet();
	}
	
	private HashMap<HashSet<Node>, Contact> getFirstContacts()
	{
		HashMap<HashSet<Node>, Contact> result = new HashMap<HashSet<Node>, Contact>();
		for (HashSet<Node> pair : contacts.keySet())
		{
			result.put(pair, contacts.get(pair).first());
		}
		return result;
	}
	
	private HashMap<HashSet<Node>, Contact> getLastContacts()
	{
		HashMap<HashSet<Node>, Contact> result = new HashMap<HashSet<Node>, Contact>();
		for (HashSet<Node> pair : contacts.keySet())
		{
			result.put(pair, contacts.get(pair).last());
		}
		return result;
	}
	
	public synchronized void mergeWithNext(ContactsManager manNext)
	{
		for (HashSet<Node> pair : contacts.keySet())
		{
			Contact prev = contacts.get(pair).last();
			Contact next;
			try {
				next = manNext.contacts.get(pair).first();
			} catch (NullPointerException e) {
				continue;
			}
			if (prev.endsAfter(next.getStartTime().minusSeconds(Main.TEMP_GRANULARITY)))
			{
				if (prev.startsAfter(firstInstant))
				{
					mergeContact(prev, next);
					for (Node n : pair)
					{
						this.nodes.getNode(n.getId()).removeContact(prev);
					}
					this.contacts.get(pair).remove(prev);
				}
				else
				{
					mergeContact(next, prev);
					for (Node n : pair)
					{
						manNext.nodes.getNode(n.getId()).removeContact(next);
					}
					manNext.contacts.get(pair).remove(next);
				}
			}
			
		}
	}
	
	public synchronized Contact putContact(Node node1, Node node2, Position position1, Position position2)
	{
		Instant firstTime = null;
		Instant lastTime = null;
		if (position1.getDate().compareTo(position2.getDate()) < 0)
		{
			firstTime = position1.getDate();
			lastTime = position2.getDate();
		}
		else
		{
			firstTime = position2.getDate();
			lastTime = position1.getDate();
		}
		TreeSet<Contact> contactSet = getContactSet(node1, node2);
		if (contactSet.isEmpty())
		{
			Contact c = new Contact(node1, node2);
			c.addPosition(position1);
			c.addPosition(position2);
			node1.addContact(c);
			node2.addContact(c);
			contactSet.add(c);
			updateFirstInstant(firstTime);
			return c;
		}
		else
		{
			Contact result = null;
			Contact fake1 = new Contact(firstTime, null);
			Contact fake2 = new Contact(lastTime, null);
			boolean changed = false;
			Contact floor = contactSet.floor(fake1);
			Contact ceiling = contactSet.ceiling(fake2);
			NavigableSet<Contact> subContactSet;
			if (floor == null)
			{
				floor = contactSet.ceiling(fake1);
			}
			if (ceiling == null)
			{
				subContactSet = contactSet.tailSet(floor, true);
			}
			else
			{
				subContactSet = contactSet.subSet(floor, true, ceiling, true);
			}
			for (Contact c : subContactSet)
			{
				if (c.startsAfter(lastTime.plusSeconds(Main.TEMP_GRANULARITY)))
					continue;
				else if (c.endsBefore(firstTime.minusSeconds(Main.TEMP_GRANULARITY)))
					continue;
				else
				{
					c.addPosition(position1);
					c.addPosition(position2);
					changed = true;
					result = c;
					break;
				}
			}
			if (changed)
			{
				//scanMergeContacts(node1, node2, contactSet);
			}
			else
			{
				result = new Contact(node1, node2);
				result.addPosition(position1);
				result.addPosition(position2);
				node1.addContact(result);
				node2.addContact(result);
				contactSet.add(result);
				
			}
			return result;
		}
	}
	
	public synchronized void scanMergeContacts(Node node1, Node node2, TreeSet<Contact> contactSet)
	{
		Iterator<Contact> it = contactSet.iterator();
		Contact cur, next;
		if (it.hasNext())
		{
			cur = it.next();
			while (it.hasNext())
			{
				next = it.next();
				if (cur.endsAfter(next.getStartTime().minusSeconds(Main.TEMP_GRANULARITY)))
				{
					mergeContact(next, cur);
					node1.removeContact(next);
					node2.removeContact(next);
					it.remove();
				}
				else
				{
					cur = next;
				}
			}
		}
	}
	/**
	 * Copy all Positions from Contact {@code}from to Contact {@code}to. 
	 * Contact From should be deleted from each node after this function returns.
	 * @param from 
	 * @param to
	 */
	private void mergeContact(Contact from, Contact to)
	{
		for (Position p : from.getPositions(1))
			to.addPosition(p);
		for (Position p : from.getPositions(2))
			to.addPosition(p);
	}
	
	private void scanMergeContacts(Node node1, Node node2)
	{
		scanMergeContacts(node1, node2, getContactSet(node1, node2));
	}
	
	private void scanMergeContacts()
	{
		Iterator<Node> it = null;
		Node node1, node2;
		for(HashSet<Node> pair : contacts.keySet())
		{
			it = pair.iterator();
			node1 = it.next();
			node2 = it.next();
			scanMergeContacts(node1, node2);
		}
	}
	
	public synchronized void calculateContacts()
	{
		
		for (int cur : nodes.getNodes().keySet())
		{
			Iterator<Integer> itN = nodes.getNodes().navigableKeySet().tailSet(cur, false).iterator();
			Node curN = nodes.getNodes().get(cur);
			Node peerN = null;
			Instant start = null, end = null;
			SortedMap<Instant, Position> subMap = null;
			double distance = 0;
			
			while (itN.hasNext())
			{
				peerN = nodes.getNodes().get(itN.next());
				for (Position curP : curN.getPositions().values())
				{
					if (curN != curP.getNodeRef())
					{
						System.out.println("ERROR: position node reference does not correspond to node itself. Node: " + curN.getId());
						continue;
					}
					start = curP.getDate().minusSeconds(Main.TEMP_GRANULARITY);
					end = curP.getDate().plusSeconds(Main.TEMP_GRANULARITY);
					subMap = peerN.getPositions().subMap(start, true, end, true);
					for (Position peerP : subMap.values())
					{
						if (peerN != peerP.getNodeRef())
						{
							System.out.println("ERROR: position node reference does not correspond to node itself. Node: " + curN.getId());
							continue;
						}
						//distance = JTS.orthodromicDistance(peerP.getCoordinate(), curP.getCoordinate(), DefaultGeographicCRS.WGS84);
						calc.setStartingGeographicPoint(curP.getCoordinate().y, curP.getCoordinate().x);
						calc.setDestinationGeographicPoint(peerP.getCoordinate().y, peerP.getCoordinate().x);
						distance = calc.getOrthodromicDistance();
						//distance =  peerP.getCoordinate().distance(curP.getCoordinate());
						if (distance < Main.SPAT_GRANULARITY)
						{
							Contact c = putContact(curN, peerN, curP, peerP);	
							//System.out.println("Found contact: " + c);
							//System.out.println("Distance " + distance);
						}
					}

				}

			}

		}
		scanMergeContacts();
	}
	
	private void updateFirstInstant(Instant instant)
	{
		if (firstInstant == null || firstInstant.isAfter(instant))
			firstInstant = instant.truncatedTo(ChronoUnit.DAYS);
	}
	
	private TreeSet<Contact> getContactSet(Node node1, Node node2)
	{
		HashSet<Node> temp = new HashSet<Node>(2);
		temp.add(node1);
		temp.add(node2);
		if (! contacts.containsKey(temp))
			contacts.put(temp, new TreeSet<Contact>());
		return contacts.get(temp);
	}

	public void printContactManagerBin(ObjectOutputStream outputStream) throws IOException
	{
		outputStream.writeObject(this);
	}

}