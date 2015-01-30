package com.ctrip.hermes.message.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.message.MessageRegistry;
import com.ctrip.hermes.spi.MessageValve;
import com.ctrip.hermes.spi.internal.TracingMessageValve;

public class DefaultMessageRegistry extends ContainerHolder implements Initializable, MessageRegistry {

	private SortedSet<Triple<MessageValve, String, Integer>> m_tuples = new TreeSet<Triple<MessageValve, String, Integer>>(
	      new Comparator<Triple<MessageValve, String, Integer>>() {

		      @Override
		      public int compare(Triple<MessageValve, String, Integer> t1, Triple<MessageValve, String, Integer> t2) {
			      return t1.getLast().compareTo(t2.getLast());
		      }
	      });

	private List<MessageValve> m_valves = Collections.emptyList();

	@Override
	public List<MessageValve> getValveList() {
		return m_valves;
	}

	@Override
	public void registerValve(MessageValve valve, String name, int order) {
		m_tuples.add(new Triple<MessageValve, String, Integer>(valve, name, order));

		ArrayList<MessageValve> newValves = new ArrayList<MessageValve>();
		for (Triple<MessageValve, String, Integer> triple : m_tuples) {
			newValves.add(triple.getFirst());
		}

		m_valves = newValves;
	}

	@Override
	public void initialize() throws InitializationException {
		doRegister(TracingMessageValve.ID, 0);
	}

	private void doRegister(String id, int order) {
		registerValve(lookup(MessageValve.class, id), id, order);
	}
}
