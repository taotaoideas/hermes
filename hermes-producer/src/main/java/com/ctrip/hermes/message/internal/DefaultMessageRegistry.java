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

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.message.ValveRegistry;
import com.ctrip.hermes.spi.Valve;
import com.ctrip.hermes.spi.internal.TracingMessageValve;

public class DefaultMessageRegistry extends ContainerHolder implements Initializable, ValveRegistry<Message<Object>> {

	private SortedSet<Triple<Valve<Message<Object>>, String, Integer>> m_tuples = new TreeSet<>(
	      new Comparator<Triple<Valve<Message<Object>>, String, Integer>>() {

		      @Override
		      public int compare(Triple<Valve<Message<Object>>, String, Integer> t1,
		            Triple<Valve<Message<Object>>, String, Integer> t2) {
			      return t1.getLast().compareTo(t2.getLast());
		      }
	      });

	private List<Valve<Message<Object>>> m_valves = Collections.emptyList();

	@Override
	public List<Valve<Message<Object>>> getValveList() {
		return m_valves;
	}

	@Override
	public void registerValve(Valve<Message<Object>> valve, String name, int order) {
		m_tuples.add(new Triple<Valve<Message<Object>>, String, Integer>(valve, name, order));

		ArrayList<Valve<Message<Object>>> newValves = new ArrayList<>();
		for (Triple<Valve<Message<Object>>, String, Integer> triple : m_tuples) {
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
