package uk.ac.ebi.uniprot.indexer.uniprot.inactiveentry;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractInactiveUniProtEntryIterator implements Iterator<InactiveUniProtEntry> {
	protected boolean hasResult = false;
	private InactiveUniProtEntry nextEntry = null;

	abstract protected InactiveUniProtEntry nextEntry();

	@Override
	public boolean hasNext() {
		if (hasResult)
			return hasResult;

		nextEntry = nextEntry();
		return hasResult;

	}

	@Override
	public InactiveUniProtEntry next() {
		InactiveUniProtEntry currentOne = nextEntry;
		try {
			if (currentOne == null) {
				currentOne = nextEntry();
			}
			if (currentOne == null)
				return currentOne;
			List<InactiveUniProtEntry> group = new ArrayList<>();
			group.add(currentOne);
			do {
				InactiveUniProtEntry nextOne = nextEntry();
				nextEntry = nextOne;
				if (nextOne == null) {
					return currentOne;
				} else if (nextOne.getAccession().equals(currentOne.getAccession())) {
					group.add(nextOne);
				} else {

					break;
				}
			} while (true);
			if (group.size() == 1) {
				return currentOne;
			} else
				return InactiveUniProtEntry.merge(group);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
