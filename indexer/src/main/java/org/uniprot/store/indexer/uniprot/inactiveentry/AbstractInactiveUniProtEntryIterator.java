package org.uniprot.store.indexer.uniprot.inactiveentry;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public abstract class AbstractInactiveUniProtEntryIterator implements InactiveEntryIterator {
    protected boolean hasResult = false;
    private InactiveUniProtEntry nextEntry = null;

    protected abstract InactiveUniProtEntry nextEntry();

    @Override
    public boolean hasNext() {
        if (hasResult) return hasResult;

        nextEntry = nextEntry();
        return hasResult;
    }

    @Override
    public InactiveUniProtEntry next() {
        InactiveUniProtEntry currentOne = nextEntry;
        if (currentOne == null) {
            currentOne = nextEntry();
        }
        if (currentOne == null) throw new NoSuchElementException();
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
        } else return InactiveUniProtEntry.merge(group);
    }
}
