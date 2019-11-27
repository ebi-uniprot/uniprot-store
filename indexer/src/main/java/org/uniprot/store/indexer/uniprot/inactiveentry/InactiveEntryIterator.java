package org.uniprot.store.indexer.uniprot.inactiveentry;

import java.util.Iterator;

/**
 * @author jluo
 * @date: 5 Sep 2019
 */
public interface InactiveEntryIterator extends Iterator<InactiveUniProtEntry>, AutoCloseable {}
