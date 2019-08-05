package org.uniprot.store.indexer.common.model;

/**
 * Created 18/04/19
 *
 * @author Edd
 */
public class AbstractEntryDocumentPair<E, D> implements EntryDocumentPair<E, D> {
    private final E entry;
    private D document;

    public AbstractEntryDocumentPair(E entry) {
        this.entry = entry;
    }

    @Override
    public E getEntry() {
        return this.entry;
    }

    @Override
    public D getDocument() {
        return this.document;
    }

    @Override
    public void setDocument(D document) {
        this.document = document;
    }
}
