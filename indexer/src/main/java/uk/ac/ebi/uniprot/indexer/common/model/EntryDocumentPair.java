package uk.ac.ebi.uniprot.indexer.common.model;

import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;

/**
 * Represents a pair of entry and document, with the purpose of recording this pair of entities throughout a Spring Batch
 * process and keeping them together, when Spring Batch would naturally separate them in different phases
 * (entries are converted to documents in different parts of the Step).
 * <p>
 * This means we can now, for example, when writing documents, if there is a write error, then we can also
 * write the corresponding {@link UniProtEntry} to a log file for future reference / reprocessing.
 * <p>
 * Created 12/04/19
 *
 * @author Edd
 */
public interface EntryDocumentPair<E, D> {
    E getEntry();

    D getDocument();

    void setDocument(D document);
}
