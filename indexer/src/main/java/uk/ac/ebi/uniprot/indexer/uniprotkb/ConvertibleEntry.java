package uk.ac.ebi.uniprot.indexer.uniprotkb;

import lombok.EqualsAndHashCode;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.indexer.document.uniprot.UniProtDocument;

/**
 * Represents a {@link UniProtEntry} and an associated {@link UniProtDocument}. The purpose of this class
 * is to record the pair of entities through a Spring Batch process and keep them together, when Spring Batch
 * natural separates the two in different phases (entries are converted to documents in different parts of the Step).
 * This means we can now, for example, when writing documents, if there is a write error, then we can also
 * write the corresponding {@link UniProtEntry} to a log file for future reference / reprocessing.
 *
 * Created 12/04/19
 *
 * @author Edd
 */
@EqualsAndHashCode
public class ConvertibleEntry {
    private final UniProtEntry entry;
    private UniProtDocument document;

    private ConvertibleEntry(UniProtEntry entry) {
        this.entry = entry;
    }

    public static ConvertibleEntry createConvertableEntry(UniProtEntry entry) {
        return new ConvertibleEntry(entry);
    }

    public void convertsTo(UniProtDocument document) {
        this.document = document;
    }

    public UniProtEntry getEntry() {
        return entry;
    }

    public UniProtDocument getDocument() {
        return document;
    }
}
