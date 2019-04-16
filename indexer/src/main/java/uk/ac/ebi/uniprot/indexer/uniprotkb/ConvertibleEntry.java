package uk.ac.ebi.uniprot.indexer.uniprotkb;

import lombok.EqualsAndHashCode;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.indexer.document.uniprot.UniProtDocument;

/**
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
