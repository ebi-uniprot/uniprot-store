package org.uniprot.store.indexer.uniprotkb.processor;

import static org.uniprot.core.util.Utils.*;

import org.uniprot.store.indexer.uniprot.inactiveentry.InactiveUniProtEntry;
import org.uniprot.store.search.document.DocumentConverter;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * Created 18/04/19
 *
 * @author Edd
 */
public class InactiveEntryConverter
        implements DocumentConverter<InactiveUniProtEntry, UniProtDocument> {
    @Override
    public UniProtDocument convert(InactiveUniProtEntry source) {
        UniProtDocument document = new UniProtDocument();

        document.accession = source.getAccession();
        if (notNull(source.getId())) {
            document.id.add(source.getId());
            if (!source.getReason().equalsIgnoreCase("demerged")) {
                document.idInactive = source.getId();
            }
        }

        document.inactiveReason = source.getInactiveReason();
        if (notNullNotEmpty(source.getDeletedReason())) {
            document.inactiveReason += ":" + source.getDeletedReason();
        }
        if (notNullNotEmpty(source.getUniParcId())) {
            document.deletedEntryUniParc = source.getUniParcId();
        }
        document.active = false;

        return document;
    }
}
