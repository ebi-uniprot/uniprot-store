package org.uniprot.store.indexer.uniprotkb.processor;

import org.uniprot.core.util.Utils;
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
        if (Utils.notNull(source.getId())) {
            document.id = source.getId();
            if (!source.getReason().equalsIgnoreCase("demerged")) {
                document.idInactive = source.getId();
            }
        }
        // TODO: 17/12/2021 no need to add accession to content when already added to accession
        // field in document.
        //        if (source.getReason().equalsIgnoreCase("deleted")) {
        //            document.content.add(source.getAccession());
        //        }
        document.inactiveReason = source.getInactiveReason();
        document.active = false;

        return document;
    }
}
