package org.uniprot.store.spark.indexer.uniprot.converter;

import org.uniprot.core.uniprot.UniProtEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.store.job.common.converter.DocumentConverter;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-12-21
 */
public class InactiveUniprotEntryConverter
        implements DocumentConverter<UniProtEntry, UniProtDocument> {

    @Override
    public UniProtDocument convert(UniProtEntry source) {
        UniProtDocument document = new UniProtDocument();

        document.accession = source.getPrimaryAccession().getValue();
        if (Utils.notNull(source.getUniProtId())) {
            document.id = source.getUniProtId().getValue();
        }
        document.inactiveReason =
                source.getInactiveReason().getInactiveReasonType().toDisplayName();
        if (Utils.notNullOrEmpty(source.getInactiveReason().getMergeDemergeTo())) {
            document.inactiveReason +=
                    ":" + String.join(",", source.getInactiveReason().getMergeDemergeTo());
        }
        document.active = false;

        return document;
    }
}
