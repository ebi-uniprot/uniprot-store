package org.uniprot.store.spark.indexer.uniprot.converter;

import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.store.job.common.converter.DocumentConverter;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-12-21
 */
public class InactiveUniprotEntryConverter
        implements DocumentConverter<UniProtKBEntry, UniProtDocument> {

    @Override
    public UniProtDocument convert(UniProtKBEntry source) {
        UniProtDocument document = new UniProtDocument();

        document.accession = source.getPrimaryAccession().getValue();
        if (Utils.notNull(source.getUniProtkbId())) {
            document.id = source.getUniProtkbId().getValue();
        }
        document.inactiveReason =
                source.getInactiveReason().getInactiveReasonType().getDisplayName();
        if (Utils.notNullNotEmpty(source.getInactiveReason().getMergeDemergeTos())) {
            document.inactiveReason +=
                    ":" + String.join(",", source.getInactiveReason().getMergeDemergeTos());
        }
        document.active = false;

        return document;
    }
}
