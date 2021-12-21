package org.uniprot.store.spark.indexer.uniprot.converter;

import org.uniprot.core.uniprotkb.EntryInactiveReason;
import org.uniprot.core.uniprotkb.InactiveReasonType;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.DocumentConverter;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-12-21
 */
public class InactiveUniprotEntryConverter
        implements DocumentConverter<UniProtKBEntry, UniProtDocument> {

    @Override
    public UniProtDocument convert(UniProtKBEntry source) {
        EntryInactiveReason inactiveReason = source.getInactiveReason();
        InactiveReasonType type = inactiveReason.getInactiveReasonType();
        UniProtDocument document = new UniProtDocument();

        document.accession = source.getPrimaryAccession().getValue();
        if (Utils.notNull(source.getUniProtkbId())) {
            document.id = source.getUniProtkbId().getValue();
            if (!type.equals(InactiveReasonType.DEMERGED)) {
                document.idInactive = source.getUniProtkbId().getValue();
            }
        }

//        if (type.equals(InactiveReasonType.DELETED)) {
//            // todo why adding to content? i think it'd be better to add to accession
//            // content is a field added to default fields (qf), and should only be used for
//            // dynamic fields (because we can't add all dynamic fields to qf (too many).
//            document.content.add(source.getPrimaryAccession().getValue());
//
//            // TODO: 17/12/2021 Question: what does adding it to content do that is different from adding
//            // it to document.accession (line 23)?
//        }
       document.inactiveReason = type.getDisplayName();
        if (Utils.notNullNotEmpty(inactiveReason.getMergeDemergeTos())) {
            document.inactiveReason += ":" + String.join(",", inactiveReason.getMergeDemergeTos());
        }
        document.active = false;

        return document;
    }
}
