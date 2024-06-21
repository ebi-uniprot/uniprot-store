package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder.UNIPARC_ID_ATTRIB;

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
            document.id.add(source.getUniProtkbId().getValue());
            if (!type.equals(InactiveReasonType.DEMERGED)) {
                document.idInactive = source.getUniProtkbId().getValue();
            }
        }

        document.inactiveReason = type.getDisplayName();
        if (Utils.notNullNotEmpty(inactiveReason.getMergeDemergeTos())) {
            document.inactiveReason += ":" + String.join(",", inactiveReason.getMergeDemergeTos());
        }
        if (Utils.notNull(inactiveReason.getDeletedReason())) {
            document.inactiveReason += ":" + inactiveReason.getDeletedReason();
        }
        Object uniParcID = source.getExtraAttributeValue(UNIPARC_ID_ATTRIB);
        if (Utils.notNull(uniParcID)) {
            document.uniparcDeleted = String.valueOf(uniParcID);
        }
        document.active = false;

        return document;
    }
}
