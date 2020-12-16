package org.uniprot.store.indexer.publication.common;

import java.util.Objects;

import org.uniprot.core.publication.MappedReference;

/**
 * @author sahmad
 * @created 16/12/2020
 */
public class PublicationUtils {
    private static final String ID_COMPONENT_SEPARATOR = "__";

    public static String computeDocumentId(MappedReference reference) {
        String sourceId = reference.getSource().getId();
        StringBuilder builder = new StringBuilder(reference.getUniProtKBAccession().getValue());
        builder.append(ID_COMPONENT_SEPARATOR).append(reference.getPubMedId());
        if (Objects.nonNull(sourceId)) {
            builder.append(ID_COMPONENT_SEPARATOR).append(sourceId);
        }
        return builder.toString();
    }
}
