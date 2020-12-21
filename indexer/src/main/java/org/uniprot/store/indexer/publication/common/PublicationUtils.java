package org.uniprot.store.indexer.publication.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.uniprot.core.json.parser.publication.MappedPublicationsJsonConfig;
import org.uniprot.core.publication.MappedPublications;
import org.uniprot.core.publication.MappedReference;
import org.uniprot.core.publication.MappedReferenceType;
import org.uniprot.core.publication.UniProtKBMappedReference;
import org.uniprot.store.search.document.publication.PublicationDocument;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.uniprot.core.publication.MappedReferenceType.COMMUNITY;

/**
 * @author sahmad
 * @created 16/12/2020
 */
public class PublicationUtils {
    private static final String ID_COMPONENT_SEPARATOR = "__";
    private static final ObjectMapper MAPPED_PUBLICATIONS_OBJECT_MAPPER =
            MappedPublicationsJsonConfig.getInstance().getFullObjectMapper();

    public static String computeDocumentId(MappedReference reference) {
        String sourceId = reference.getSource().getId();
        StringBuilder builder = new StringBuilder(reference.getUniProtKBAccession().getValue());
        builder.append(ID_COMPONENT_SEPARATOR).append(reference.getPubMedId());
        if (Objects.nonNull(sourceId)) {
            builder.append(ID_COMPONENT_SEPARATOR).append(sourceId);
        }
        return builder.toString();
    }

    public static String computeDocumentId(
            UniProtKBMappedReference reference, MappedReferenceType type) {
        return computeDocumentId(reference) + ID_COMPONENT_SEPARATOR + type.getIntValue();
    }

    public static String docsToUpdateQuery(MappedReference reference) {
        return "accession:"
                + reference.getUniProtKBAccession().getValue()
                + " AND "
                + "pubmed_id:"
                + reference.getPubMedId();
    }

    public static Set<String> getMergedCategories(
            MappedReference reference, PublicationDocument doc) {
        Set<String> categories = new HashSet<>();
        categories.addAll(doc.getCategories());
        categories.addAll(reference.getSourceCategories());
        return categories;
    }

    public static Set<Integer> getMergedTypes(PublicationDocument doc) {
        Set<Integer> categories = new HashSet<>(doc.getTypes());
        categories.add(COMMUNITY.getIntValue());
        return categories;
    }

    public static byte[] asBinary(MappedPublications reference) {
        try {
            return MAPPED_PUBLICATIONS_OBJECT_MAPPER.writeValueAsBytes(reference);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse MappedPublications to binary json: ", e);
        }
    }
}
