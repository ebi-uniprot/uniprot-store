package org.uniprot.store.indexer.publication.common;

import java.util.UUID;

import org.uniprot.core.json.parser.publication.MappedPublicationsJsonConfig;
import org.uniprot.core.publication.MappedPublications;
import org.uniprot.core.publication.MappedReference;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author sahmad
 * @created 16/12/2020
 */
public class PublicationUtils {

    private PublicationUtils(){

    }

    private static final ObjectMapper MAPPED_PUBLICATIONS_OBJECT_MAPPER =
            MappedPublicationsJsonConfig.getInstance().getFullObjectMapper();

    public static String getDocumentId() {
        return UUID.randomUUID().toString();
    }

    public static String docsToUpdateQuery(MappedReference reference) {
        return "accession:"
                + reference.getUniProtKBAccession().getValue()
                + " AND "
                + "pubmed_id:"
                + reference.getCitationId();
    }

    public static byte[] asBinary(MappedPublications reference) {
        try {
            return MAPPED_PUBLICATIONS_OBJECT_MAPPER.writeValueAsBytes(reference);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse MappedPublications to binary json: ", e);
        }
    }
}
