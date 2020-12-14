package org.uniprot.store.search.document.publication;

import java.nio.ByteBuffer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

/**
 * @author sahmad
 * @created 04/12/2020
 */
@Builder(toBuilder = true)
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PublicationDocument implements Document {

    // note: thinking to keep flat so that easier to have different release cycles in future
    @Field("id")
    private String id; // unique id composed of accession and pmid & orchid (if present)

    @Field("accession")
    private String accession;

    @Field("pubmed_id")
    private String pubMedId;

    @Field("type")
    private int type;

    // to be MappedReference
    @Field("publication_obj")
    private ByteBuffer publicationMappedReference;

    @Override
    public String getDocumentId() {
        return id;
    }
}
