package org.uniprot.store.search.document.publication;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

import java.nio.ByteBuffer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * @author sahmad
 * @created 04/12/2020
 */
@Builder(toBuilder = true)
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PublicationDocument implements Document {

    @Field("id")
    private String id;// unique id composed of accession and pmid

    @Field("accession")
    private String accession;

    @Field("pubmed_id")
    private String pubMedId;

    @Field("publication_obj")
    private ByteBuffer publicationObj;

    @Override
    public String getDocumentId() {
        return id;
    }
}
