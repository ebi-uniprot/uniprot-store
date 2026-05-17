package org.uniprot.store.search.document.precomputed;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Builder(toBuilder = true)
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PrecomputedAnnotationDocument implements Document {
    @Field("accession")
    private String accession;

    @Field("proteome")
    private String proteome;

    @Override
    public String getDocumentId() {
        return accession;
    }
}
