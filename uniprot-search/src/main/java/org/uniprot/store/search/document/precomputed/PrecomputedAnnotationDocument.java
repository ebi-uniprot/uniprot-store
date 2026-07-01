package org.uniprot.store.search.document.precomputed;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

import lombok.*;

@Builder(toBuilder = true)
@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class PrecomputedAnnotationDocument implements Document {
    @Field("accession")
    private String accession;

    @Field("uniparc")
    private String uniparc;

    @Field("taxonomy_id")
    public Integer taxonomyId;

    @Override
    public String getDocumentId() {
        return accession;
    }
}
