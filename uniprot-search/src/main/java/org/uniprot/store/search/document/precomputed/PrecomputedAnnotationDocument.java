package org.uniprot.store.search.document.precomputed;

import java.util.ArrayList;
import java.util.List;

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

    @Singular("proteome")
    @Field("proteome")
    private List<String> proteome = new ArrayList<>();

    @Field("uniparc")
    private String uniparc;

    @Field("taxonomy_id")
    public Integer taxonomyId;

    @Override
    public String getDocumentId() {
        return accession;
    }
}
