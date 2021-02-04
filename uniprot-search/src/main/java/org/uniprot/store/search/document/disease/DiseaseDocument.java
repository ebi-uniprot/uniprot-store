package org.uniprot.store.search.document.disease;

import java.nio.ByteBuffer;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DiseaseDocument implements Document {
    private static final long serialVersionUID = -427975560658449173L;
    @Field private String id;
    @Field private List<String> name; // search by name

    @Field("disease_obj")
    private ByteBuffer diseaseObj;

    @Override
    public String getDocumentId() {
        return id;
    }
}
