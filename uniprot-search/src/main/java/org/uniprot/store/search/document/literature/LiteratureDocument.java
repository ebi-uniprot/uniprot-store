package org.uniprot.store.search.document.literature;

import java.nio.ByteBuffer;
import java.util.Set;

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
public class LiteratureDocument implements Document {

    @Field private String id;

    @Field private String doi;

    @Field private String title;

    @Field private Set<String> author;

    @Field private String journal;

    @Field private String published;

    @Field private boolean citedin;

    @Field private boolean mappedin;

    @Field private Set<String> content;

    @Field("mapped_protein")
    private Set<String> mappedProteins;

    @Field("literature_obj")
    private ByteBuffer literatureObj;

    @Override
    public String getDocumentId() {
        return this.id;
    }
}
