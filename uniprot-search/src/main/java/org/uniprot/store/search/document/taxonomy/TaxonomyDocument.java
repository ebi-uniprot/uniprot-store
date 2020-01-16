package org.uniprot.store.search.document.taxonomy;

import java.nio.ByteBuffer;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class TaxonomyDocument implements Document {

    @Field private String id;

    @Field("tax_id")
    private Long taxId;

    @Field private Long ancestor;
    @Field private String rank;
    @Field private String scientific;
    @Field private String common;
    @Field private String synonym;
    @Field private String mnemonic;
    @Field private boolean hidden;
    @Field private boolean active;
    @Field private boolean linked;
    @Field private boolean proteome;
    @Field private boolean reference;
    @Field private boolean reviewed;
    @Field private boolean annotated;
    @Field private List<String> content;
    @Field private List<String> strain;
    @Field private List<Long> host;
    @Field private List<Long> lineage;

    @Field("taxonomy_obj")
    private ByteBuffer taxonomyObj;

    @Override
    public String getDocumentId() {
        return id;
    }
}
