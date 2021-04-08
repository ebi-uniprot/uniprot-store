package org.uniprot.store.search.document.keyword;

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
public class KeywordDocument implements Document {

    private static final long serialVersionUID = 1014031525256478859L;
    @Field private String id;

    @Field private String name;

    @Field private List<String> parent;

    @Field private List<String> ancestor;

    @Field("synonym")
    private List<String> synonyms;

    @Field private String definition;

    @Field private String category;

    @Field("keyword_obj")
    private ByteBuffer keywordObj;

    @Override
    public String getDocumentId() {
        return id;
    }
}
