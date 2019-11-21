package org.uniprot.store.search.document.keyword;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

import java.nio.ByteBuffer;
import java.util.List;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KeywordDocument implements Document {

    @Field
    private String id;

    @Field
    private String name;

    @Field
    private List<String> parent;

    @Field
    private List<String> ancestor;

    @Field
    private List<String> content;

    @Field("keyword_obj")
    private ByteBuffer keywordObj;

    @Override
    public String getDocumentId() {
        return id;
    }

}
