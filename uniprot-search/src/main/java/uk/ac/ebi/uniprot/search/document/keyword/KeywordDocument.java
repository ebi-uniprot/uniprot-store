package uk.ac.ebi.uniprot.search.document.keyword;

import lombok.Builder;
import lombok.Getter;
import org.apache.solr.client.solrj.beans.Field;
import uk.ac.ebi.uniprot.search.document.Document;

import java.nio.ByteBuffer;
import java.util.List;

@Getter
@Builder
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
