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

    private static final long serialVersionUID = 3330874625625289086L;
    @Field private String id;

    @Field private String doi;

    @Field private String title;

    @Field private Set<String> author;

    @Field private String journal;

    @Field private String published;

    @Field("is_uniprotkb_mapped")
    private boolean isUniprotkbMapped;

    @Field("is_computational_mapped")
    private boolean isComputationalMapped;

    @Field("is_community_mapped")
    private boolean isCommunityMapped;

    @Field("literature_obj")
    private ByteBuffer literatureObj;

    @Override
    public String getDocumentId() {
        return this.id;
    }
}
