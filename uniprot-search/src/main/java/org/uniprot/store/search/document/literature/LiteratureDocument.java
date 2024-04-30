package org.uniprot.store.search.document.literature;

import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

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

    @Field("lit_abstract")
    private String litAbstract;

    @Field("author_group")
    private Set<String> authorGroups;

    @Field("citations_with")
    private List<String> citationsWith;

    @Field("literature_obj")
    private byte[] literatureObj;

    @Override
    public String getDocumentId() {
        return this.id;
    }
}
