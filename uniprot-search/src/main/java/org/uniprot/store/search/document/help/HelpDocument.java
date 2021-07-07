package org.uniprot.store.search.document.help;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

/**
 * @author sahmad
 * @created 05/07/2021
 */
@Builder(toBuilder = true)
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class HelpDocument implements Document {

    private static final long serialVersionUID = 2979685307668121593L;
    @Field private String id;
    @Field private String title;
    @Field private String content; // content without meta

    @Field("category")
    private List<String> categories;

    @Override
    public String getDocumentId() {
        return id;
    }
}
