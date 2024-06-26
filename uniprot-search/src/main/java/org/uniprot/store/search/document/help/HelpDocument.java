package org.uniprot.store.search.document.help;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

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
    @Field private String content;

    @Field("date_modified")
    private Date lastModified;

    @Field("content_original")
    private String contentOriginal;

    @Field("category")
    private List<String> categories;

    @Field("type")
    private String type;

    @Field("release_date")
    private Date releaseDate;

    private Map<String, List<String>> matches;

    @Override
    public String getDocumentId() {
        return id;
    }
}
