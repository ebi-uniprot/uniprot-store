package org.uniprot.store.search.document.help;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

import java.nio.ByteBuffer;

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
    @Field
    private String id;
    @Field
    private String title;
    @Field
    private String summary;
    @Field
    private String description;
    @Field
    private String category;
    @Field
    private String section;


    @Field("help_obj")
    private ByteBuffer helpPageObj;


    @Override
    public String getDocumentId() {
        return id;
    }
}
