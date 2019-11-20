package org.uniprot.store.search.document.subcell;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author lgonzales
 * @since 2019-07-11
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SubcellularLocationDocument implements Document {

    @Field
    private String id;

    @Field
    private String name;

    @Field
    private String category;

    @Field
    private List<String> content;

    @Field("subcellularlocation_obj")
    private ByteBuffer subcellularlocationObj;

    @Override
    public String getDocumentId() {
        return id;
    }
}
