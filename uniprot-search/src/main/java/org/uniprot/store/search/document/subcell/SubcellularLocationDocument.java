package org.uniprot.store.search.document.subcell;

import java.util.List;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * @author lgonzales
 * @since 2019-07-11
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SubcellularLocationDocument implements Document {

    private static final long serialVersionUID = -4478266706383173211L;
    @Field private String id;

    @Field private String name;

    @Field private String category;

    @Field("subcellularlocation_obj")
    private byte[] subcellularlocationObj;

    @Field private String definition;

    @Field("synonym")
    private List<String> synonyms;

    @Override
    public String getDocumentId() {
        return id;
    }
}
