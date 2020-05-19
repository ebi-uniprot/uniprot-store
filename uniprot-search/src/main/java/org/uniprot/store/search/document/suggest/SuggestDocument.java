package org.uniprot.store.search.document.suggest;

import java.io.Serializable;
import java.util.List;

import lombok.*;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
@Builder(builderClassName = "SuggestDocumentBuilder")
public class SuggestDocument implements Document {
    public static final String DEFAULT_IMPORTANCE = "medium";
    private static final long serialVersionUID = 2126936244930669278L;

    @Field("id")
    public String id;

    @Field("value")
    public String value;

    @Field("importance")
    public String importance = DEFAULT_IMPORTANCE;

    @Singular
    @Field("altValue")
    public List<String> altValues;

    @Field("dict")
    public String dictionary;

    @Override
    public String getDocumentId() {
        return id;
    }

    // setting default field values in a builder following instructions here:
    // https://www.baeldung.com/lombok-builder-default-value
    public static class SuggestDocumentBuilder implements Serializable {
        private static final long serialVersionUID = 8082411551239368406L;
        private String importance = DEFAULT_IMPORTANCE;
    }
}
