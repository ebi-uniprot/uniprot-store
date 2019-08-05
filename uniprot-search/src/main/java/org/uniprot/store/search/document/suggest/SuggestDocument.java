package org.uniprot.store.search.document.suggest;


import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Singular;
import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

import java.util.List;


@EqualsAndHashCode
@Builder(builderClassName = "SuggestDocumentBuilder")
public class SuggestDocument implements Document {
    static final String DEFAULT_IMPORTANCE = "medium";

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
    public static class SuggestDocumentBuilder {
        private String importance = DEFAULT_IMPORTANCE;
    }
}
