package org.uniprot.store.search.document.suggest;

import java.io.Serializable;
import java.util.Collections;
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

    @Field("suggest_id")
    public String suggestId;

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
        return suggestId;
    }

    // setting default field values in a builder following instructions here:
    // https://www.baeldung.com/lombok-builder-default-value
    public static class SuggestDocumentBuilder implements Serializable {
        private static final long serialVersionUID = 8082411551239368406L;
        private String importance = DEFAULT_IMPORTANCE;

        public SuggestDocument build() {
            String suggest = dictionary + "_" + id;
            List<String> altValueList = altValues;
            if (altValueList == null) {
                altValueList = Collections.emptyList();
            }
            return new SuggestDocument(suggest, id, value, importance, altValueList, dictionary);
        }
    }
}
