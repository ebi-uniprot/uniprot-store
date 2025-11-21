package org.uniprot.store.search.document.suggest;

import static org.uniprot.core.util.Utils.addOrIgnoreEmpty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.ListUtils;
import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

import lombok.*;

@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
@Builder(builderClassName = "SuggestDocumentBuilder")
public class SuggestDocument implements Document {
    public static final String DEFAULT_IMPORTANCE = "medium";
    private static final long serialVersionUID = -447760324653086807L;

    @Field("suggest_id")
    public String suggestId;

    @Field("id")
    public String id;

    @Field("value")
    public String value;

    @Field("importance")
    public String importance = DEFAULT_IMPORTANCE;

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

        public SuggestDocumentBuilder altValue(String altValue) {
            this.altValues = ListUtils.defaultIfNull(this.altValues, new ArrayList<>());
            addOrIgnoreEmpty(altValue, this.altValues);
            return this;
        }

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
