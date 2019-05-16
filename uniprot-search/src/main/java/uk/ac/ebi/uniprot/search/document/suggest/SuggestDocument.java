package uk.ac.ebi.uniprot.search.document.suggest;


import lombok.*;
import org.apache.solr.client.solrj.beans.Field;
import uk.ac.ebi.uniprot.search.document.Document;

import java.util.List;


@EqualsAndHashCode
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SuggestDocument implements Document {
    @Field("id")
    public String id;

    @Field("value")
    public String value;
    
    @Singular
    @Field("altValue")
    public List<String> altValues;

    @Field("dict")
    public String dictionary;

    @Override
    public String getDocumentId() {
        return id;
    }
}
