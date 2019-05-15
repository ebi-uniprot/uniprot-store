package uk.ac.ebi.uniprot.search.document.suggest;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.solr.client.solrj.beans.Field;
import uk.ac.ebi.uniprot.search.document.Document;


@EqualsAndHashCode
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SuggestDocument implements Document {
    @Field("id")
    public String id;

    @Field("value")
    public String value;

    @Field("altValue")
    public String altValue;

    @Field("dict")
    public String dictionary;
}
