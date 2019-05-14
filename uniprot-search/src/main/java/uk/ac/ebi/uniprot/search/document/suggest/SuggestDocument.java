package uk.ac.ebi.uniprot.search.document.suggest;


import lombok.Builder;
import org.apache.solr.client.solrj.beans.Field;
import uk.ac.ebi.uniprot.search.document.Document;


@Builder
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
