package uk.ac.ebi.uniprot.search.document.taxonomy;

import lombok.Builder;
import lombok.Getter;
import org.apache.solr.client.solrj.beans.Field;
import uk.ac.ebi.uniprot.search.document.Document;

import java.nio.ByteBuffer;
import java.util.List;

@Builder
@Getter
public class TaxonomyDocument implements Document {

    @Field
    private String id;
    @Field("tax_id")
    private Long taxId;
    @Field
    private Long ancestor;
    @Field
    private String rank;
    @Field
    private String scientific;
    @Field
    private String common;
    @Field
    private String synonym;
    @Field
    private String mnemonic;
    @Field
    private boolean hidden;
    @Field
    private boolean active;
    @Field
    private boolean linked;
    @Field
    private List<String> content;
    @Field
    private List<String> strain;
    @Field
    private List<Long> host;
    @Field
    private List<Long> lineage;

    @Field("taxonomy_obj")
    private ByteBuffer taxonomyObj;

}
