package uk.ac.ebi.uniprot.indexer.document.taxonomy;

import lombok.Builder;
import lombok.Getter;
import uk.ac.ebi.uniprot.indexer.document.Document;

import org.apache.solr.client.solrj.beans.Field;

import java.util.List;

@Builder
@Getter
public class TaxonomyDocument implements Document {

    @Field
    private String id;
    @Field("tax_id")
    private Long taxId;
    @Field("name_sort")
    private String nameSort;
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
    private Long swissprotCount;
    @Field
    private Long tremblCount;
    @Field
    private boolean hidden;
    @Field
    private boolean complete;
    @Field
    private boolean reference;
    @Field
    private boolean reviewed;
    @Field
    private boolean annotated;
    @Field
    private boolean active;
    @Field
    private boolean linked;
    @Field
    private List<String> content;
    @Field
    private List<String> strain;
    @Field("other_names")
    private List<String> otherNames;
    @Field
    private List<Long> host;
    @Field
    private List<Long> lineage;
    @Field
    private List<String> url;
}
