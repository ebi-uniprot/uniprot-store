package uk.ac.ebi.uniprot.search.document.dbxref;

import lombok.Builder;
import lombok.Getter;
import org.apache.solr.client.solrj.beans.Field;

import uk.ac.ebi.uniprot.search.document.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Builder
@Getter
public class CrossRefDocument implements Document {
    @Field
    private String accession;
    @Field
    private String abbrev;
    @Field
    private String name;
    @Field("pubmed_id")
    private String pubMedId;
    @Field("doi_id")
    private String doiId;
    @Field("link_type")
    private String linkType;
    @Field
    private String server;
    @Field("db_url")
    private String dbUrl;
    @Field("category_str")
    private String category;
    @Field("content")
    private List<String> content =new ArrayList<>();
    @Field("category_facet")
    private String categoryFacet;
    @Field("uniprotkb_entry_count")
    private Long uniprotCount;
}
