package org.uniprot.store.search.document.dbxref;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CrossRefDocument implements Document {
    private static final long serialVersionUID = -3873122856685299890L;
    @Field private String accession;
    @Field private String abbrev;

    @Field("name_only")
    private String name;

    @Field("pubmed_id")
    private String pubMedId;

    @Field("doi_id")
    private String doiId;

    @Field("link_type")
    private String linkType;

    @Field private String server;

    @Field("db_url")
    private String dbUrl;

    @Field("category_str")
    private String categoryStr;

    @Field("category_facet")
    private String category;

    @Field("reviewed_protein_count")
    private Long reviewedProteinCount;

    @Field("unreviewed_protein_count")
    private Long unreviewedProteinCount;

    @Override
    public String getDocumentId() {
        return accession;
    }
}
