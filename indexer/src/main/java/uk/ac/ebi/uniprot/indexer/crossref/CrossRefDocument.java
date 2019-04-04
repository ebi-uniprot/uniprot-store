package uk.ac.ebi.uniprot.indexer.crossref;

import org.apache.solr.client.solrj.beans.Field;
import uk.ac.ebi.uniprot.indexer.common.model.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    @Field
    private List<String> content;
    @Field("category_facet")
    private String categoryFacet;

    public static class CrossRefDocumentBuilder {
        private String accession;
        private String abbrev;
        private String name;
        private String pubMedId;
        private String doiId;
        private String linkType;
        private String server;
        private String dbUrl;
        private String category;

        public CrossRefDocumentBuilder accession(String accession){
            this.accession = accession;
            return this;
        }
        public CrossRefDocumentBuilder abbr(String abbrev){
            this.abbrev = abbrev;
            return this;
        }
        public CrossRefDocumentBuilder name(String name){
            this.name = name;
            return this;
        }
        public CrossRefDocumentBuilder pubMedId(String pubMedId){
            this.pubMedId = pubMedId;
            return this;
        }
        public CrossRefDocumentBuilder doiId(String doiId){
            this.doiId = doiId;
            return this;
        }
        public CrossRefDocumentBuilder linkType(String linkType){
            this.linkType = linkType;
            return this;
        }
        public CrossRefDocumentBuilder server(String server){
            this.server = server;
            return this;
        }
        public CrossRefDocumentBuilder dbUrl(String dbUrl){
            this.dbUrl = dbUrl;
            return this;
        }
        public CrossRefDocumentBuilder category(String category){
            this.category = category;
            return this;
        }
        public CrossRefDocument build(){
            CrossRefDocument dbxRef = new CrossRefDocument();
            dbxRef.accession = this.accession;
            dbxRef.abbrev = this.abbrev;
            dbxRef.name = this.name;
            dbxRef.pubMedId = this.pubMedId;
            dbxRef.doiId = this.doiId;
            dbxRef.linkType = this.linkType;
            dbxRef.server = this.server;
            dbxRef.dbUrl = this.dbUrl;
            dbxRef.category = this.category;
            dbxRef.content = new ArrayList<>();
            dbxRef.content.addAll(
                    Arrays.asList(dbxRef.accession, dbxRef.abbrev, dbxRef.name, dbxRef.pubMedId,
                            dbxRef.doiId, dbxRef.linkType, dbxRef.category, dbxRef.dbUrl, dbxRef.server));
            return dbxRef;
        }
    }

    public String getAccession() {
        return accession;
    }

    public String getAbbrev() {
        return abbrev;
    }

    public String getName() {
        return name;
    }

    public String getPubMedId() {
        return pubMedId;
    }

    public String getDoiId(){
        return doiId;
    }

    public String getLinkType() {
        return linkType;
    }

    public String getServer() {
        return server;
    }

    public String getDbUrl() {
        return dbUrl;
    }

    public String getCategory() {
        return category;
    }

    public String getCategoryFacet() {
        return categoryFacet;
    }

    @Override
    public String toString() {
        return "DBXRef{" +
                "accession='" + accession + '\'' +
                ", abbrev='" + abbrev + '\'' +
                ", name='" + name + '\'' +
                ", pubMedId='" + pubMedId + '\'' +
                ", doiId='" + doiId + '\'' +
                ", linkType='" + linkType + '\'' +
                ", server='" + server + '\'' +
                ", dbUrl='" + dbUrl + '\'' +
                ", category='" + category + '\'' +
                '}';
    }
}
