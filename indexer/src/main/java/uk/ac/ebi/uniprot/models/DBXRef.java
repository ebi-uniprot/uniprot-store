package uk.ac.ebi.uniprot.models;

import org.apache.solr.client.solrj.beans.Field;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DBXRef {
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

    public static class DBXRefBuilder{
        private String accession;
        private String abbrev;
        private String name;
        private String pubMedId;
        private String doiId;
        private String linkType;
        private String server;
        private String dbUrl;
        private String category;

        public DBXRefBuilder accession(String accession){
            this.accession = accession;
            return this;
        }
        public DBXRefBuilder abbr(String abbrev){
            this.abbrev = abbrev;
            return this;
        }
        public DBXRefBuilder name(String name){
            this.name = name;
            return this;
        }
        public DBXRefBuilder pubMedId(String pubMedId){
            this.pubMedId = pubMedId;
            return this;
        }
        public DBXRefBuilder doiId(String doiId){
            this.doiId = doiId;
            return this;
        }
        public DBXRefBuilder linkType(String linkType){
            this.linkType = linkType;
            return this;
        }
        public DBXRefBuilder server(String server){
            this.server = server;
            return this;
        }
        public DBXRefBuilder dbUrl(String dbUrl){
            this.dbUrl = dbUrl;
            return this;
        }
        public DBXRefBuilder category(String category){
            this.category = category;
            return this;
        }
        public DBXRef build(){
            DBXRef dbxRef = new DBXRef();
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
