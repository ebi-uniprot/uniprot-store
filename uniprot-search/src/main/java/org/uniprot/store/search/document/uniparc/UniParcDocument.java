package org.uniprot.store.search.document.uniparc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

import lombok.*;

@Builder(toBuilder = true)
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class UniParcDocument implements Document {

    private static final long serialVersionUID = 1282262932688163225L;

    @Field("upi")
    private String upi;

    @Singular
    @Field("checksum")
    private Set<String> sequenceChecksums; // crc64 and md5

    @Field("length")
    private int seqLength;

    @Singular
    @Field("database_facet")
    private List<Integer> databasesFacets;

    @Field("database")
    @Singular
    private Set<String> databases;

    @Field("dbid")
    @Singular
    private Set<String> dbIds;

    @Singular
    @Field("active")
    private Set<String> actives;

    @Singular
    @Field("gene")
    private Set<String> geneNames;

    @Singular
    @Field("protein_name")
    private Set<String> proteinNames;

    @Singular
    @Field("proteome")
    private Set<String> proteomes = new HashSet<>();

    @Singular
    @Field("proteomecomponent")
    public Set<String> proteomeComponents = new HashSet<>();

    @Singular
    @Field("taxonomy_name")
    private Set<String> organismTaxons = new HashSet<>();

    @Singular private Set<String> organismNames = new HashSet<>();

    @Singular
    @Field("feature_id")
    private Set<String> featureIds = new HashSet<>();

    @Singular private Set<Integer> taxLineageIds = new HashSet<>();

    @Singular
    @Field("uniprotkb")
    private List<String> uniprotAccessions;

    @Singular
    @Field("isoform")
    private List<String> uniprotIsoforms;

    @Singular private Set<Integer> organismIds;

    @Override
    public String getDocumentId() {
        return upi;
    }

    @Field("taxonomy_id")
    public void setTaxLineageIds(List<Integer> taxLineageIds) {
        this.taxLineageIds = new HashSet<>(taxLineageIds);
    }

    @Field("organism_name")
    public void setOrganismNames(List<String> organismNames) {
        this.organismNames = new HashSet<>(organismNames);
    }

    @Field("organism_id")
    public void setOrganismIds(List<Integer> organismIds) {
        this.organismIds = new HashSet<>(organismIds);
    }

    public static class UniParcDocumentBuilder implements Serializable {
        private static final long serialVersionUID = 8664627033779718863L;

        public UniParcDocumentBuilder() {
            super();
            databasesFacets = new ArrayList<>();
        }

        public UniParcDocumentBuilder notDuplicatedDatabasesFacet(Integer facetValue) {
            if (!databasesFacets.contains(facetValue)) {
                databasesFacets.add(facetValue);
            }
            return this;
        }
    }
}
