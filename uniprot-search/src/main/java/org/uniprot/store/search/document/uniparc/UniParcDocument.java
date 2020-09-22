package org.uniprot.store.search.document.uniparc;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.*;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

@Builder(toBuilder = true)
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class UniParcDocument implements Document {

    private static final long serialVersionUID = 6897342859069571092L;

    @Field("upi")
    private String upi;

    @Field("checksum")
    private String sequenceChecksum;

    @Field("md5")
    private String sequenceMd5;

    @Field("length")
    private int seqLength;

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
    @Field("protein")
    private Set<String> proteinNames;

    @Singular
    @Field("upid")
    private Set<String> upids = new HashSet<>();

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
}
