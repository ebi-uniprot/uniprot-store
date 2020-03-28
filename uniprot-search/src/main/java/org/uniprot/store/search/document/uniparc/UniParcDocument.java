package org.uniprot.store.search.document.uniparc;

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

    @Field("length")
    private int seqLength;

    @Field("database")
    @Singular
    private Set<String> databases;

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
    private List<String> upids;

    @Singular
    @Field("taxonomy_name")
    private List<String> organismTaxons;

    @Singular
    @Field("taxonomy_id")
    private List<Integer> taxLineageIds;

    @Singular
    @Field("accession")
    private List<String> uniprotAccessions;

    @Singular
    @Field("isoform")
    private List<String> uniprotIsoforms;

    // DEFAULT SEARCH FIELD
    @Singular("contentAdd")
    @Field("content")
    public List<String> content;

    @Override
    public String getDocumentId() {
        return upi;
    }
}
