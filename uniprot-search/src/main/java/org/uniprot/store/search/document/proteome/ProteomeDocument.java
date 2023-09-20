package org.uniprot.store.search.document.proteome;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

import java.util.ArrayList;
import java.util.List;

public class ProteomeDocument implements Document {

    private static final long serialVersionUID = -1646520136782599683L;

    @Field("upid")
    public String upid;

    @Field("organism_name")
    public List<String> organismName = new ArrayList<>();

    @Field("organism_sort")
    public String organismSort;

    @Field("organism_id")
    public int organismTaxId;

    @Field("taxonomy_name")
    public List<String> organismTaxon = new ArrayList<>();

    @Field("taxonomy_id")
    public List<Integer> taxLineageIds = new ArrayList<>();

    @Field("strain")
    public String strain;

    @Field("reference")
    public boolean isReferenceProteome;

    @Field("redundant")
    public boolean isRedundant;

    @Field("excluded")
    public boolean isExcluded;

    @Field("superkingdom")
    public String superkingdom;

    @Field("genome_accession")
    public List<String> genomeAccession = new ArrayList<>();

    @Field("genome_assembly")
    public List<String> genomeAssembly = new ArrayList<>();

    @Field("proteome_stored")
    public byte[] proteomeStored;

    @Field("annotation_score")
    public int score = 0;

    @Field("proteome_type")
    public int proteomeType; // reference=1, complete=2, redundant=3,  excluded=4

    @Field("busco")
    public Float busco;

    @Field("cpd")
    public int cpd;

    @Field("protein_count")
    public int proteinCount;

    @Override
    public String getDocumentId() {
        return upid;
    }
}
