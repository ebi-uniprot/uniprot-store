package uk.ac.ebi.uniprot.search.document.proteome;



import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.beans.Field;

import uk.ac.ebi.uniprot.search.document.Document;


public class ProteomeDocument  implements Document {
    
    @Field("upid")
    public String upid;

    @Field("organism_name")
    public String organismName;
    
    @Field("organism_id")
    public int organismTaxId;
    
    @Field("taxonomy_name")
    public List<String> organismTaxon = new ArrayList<>();

    @Field("taxonomy_id")
    public List<Integer> taxLineageIds = new ArrayList<>();
    
    @Field("reference")
    public boolean isReferenceProteome;
    
    @Field("is_redundant")
    public boolean isRedundant;
    
    @Field("genome_accession")
    public List<String> genomeId;
    
    @Field("genome_assembly")
    public List<String> genomeAssemblyId;

    @Field("accession")
    public List<String> accession;

    @Field("gene")
    public List<String> gene;

    @Field("proteome_stored")
    public byte[] proteomeStored;
}
