package uk.ac.ebi.uniprot.search.document.uniprot;

import org.apache.solr.client.solrj.beans.Field;

import uk.ac.ebi.uniprot.search.document.Document;

import java.util.*;

/**
 * Document used for indexing uniprot entries into Solr
 */

public class UniProtDocument implements Document {

    @Field("accession_id")
    public String accession;

    @Field("sec_acc")
    public List<String> secacc = new ArrayList<>();

    @Field("mnemonic")
    public String id;

    @Field("reviewed")
    public Boolean reviewed;
    
    @Field("name")
    public List<String> proteinNames = new ArrayList<>();

    @Field("name_sort")
    public String proteinsNamesSort;

    @Field("ec")
    public List<String> ecNumbers = new ArrayList<>();
    
    @Field("ec_exact")
    public List<String> ecNumbersExact = new ArrayList<>();
    
    @Field("modified")
    public Date lastModified;

    @Field("created")
    public Date firstCreated;
    
    @Field("sequence_modified")
    public Date sequenceUpdated;

    @Field("keyword")
    public List<String> keywords = new ArrayList<>();
    
    @Field("keyword_id")
    public List<String> keywordIds = new ArrayList<>();

    @Field("gene")
    public List<String> geneNames = new ArrayList<>();

    @Field("gene_sort")
    public String geneNamesSort;

    @Field("gene_exact")
    public List<String> geneNamesExact = new ArrayList<>();

    @Field("organism_name")
    public List<String> organismName = new ArrayList<>();

    @Field("organism_sort")
    public String organismSort;

    @Field("organism_id")
    public int organismTaxId;
    
    @Field("popular_organism")
    public String popularOrganism;
    
    @Field("other_organism")
    public String otherOrganism;

    @Field("taxonomy_name")
    public List<String> organismTaxon = new ArrayList<>();

    @Field("taxonomy_id")
    public List<Integer> taxLineageIds = new ArrayList<>();

    @Field("organelle")
    public List<String> organelles = new ArrayList<>();

    @Field("host_name")
    public List<String> organismHostNames = new ArrayList<>();

    @Field("host_id")
    public List<Integer> organismHostIds = new ArrayList<>();
    
    @Field("pathway")
    public List<String> pathway = new ArrayList<>();
    

    @Field("xref")
    public Set<String> xrefs = new HashSet<>();

    @Field("database")
    public Set<String> databases = new HashSet<>();

    @Field("xref_*")
    public Map<String, Collection<String>> xrefMap = new HashMap<>();

    @Field("lit_title")
    public List<String> referenceTitles = new ArrayList<>();

    @Field("lit_author")
    public List<String> referenceAuthors = new ArrayList<>();

    @Field("lit_pubmed")
    public List<String> referencePubmeds = new ArrayList<>();

    @Field("lit_organisation")
    public List<String> referenceOrganizations = new ArrayList<>();

    @Field("lit_pubdate")
    public List<Date> referenceDates = new ArrayList<>();
    
    @Field("lit_journal")
    public List<String> referenceJournals = new ArrayList<>();

    @Field("cc_*")
    public Map<String, Collection<String>> commentMap = new HashMap<>();
    
    @Field("ccev_*")
    public Map<String, Collection<String>> commentEvMap = new HashMap<>();
    
    //DEFAULT SEARCH FIELD
    @Field("content")
    public Set<String> content = new HashSet<>();

    @Field("ft_*")
    public Map<String, Collection<String>> featuresMap = new HashMap<>();
    
    @Field("ftev_*")
    public Map<String, Collection<String>> featureEvidenceMap = new HashMap<>();
    
    @Field("ftlen_*")
    public Map<String, Collection<Integer>> featureLengthMap = new HashMap<>();
    
    @Field("existence")
    public String proteinExistence;

    @Field("fragment")
    public boolean fragment;
    
    @Field("precursor")
    public boolean precursor;
    
    @Field("active")
    public boolean active =true;
    
    @Field("d3structure")
    public boolean d3structure =false;
    
    @Field("cc_scl_term")
    public Set<String> subcellLocationTerm = new HashSet<>();
    
    @Field("ccev_scl_term")
    public Set<String> subcellLocationTermEv = new HashSet<>();
    
    @Field("cc_scl_note")
    public Set<String> subcellLocationNote = new HashSet<>();
    
    @Field("ccev_scl_note")
    public Set<String> subcellLocationNoteEv = new HashSet<>();
    
    @Field("cc_ap")
    public  Set<String> ap = new HashSet<>();
    @Field("ccev_ap")
    public  Set<String> apEv = new HashSet<>();
    
    @Field("cc_ap_apu")
    public  Set<String> apApu = new HashSet<>();
    @Field("ccev_ap_apu")
    public  Set<String> apApuEv = new HashSet<>();
    
    @Field("cc_ap_as")
    public  Set<String> apAs = new HashSet<>();
    @Field("ccev_ap_as")
    public  Set<String> apAsEv = new HashSet<>();
    
    @Field("cc_ap_ai")
    public  Set<String> apAi = new HashSet<>();
    @Field("ccev_ap_ai")
    public  Set<String> apAiEv = new HashSet<>();
    
    @Field("cc_ap_rf")
    public  Set<String> apRf = new HashSet<>();
    @Field("ccev_ap_rf")
    public  Set<String> apRfEv = new HashSet<>();
    
    @Field("cc_bpcp")
    public Set<String> bpcp = new HashSet<>();
    
    @Field("cc_bpcp_absorption")
    public Set<String> bpcpAbsorption = new HashSet<>();
    
    @Field("cc_bpcp_kinetics")
    public Set<String> bpcpKinetics = new HashSet<>();
    
    @Field("cc_bpcp_ph_dependence")
    public Set<String> bpcpPhDependence = new HashSet<>();
    
    @Field("cc_bpcp_redox_potential")
    public Set<String> bpcpRedoxPotential = new HashSet<>();
    
    @Field("cc_bpcp_temp_dependence")
    public Set<String> bpcpTempDependence = new HashSet<>();
    
    
    @Field("ccev_bpcp")
    public Set<String> bpcpEv = new HashSet<>();
    
    @Field("ccev_bpcp_absorption")
    public Set<String> bpcpAbsorptionEv = new HashSet<>();
    
    @Field("ccev_bpcp_kinetics")
    public Set<String> bpcpKineticsEv = new HashSet<>();
    
    @Field("ccev_bpcp_ph_dependence")
    public Set<String> bpcpPhDependenceEv = new HashSet<>();
    
    @Field("ccev_bpcp_redox_potential")
    public Set<String> bpcpRedoxPotentialEv = new HashSet<>();
    
    @Field("ccev_bpcp_temp_dependence")
    public Set<String> bpcpTempDependenceEv = new HashSet<>();
    
    @Field("cc_cofactor_chebi")
    public Set<String> cofactorChebi = new HashSet<>();
    @Field("cc_cofactor_note")
    public Set<String> cofactorNote = new HashSet<>();
    
    @Field("ccev_cofactor_chebi")
    public Set<String> cofactorChebiEv = new HashSet<>();
    @Field("ccev_cofactor_note")
    public Set<String> cofactorNoteEv = new HashSet<>();
    
    @Field("cc_sc")
    public Set<String> seqCaution = new HashSet<>();
    @Field("cc_sc_framesh")
    public Set<String> seqCautionFrameshift = new HashSet<>();
    @Field("cc_sc_einit")
    public Set<String> seqCautionErInit = new HashSet<>();
    @Field("cc_sc_eterm")
    public Set<String> seqCautionErTerm = new HashSet<>();
    @Field("cc_sc_epred")
    public Set<String> seqCautionErPred = new HashSet<>();
    @Field("cc_sc_etran")
    public Set<String> seqCautionErTran = new HashSet<>();
    @Field("cc_sc_misc")
    public Set<String> seqCautionMisc = new HashSet<>();
    
    
    @Field("ccev_sc")
    public Set<String> seqCautionEv = new HashSet<>();
    @Field("ccev_sc_misc")
    public Set<String> seqCautionMiscEv  = new HashSet<>();
    
    
    @Field("interactor")
    public Set<String> interactors  = new HashSet<>();
  
    @Field("family")
    public Set<String> familyInfo  = new HashSet<>();
    
    @Field("mass")
    public int seqMass;
    
    @Field("length")
    public int seqLength;
   
    
    @Field("tissue")
    public Set<String> rcTissue = new HashSet<>();
    
    @Field("strain")
    public Set<String> rcStrain = new HashSet<>();
    
    @Field("plasmid")
    public Set<String> rcPlasmid = new HashSet<>();
    
    @Field("transposon")
    public Set<String> rcTransposon = new HashSet<>();
    
    @Field("scope")
    public Set<String> scopes = new HashSet<>();
   
    @Field("proteome")
    public Set<String> proteomes = new HashSet<>();
    @Field("proteomecomponent")
    public Set<String> proteomeComponents = new HashSet<>();
   
    @Field("go")
    public Set<String> goes = new HashSet<>();
    
    @Field("go_id")
    public Set<String> goIds = new HashSet<>();

    @Field("go_*")
    public Map<String, Collection<String>> goWithEvidenceMaps = new HashMap<>();

    @Field("annotation_score")
    public int score;
    
    @Field("avro_binary")
    public byte[] avro_binray;

    @Field("avro_bin")
    public String avro_binary;

    @Field("avro_json")
    public String avro_json;
    
    @Field("inactive_reason")
    public String inactiveReason;
    
    @Field("is_isoform")
    public Boolean isIsoform =false;
    
    @Field("uniref_cluster_50")
    public String unirefCluster50;
    
    @Field("uniref_cluster_90")
    public String unirefCluster90;
    
    @Field("uniref_cluster_100")
    public String unirefCluster100;

    public String file_path;
    public long obj_offset;
    public long obj_location;

    //extra proteomes fields
    @Field("genome_accession")
    public List<String> genomeAccession = new ArrayList<>();
    
    @Field("genome_assembly")
    public String genomeAssembly ;

}