package org.uniprot.store.search.document.uniprot;

import java.io.Serializable;
import java.util.*;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

/** Document used for indexing uniprot entries into Solr */
public class UniProtDocument implements Document, Serializable {

    private static final long serialVersionUID = 6487942504460456915L;

    @Field("accession_id")
    public String accession;

    @Field("sec_acc")
    public List<String> secacc = new ArrayList<>();

    @Field("mnemonic")
    public String id;

    @Field("mnemonic_default")
    public String idDefault;

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

    // DEFAULT SEARCH FIELD
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
    public boolean active = true;

    @Field("d3structure")
    public boolean d3structure = false;

    @Field("proteins_with")
    public Set<String> proteinsWith = new HashSet<>();

    @Field("cc_scl_term")
    public Set<String> subcellLocationTerm = new HashSet<>();

    @Field("ccev_scl_term")
    public Set<String> subcellLocationTermEv = new HashSet<>();

    @Field("cc_scl_note")
    public Set<String> subcellLocationNote = new HashSet<>();

    @Field("ccev_scl_note")
    public Set<String> subcellLocationNoteEv = new HashSet<>();

    @Field("cc_ap")
    public Set<String> ap = new HashSet<>();

    @Field("ccev_ap")
    public Set<String> apEv = new HashSet<>();

    @Field("cc_ap_apu")
    public Set<String> apApu = new HashSet<>();

    @Field("ccev_ap_apu")
    public Set<String> apApuEv = new HashSet<>();

    @Field("cc_ap_as")
    public Set<String> apAs = new HashSet<>();

    @Field("ccev_ap_as")
    public Set<String> apAsEv = new HashSet<>();

    @Field("cc_ap_ai")
    public Set<String> apAi = new HashSet<>();

    @Field("ccev_ap_ai")
    public Set<String> apAiEv = new HashSet<>();

    @Field("cc_ap_rf")
    public Set<String> apRf = new HashSet<>();

    @Field("ccev_ap_rf")
    public Set<String> apRfEv = new HashSet<>();

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
    public Set<String> seqCautionMiscEv = new HashSet<>();

    @Field("interactor")
    public Set<String> interactors = new HashSet<>();

    @Field("family")
    public Set<String> familyInfo = new HashSet<>();

    @Field("mass")
    public int seqMass;

    @Field("length")
    public int seqLength;

    // Added by Chuming Chen for Peptide Search on Sept. 16, 2019.
    @Field("sq")
    public String seqAA;

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
    public byte[] avroBinary;

    @Field("avro_bin")
    public String avroBin;

    @Field("avro_json")
    public String avroJson;

    @Field("inactive_reason")
    public String inactiveReason;

    @Field("is_isoform")
    public Boolean isIsoform = false;

    @Field("xref_count_*")
    public Map<String, Long> xrefCountMap = new HashMap<>();

    @Field("source")
    public List<String> sources = new ArrayList<>();

    @Field("uniref_cluster_50")
    public List<String> unirefCluster50 = new ArrayList<>();

    @Field("uniref_cluster_90")
    public List<String> unirefCluster90 = new ArrayList<>();;

    @Field("uniref_cluster_100")
    public List<String> unirefCluster100 = new ArrayList<>();;

    @Field("uniref_size_50")
    public int unirefSize50;

    @Field("uniref_size_90")
    public int unirefSize90;

    @Field("uniref_size_100")
    public int unirefSize100;

    @Field("mapped_citation")
    public List<String> mappedCitation;

    @Field("uniparc")
    public String uniparc;

    @Override
    public String getDocumentId() {
        return accession;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UniProtDocument)) return false;

        UniProtDocument that = (UniProtDocument) o;

        if (fragment != that.fragment) return false;
        if (organismTaxId != that.organismTaxId) return false;
        if (reviewed != that.reviewed) return false;
        if (accession != null ? !accession.equals(that.accession) : that.accession != null)
            return false;
        if (commentMap != null ? !commentMap.equals(that.commentMap) : that.commentMap != null)
            return false;
        if (content != null ? !content.equals(that.content) : that.content != null) return false;
        if (ecNumbers != null ? !ecNumbers.equals(that.ecNumbers) : that.ecNumbers != null)
            return false;
        if (featuresMap != null ? !featuresMap.equals(that.featuresMap) : that.featuresMap != null)
            return false;
        if (firstCreated != null
                ? !firstCreated.equals(that.firstCreated)
                : that.firstCreated != null) return false;
        if (geneNames != null ? !geneNames.equals(that.geneNames) : that.geneNames != null)
            return false;
        if (geneNamesExact != null
                ? !geneNamesExact.equals(that.geneNamesExact)
                : that.geneNamesExact != null) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (keywords != null ? !keywords.equals(that.keywords) : that.keywords != null)
            return false;
        if (lastModified != null
                ? !lastModified.equals(that.lastModified)
                : that.lastModified != null) return false;
        if (organelles != null ? !organelles.equals(that.organelles) : that.organelles != null)
            return false;
        if (organismHostIds != null
                ? !organismHostIds.equals(that.organismHostIds)
                : that.organismHostIds != null) return false;
        if (organismHostNames != null
                ? !organismHostNames.equals(that.organismHostNames)
                : that.organismHostNames != null) return false;
        if (organismName != null
                ? !organismName.equals(that.organismName)
                : that.organismName != null) return false;
        if (organismTaxon != null
                ? !organismTaxon.equals(that.organismTaxon)
                : that.organismTaxon != null) return false;
        if (proteinExistence != null
                ? !proteinExistence.equals(that.proteinExistence)
                : that.proteinExistence != null) return false;
        if (proteinNames != null
                ? !proteinNames.equals(that.proteinNames)
                : that.proteinNames != null) return false;
        if (referenceAuthors != null
                ? !referenceAuthors.equals(that.referenceAuthors)
                : that.referenceAuthors != null) return false;
        if (referenceDates != null
                ? !referenceDates.equals(that.referenceDates)
                : that.referenceDates != null) return false;
        if (referenceOrganizations != null
                ? !referenceOrganizations.equals(that.referenceOrganizations)
                : that.referenceOrganizations != null) return false;
        if (referencePubmeds != null
                ? !referencePubmeds.equals(that.referencePubmeds)
                : that.referencePubmeds != null) return false;
        if (referenceTitles != null
                ? !referenceTitles.equals(that.referenceTitles)
                : that.referenceTitles != null) return false;
        if (secacc != null ? !secacc.equals(that.secacc) : that.secacc != null) return false;
        if (taxLineageIds != null
                ? !taxLineageIds.equals(that.taxLineageIds)
                : that.taxLineageIds != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = accession != null ? accession.hashCode() : 0;
        result = 31 * result + (secacc != null ? secacc.hashCode() : 0);
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (reviewed ? 1 : 0);
        result = 31 * result + (proteinNames != null ? proteinNames.hashCode() : 0);
        result = 31 * result + (ecNumbers != null ? ecNumbers.hashCode() : 0);
        result = 31 * result + (lastModified != null ? lastModified.hashCode() : 0);
        result = 31 * result + (firstCreated != null ? firstCreated.hashCode() : 0);
        result = 31 * result + (keywords != null ? keywords.hashCode() : 0);
        result = 31 * result + (geneNames != null ? geneNames.hashCode() : 0);
        result = 31 * result + (geneNamesExact != null ? geneNamesExact.hashCode() : 0);
        result = 31 * result + (organismName != null ? organismName.hashCode() : 0);
        result = 31 * result + organismTaxId;
        result = 31 * result + (organismTaxon != null ? organismTaxon.hashCode() : 0);
        result = 31 * result + (taxLineageIds != null ? taxLineageIds.hashCode() : 0);
        result = 31 * result + (organelles != null ? organelles.hashCode() : 0);
        result = 31 * result + (organismHostNames != null ? organismHostNames.hashCode() : 0);
        result = 31 * result + (organismHostIds != null ? organismHostIds.hashCode() : 0);
        result = 31 * result + (referenceTitles != null ? referenceTitles.hashCode() : 0);
        result = 31 * result + (referenceAuthors != null ? referenceAuthors.hashCode() : 0);
        result = 31 * result + (referencePubmeds != null ? referencePubmeds.hashCode() : 0);
        result =
                31 * result
                        + (referenceOrganizations != null ? referenceOrganizations.hashCode() : 0);
        result = 31 * result + (referenceDates != null ? referenceDates.hashCode() : 0);
        result = 31 * result + (commentMap != null ? commentMap.hashCode() : 0);
        result = 31 * result + (content != null ? content.hashCode() : 0);
        result = 31 * result + (featuresMap != null ? featuresMap.hashCode() : 0);
        result = 31 * result + (fragment ? 1 : 0);
        result = 31 * result + (proteinExistence != null ? proteinExistence.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "UniProtDocument{"
                + "accession='"
                + accession
                + '\''
                + ", secacc="
                + secacc
                + ", id='"
                + id
                + '\''
                + ", reviewed="
                + reviewed
                + ", proteinNames="
                + proteinNames
                + ", ecNumbers="
                + ecNumbers
                + ", lastModified="
                + lastModified
                + ", firstCreated="
                + firstCreated
                + ", keywords="
                + keywords
                + ", geneNames="
                + geneNames
                + ", geneNamesExact="
                + geneNamesExact
                + ", organismNames="
                + organismName
                + ", organismTaxId="
                + organismTaxId
                + ", organismTaxon="
                + organismTaxon
                + ", taxLineageIds="
                + taxLineageIds
                + ", organelles="
                + organelles
                + ", organismHostNames="
                + organismHostNames
                + ", organismHostIds="
                + organismHostIds
                + ", referenceTitles="
                + referenceTitles
                + ", referenceAuthors="
                + referenceAuthors
                + ", referencePubmeds="
                + referencePubmeds
                + ", referenceOrganizations="
                + referenceOrganizations
                + ", referenceDates="
                + referenceDates
                + ", commentMap="
                + commentMap
                + ", content="
                + content
                + ", featuresMap="
                + featuresMap
                + ", fragment="
                + fragment
                + ", proteinExistence='"
                + proteinExistence
                + '\''
                + '}';
    }
}
