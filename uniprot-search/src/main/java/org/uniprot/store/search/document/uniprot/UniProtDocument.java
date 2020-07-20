package org.uniprot.store.search.document.uniprot;

import java.util.*;

import org.apache.solr.client.solrj.beans.Field;
import org.uniprot.store.search.document.Document;

/** Document used for indexing uniprot entries into Solr */
public class UniProtDocument implements Document {

    private static final long serialVersionUID = 6487942504460456915L;

    @Field("accession_id")
    public String accession;

    @Field("sec_acc")
    public List<String> secacc = new ArrayList<>();

    @Field("id")
    public String id;

    @Field("id_default")
    public String idDefault;

    @Field("reviewed")
    public Boolean reviewed;

    @Field("protein_name")
    public List<String> proteinNames = new ArrayList<>();

    @Field("protein_name_sort")
    public String proteinsNamesSort;

    @Field("ec")
    public List<String> ecNumbers = new ArrayList<>();

    @Field("ec_exact")
    public List<String> ecNumbersExact = new ArrayList<>();

    @Field("date_modified")
    public Date lastModified;

    @Field("date_created")
    public Date firstCreated;

    @Field("date_sequence_modified")
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

    @Field("model_organism")
    public String modelOrganism;

    @Field("other_organism")
    public String otherOrganism;

    @Field("taxonomy_name")
    public List<String> organismTaxon = new ArrayList<>();

    @Field("taxonomy_id")
    public List<Integer> taxLineageIds = new ArrayList<>();

    @Field("organelle")
    public List<String> organelles = new ArrayList<>();

    @Field("virus_host_name")
    public List<String> organismHostNames = new ArrayList<>();

    @Field("virus_host_id")
    public List<Integer> organismHostIds = new ArrayList<>();

    @Field("pathway")
    public List<String> pathway = new ArrayList<>();

    @Field("xref")
    public Set<String> crossRefs = new HashSet<>();

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

    @Field("structure_3d")
    public boolean d3structure = false;

    @Field("proteins_with")
    public List<String> proteinsWith = new ArrayList<>();

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
    public String unirefCluster50;

    @Field("uniref_cluster_90")
    public String unirefCluster90;

    @Field("uniref_cluster_100")
    public String unirefCluster100;

    @Field("mapped_citation")
    public List<String> mappedCitation = new ArrayList<>();

    @Field("uniparc")
    public String uniparc;

    @Override
    public String getDocumentId() {
        return accession;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UniProtDocument document = (UniProtDocument) o;
        return organismTaxId == document.organismTaxId
                && fragment == document.fragment
                && precursor == document.precursor
                && active == document.active
                && d3structure == document.d3structure
                && seqMass == document.seqMass
                && seqLength == document.seqLength
                && score == document.score
                && Objects.equals(accession, document.accession)
                && Objects.equals(secacc, document.secacc)
                && Objects.equals(id, document.id)
                && Objects.equals(idDefault, document.idDefault)
                && Objects.equals(reviewed, document.reviewed)
                && Objects.equals(proteinNames, document.proteinNames)
                && Objects.equals(proteinsNamesSort, document.proteinsNamesSort)
                && Objects.equals(ecNumbers, document.ecNumbers)
                && Objects.equals(ecNumbersExact, document.ecNumbersExact)
                && Objects.equals(lastModified, document.lastModified)
                && Objects.equals(firstCreated, document.firstCreated)
                && Objects.equals(sequenceUpdated, document.sequenceUpdated)
                && Objects.equals(keywords, document.keywords)
                && Objects.equals(geneNames, document.geneNames)
                && Objects.equals(geneNamesSort, document.geneNamesSort)
                && Objects.equals(geneNamesExact, document.geneNamesExact)
                && Objects.equals(organismName, document.organismName)
                && Objects.equals(organismSort, document.organismSort)
                && Objects.equals(modelOrganism, document.modelOrganism)
                && Objects.equals(otherOrganism, document.otherOrganism)
                && Objects.equals(organismTaxon, document.organismTaxon)
                && Objects.equals(taxLineageIds, document.taxLineageIds)
                && Objects.equals(organelles, document.organelles)
                && Objects.equals(organismHostNames, document.organismHostNames)
                && Objects.equals(organismHostIds, document.organismHostIds)
                && Objects.equals(pathway, document.pathway)
                && Objects.equals(crossRefs, document.crossRefs)
                && Objects.equals(databases, document.databases)
                && Objects.equals(referenceTitles, document.referenceTitles)
                && Objects.equals(referenceAuthors, document.referenceAuthors)
                && Objects.equals(referencePubmeds, document.referencePubmeds)
                && Objects.equals(referenceOrganizations, document.referenceOrganizations)
                && Objects.equals(referenceDates, document.referenceDates)
                && Objects.equals(referenceJournals, document.referenceJournals)
                && Objects.equals(commentMap, document.commentMap)
                && Objects.equals(commentEvMap, document.commentEvMap)
                && Objects.equals(content, document.content)
                && Objects.equals(featuresMap, document.featuresMap)
                && Objects.equals(featureEvidenceMap, document.featureEvidenceMap)
                && Objects.equals(featureLengthMap, document.featureLengthMap)
                && Objects.equals(proteinExistence, document.proteinExistence)
                && Objects.equals(proteinsWith, document.proteinsWith)
                && Objects.equals(subcellLocationTerm, document.subcellLocationTerm)
                && Objects.equals(subcellLocationTermEv, document.subcellLocationTermEv)
                && Objects.equals(subcellLocationNote, document.subcellLocationNote)
                && Objects.equals(subcellLocationNoteEv, document.subcellLocationNoteEv)
                && Objects.equals(ap, document.ap)
                && Objects.equals(apEv, document.apEv)
                && Objects.equals(apApu, document.apApu)
                && Objects.equals(apApuEv, document.apApuEv)
                && Objects.equals(apAs, document.apAs)
                && Objects.equals(apAsEv, document.apAsEv)
                && Objects.equals(apAi, document.apAi)
                && Objects.equals(apAiEv, document.apAiEv)
                && Objects.equals(apRf, document.apRf)
                && Objects.equals(apRfEv, document.apRfEv)
                && Objects.equals(bpcp, document.bpcp)
                && Objects.equals(bpcpAbsorption, document.bpcpAbsorption)
                && Objects.equals(bpcpKinetics, document.bpcpKinetics)
                && Objects.equals(bpcpPhDependence, document.bpcpPhDependence)
                && Objects.equals(bpcpRedoxPotential, document.bpcpRedoxPotential)
                && Objects.equals(bpcpTempDependence, document.bpcpTempDependence)
                && Objects.equals(bpcpEv, document.bpcpEv)
                && Objects.equals(bpcpAbsorptionEv, document.bpcpAbsorptionEv)
                && Objects.equals(bpcpKineticsEv, document.bpcpKineticsEv)
                && Objects.equals(bpcpPhDependenceEv, document.bpcpPhDependenceEv)
                && Objects.equals(bpcpRedoxPotentialEv, document.bpcpRedoxPotentialEv)
                && Objects.equals(bpcpTempDependenceEv, document.bpcpTempDependenceEv)
                && Objects.equals(cofactorChebi, document.cofactorChebi)
                && Objects.equals(cofactorNote, document.cofactorNote)
                && Objects.equals(cofactorChebiEv, document.cofactorChebiEv)
                && Objects.equals(cofactorNoteEv, document.cofactorNoteEv)
                && Objects.equals(seqCaution, document.seqCaution)
                && Objects.equals(seqCautionFrameshift, document.seqCautionFrameshift)
                && Objects.equals(seqCautionErInit, document.seqCautionErInit)
                && Objects.equals(seqCautionErTerm, document.seqCautionErTerm)
                && Objects.equals(seqCautionErPred, document.seqCautionErPred)
                && Objects.equals(seqCautionErTran, document.seqCautionErTran)
                && Objects.equals(seqCautionMisc, document.seqCautionMisc)
                && Objects.equals(seqCautionEv, document.seqCautionEv)
                && Objects.equals(seqCautionMiscEv, document.seqCautionMiscEv)
                && Objects.equals(interactors, document.interactors)
                && Objects.equals(familyInfo, document.familyInfo)
                && Objects.equals(seqAA, document.seqAA)
                && Objects.equals(rcTissue, document.rcTissue)
                && Objects.equals(rcStrain, document.rcStrain)
                && Objects.equals(rcPlasmid, document.rcPlasmid)
                && Objects.equals(rcTransposon, document.rcTransposon)
                && Objects.equals(scopes, document.scopes)
                && Objects.equals(proteomes, document.proteomes)
                && Objects.equals(proteomeComponents, document.proteomeComponents)
                && Objects.equals(goes, document.goes)
                && Objects.equals(goIds, document.goIds)
                && Objects.equals(goWithEvidenceMaps, document.goWithEvidenceMaps)
                && Arrays.equals(avroBinary, document.avroBinary)
                && Objects.equals(avroBin, document.avroBin)
                && Objects.equals(avroJson, document.avroJson)
                && Objects.equals(inactiveReason, document.inactiveReason)
                && Objects.equals(isIsoform, document.isIsoform)
                && Objects.equals(xrefCountMap, document.xrefCountMap)
                && Objects.equals(sources, document.sources)
                && Objects.equals(unirefCluster50, document.unirefCluster50)
                && Objects.equals(unirefCluster90, document.unirefCluster90)
                && Objects.equals(unirefCluster100, document.unirefCluster100)
                && Objects.equals(mappedCitation, document.mappedCitation)
                && Objects.equals(uniparc, document.uniparc);
    }

    @Override
    public int hashCode() {
        int result =
                Objects.hash(
                        accession,
                        secacc,
                        id,
                        idDefault,
                        reviewed,
                        proteinNames,
                        proteinsNamesSort,
                        ecNumbers,
                        ecNumbersExact,
                        lastModified,
                        firstCreated,
                        sequenceUpdated,
                        keywords,
                        geneNames,
                        geneNamesSort,
                        geneNamesExact,
                        organismName,
                        organismSort,
                        organismTaxId,
                        modelOrganism,
                        otherOrganism,
                        organismTaxon,
                        taxLineageIds,
                        organelles,
                        organismHostNames,
                        organismHostIds,
                        pathway,
                        crossRefs,
                        databases,
                        referenceTitles,
                        referenceAuthors,
                        referencePubmeds,
                        referenceOrganizations,
                        referenceDates,
                        referenceJournals,
                        commentMap,
                        commentEvMap,
                        content,
                        featuresMap,
                        featureEvidenceMap,
                        featureLengthMap,
                        proteinExistence,
                        fragment,
                        precursor,
                        active,
                        d3structure,
                        proteinsWith,
                        subcellLocationTerm,
                        subcellLocationTermEv,
                        subcellLocationNote,
                        subcellLocationNoteEv,
                        ap,
                        apEv,
                        apApu,
                        apApuEv,
                        apAs,
                        apAsEv,
                        apAi,
                        apAiEv,
                        apRf,
                        apRfEv,
                        bpcp,
                        bpcpAbsorption,
                        bpcpKinetics,
                        bpcpPhDependence,
                        bpcpRedoxPotential,
                        bpcpTempDependence,
                        bpcpEv,
                        bpcpAbsorptionEv,
                        bpcpKineticsEv,
                        bpcpPhDependenceEv,
                        bpcpRedoxPotentialEv,
                        bpcpTempDependenceEv,
                        cofactorChebi,
                        cofactorNote,
                        cofactorChebiEv,
                        cofactorNoteEv,
                        seqCaution,
                        seqCautionFrameshift,
                        seqCautionErInit,
                        seqCautionErTerm,
                        seqCautionErPred,
                        seqCautionErTran,
                        seqCautionMisc,
                        seqCautionEv,
                        seqCautionMiscEv,
                        interactors,
                        familyInfo,
                        seqMass,
                        seqLength,
                        seqAA,
                        rcTissue,
                        rcStrain,
                        rcPlasmid,
                        rcTransposon,
                        scopes,
                        proteomes,
                        proteomeComponents,
                        goes,
                        goIds,
                        goWithEvidenceMaps,
                        score,
                        avroBin,
                        avroJson,
                        inactiveReason,
                        isIsoform,
                        xrefCountMap,
                        sources,
                        unirefCluster50,
                        unirefCluster90,
                        unirefCluster100,
                        mappedCitation,
                        uniparc);
        result = 31 * result + Arrays.hashCode(avroBinary);
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
                + ", idDefault='"
                + idDefault
                + '\''
                + ", reviewed="
                + reviewed
                + ", proteinNames="
                + proteinNames
                + ", proteinsNamesSort='"
                + proteinsNamesSort
                + '\''
                + ", ecNumbers="
                + ecNumbers
                + ", ecNumbersExact="
                + ecNumbersExact
                + ", lastModified="
                + lastModified
                + ", firstCreated="
                + firstCreated
                + ", sequenceUpdated="
                + sequenceUpdated
                + ", keywords="
                + keywords
                + ", geneNames="
                + geneNames
                + ", geneNamesSort='"
                + geneNamesSort
                + '\''
                + ", geneNamesExact="
                + geneNamesExact
                + ", organismName="
                + organismName
                + ", organismSort='"
                + organismSort
                + '\''
                + ", organismTaxId="
                + organismTaxId
                + ", modelOrganism='"
                + modelOrganism
                + '\''
                + ", otherOrganism='"
                + otherOrganism
                + '\''
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
                + ", pathway="
                + pathway
                + ", crossRefs="
                + crossRefs
                + ", databases="
                + databases
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
                + ", referenceJournals="
                + referenceJournals
                + ", commentMap="
                + commentMap
                + ", commentEvMap="
                + commentEvMap
                + ", content="
                + content
                + ", featuresMap="
                + featuresMap
                + ", featureEvidenceMap="
                + featureEvidenceMap
                + ", featureLengthMap="
                + featureLengthMap
                + ", proteinExistence='"
                + proteinExistence
                + '\''
                + ", fragment="
                + fragment
                + ", precursor="
                + precursor
                + ", active="
                + active
                + ", d3structure="
                + d3structure
                + ", proteinsWith="
                + proteinsWith
                + ", subcellLocationTerm="
                + subcellLocationTerm
                + ", subcellLocationTermEv="
                + subcellLocationTermEv
                + ", subcellLocationNote="
                + subcellLocationNote
                + ", subcellLocationNoteEv="
                + subcellLocationNoteEv
                + ", ap="
                + ap
                + ", apEv="
                + apEv
                + ", apApu="
                + apApu
                + ", apApuEv="
                + apApuEv
                + ", apAs="
                + apAs
                + ", apAsEv="
                + apAsEv
                + ", apAi="
                + apAi
                + ", apAiEv="
                + apAiEv
                + ", apRf="
                + apRf
                + ", apRfEv="
                + apRfEv
                + ", bpcp="
                + bpcp
                + ", bpcpAbsorption="
                + bpcpAbsorption
                + ", bpcpKinetics="
                + bpcpKinetics
                + ", bpcpPhDependence="
                + bpcpPhDependence
                + ", bpcpRedoxPotential="
                + bpcpRedoxPotential
                + ", bpcpTempDependence="
                + bpcpTempDependence
                + ", bpcpEv="
                + bpcpEv
                + ", bpcpAbsorptionEv="
                + bpcpAbsorptionEv
                + ", bpcpKineticsEv="
                + bpcpKineticsEv
                + ", bpcpPhDependenceEv="
                + bpcpPhDependenceEv
                + ", bpcpRedoxPotentialEv="
                + bpcpRedoxPotentialEv
                + ", bpcpTempDependenceEv="
                + bpcpTempDependenceEv
                + ", cofactorChebi="
                + cofactorChebi
                + ", cofactorNote="
                + cofactorNote
                + ", cofactorChebiEv="
                + cofactorChebiEv
                + ", cofactorNoteEv="
                + cofactorNoteEv
                + ", seqCaution="
                + seqCaution
                + ", seqCautionFrameshift="
                + seqCautionFrameshift
                + ", seqCautionErInit="
                + seqCautionErInit
                + ", seqCautionErTerm="
                + seqCautionErTerm
                + ", seqCautionErPred="
                + seqCautionErPred
                + ", seqCautionErTran="
                + seqCautionErTran
                + ", seqCautionMisc="
                + seqCautionMisc
                + ", seqCautionEv="
                + seqCautionEv
                + ", seqCautionMiscEv="
                + seqCautionMiscEv
                + ", interactors="
                + interactors
                + ", familyInfo="
                + familyInfo
                + ", seqMass="
                + seqMass
                + ", seqLength="
                + seqLength
                + ", seqAA='"
                + seqAA
                + '\''
                + ", rcTissue="
                + rcTissue
                + ", rcStrain="
                + rcStrain
                + ", rcPlasmid="
                + rcPlasmid
                + ", rcTransposon="
                + rcTransposon
                + ", scopes="
                + scopes
                + ", proteomes="
                + proteomes
                + ", proteomeComponents="
                + proteomeComponents
                + ", goes="
                + goes
                + ", goIds="
                + goIds
                + ", goWithEvidenceMaps="
                + goWithEvidenceMaps
                + ", score="
                + score
                + ", avroBinary="
                + Arrays.toString(avroBinary)
                + ", avroBin='"
                + avroBin
                + '\''
                + ", avroJson='"
                + avroJson
                + '\''
                + ", inactiveReason='"
                + inactiveReason
                + '\''
                + ", isIsoform="
                + isIsoform
                + ", xrefCountMap="
                + xrefCountMap
                + ", sources="
                + sources
                + ", unirefCluster50="
                + unirefCluster50
                + ", unirefCluster90="
                + unirefCluster90
                + ", unirefCluster100="
                + unirefCluster100
                + ", mappedCitation="
                + mappedCitation
                + ", uniparc='"
                + uniparc
                + '\''
                + '}';
    }
}
