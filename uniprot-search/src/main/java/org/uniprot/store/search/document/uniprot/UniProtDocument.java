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

    @Field("canonical_acc")
    public String canonicalAccession;

    @Field("id")
    public String id;

    @Field("id_default")
    public String idDefault;

    @Field("id_inactive")
    public String idInactive;

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
    public Integer modelOrganism;

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

    @Field("lit_citation_id")
    public List<String> referenceCitationIds = new ArrayList<>();

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
    public int proteinExistence;

    @Field("fragment")
    public boolean fragment;

    @Field("precursor")
    public boolean precursor;

    @Field("active")
    public Boolean active = true;

    @Field("structure_3d")
    public boolean d3structure = false;

    @Field("proteins_with")
    public List<Integer> proteinsWith = new ArrayList<>();

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

    @Field("inactive_reason")
    public String inactiveReason;

    @Field("is_isoform")
    public Boolean isIsoform = false;

    @Field("xref_count_*")
    public Map<String, Long> xrefCountMap = new HashMap<>();

    @Field("rhea")
    public List<String> rheaIds = new ArrayList<>();

    @Field("chebi")
    public Set<String> chebi = new HashSet<>();

    @Field("inchikey")
    public Set<String> inchikey = new HashSet<>();

    @Field("source")
    public List<String> sources = new ArrayList<>();

    @Field("uniref_cluster_50")
    public String unirefCluster50;

    @Field("uniref_cluster_90")
    public String unirefCluster90;

    @Field("uniref_cluster_100")
    public String unirefCluster100;

    @Field("computational_pubmed_id")
    public List<String> computationalPubmedIds = new ArrayList<>();

    @Field("community_pubmed_id")
    public List<String> communityPubmedIds = new ArrayList<>();

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
        UniProtDocument that = (UniProtDocument) o;
        return organismTaxId == that.organismTaxId
                && fragment == that.fragment
                && precursor == that.precursor
                && active == that.active
                && d3structure == that.d3structure
                && seqMass == that.seqMass
                && seqLength == that.seqLength
                && score == that.score
                && Objects.equals(accession, that.accession)
                && Objects.equals(secacc, that.secacc)
                && Objects.equals(id, that.id)
                && Objects.equals(idDefault, that.idDefault)
                && Objects.equals(reviewed, that.reviewed)
                && Objects.equals(proteinNames, that.proteinNames)
                && Objects.equals(proteinsNamesSort, that.proteinsNamesSort)
                && Objects.equals(ecNumbers, that.ecNumbers)
                && Objects.equals(ecNumbersExact, that.ecNumbersExact)
                && Objects.equals(lastModified, that.lastModified)
                && Objects.equals(firstCreated, that.firstCreated)
                && Objects.equals(sequenceUpdated, that.sequenceUpdated)
                && Objects.equals(keywords, that.keywords)
                && Objects.equals(geneNames, that.geneNames)
                && Objects.equals(geneNamesSort, that.geneNamesSort)
                && Objects.equals(geneNamesExact, that.geneNamesExact)
                && Objects.equals(organismName, that.organismName)
                && Objects.equals(organismSort, that.organismSort)
                && Objects.equals(modelOrganism, that.modelOrganism)
                && Objects.equals(organismTaxon, that.organismTaxon)
                && Objects.equals(taxLineageIds, that.taxLineageIds)
                && Objects.equals(organelles, that.organelles)
                && Objects.equals(organismHostNames, that.organismHostNames)
                && Objects.equals(organismHostIds, that.organismHostIds)
                && Objects.equals(pathway, that.pathway)
                && Objects.equals(crossRefs, that.crossRefs)
                && Objects.equals(databases, that.databases)
                && Objects.equals(referenceTitles, that.referenceTitles)
                && Objects.equals(referenceAuthors, that.referenceAuthors)
                && Objects.equals(referencePubmeds, that.referencePubmeds)
                && Objects.equals(referenceCitationIds, that.referenceCitationIds)
                && Objects.equals(referenceOrganizations, that.referenceOrganizations)
                && Objects.equals(referenceDates, that.referenceDates)
                && Objects.equals(referenceJournals, that.referenceJournals)
                && Objects.equals(commentMap, that.commentMap)
                && Objects.equals(commentEvMap, that.commentEvMap)
                && Objects.equals(content, that.content)
                && Objects.equals(featuresMap, that.featuresMap)
                && Objects.equals(featureEvidenceMap, that.featureEvidenceMap)
                && Objects.equals(featureLengthMap, that.featureLengthMap)
                && Objects.equals(proteinExistence, that.proteinExistence)
                && Objects.equals(proteinsWith, that.proteinsWith)
                && Objects.equals(subcellLocationTerm, that.subcellLocationTerm)
                && Objects.equals(subcellLocationTermEv, that.subcellLocationTermEv)
                && Objects.equals(subcellLocationNote, that.subcellLocationNote)
                && Objects.equals(subcellLocationNoteEv, that.subcellLocationNoteEv)
                && Objects.equals(ap, that.ap)
                && Objects.equals(apEv, that.apEv)
                && Objects.equals(apApu, that.apApu)
                && Objects.equals(apApuEv, that.apApuEv)
                && Objects.equals(apAs, that.apAs)
                && Objects.equals(apAsEv, that.apAsEv)
                && Objects.equals(apAi, that.apAi)
                && Objects.equals(apAiEv, that.apAiEv)
                && Objects.equals(apRf, that.apRf)
                && Objects.equals(apRfEv, that.apRfEv)
                && Objects.equals(bpcp, that.bpcp)
                && Objects.equals(bpcpAbsorption, that.bpcpAbsorption)
                && Objects.equals(bpcpKinetics, that.bpcpKinetics)
                && Objects.equals(bpcpPhDependence, that.bpcpPhDependence)
                && Objects.equals(bpcpRedoxPotential, that.bpcpRedoxPotential)
                && Objects.equals(bpcpTempDependence, that.bpcpTempDependence)
                && Objects.equals(bpcpEv, that.bpcpEv)
                && Objects.equals(bpcpAbsorptionEv, that.bpcpAbsorptionEv)
                && Objects.equals(bpcpKineticsEv, that.bpcpKineticsEv)
                && Objects.equals(bpcpPhDependenceEv, that.bpcpPhDependenceEv)
                && Objects.equals(bpcpRedoxPotentialEv, that.bpcpRedoxPotentialEv)
                && Objects.equals(bpcpTempDependenceEv, that.bpcpTempDependenceEv)
                && Objects.equals(cofactorChebi, that.cofactorChebi)
                && Objects.equals(cofactorNote, that.cofactorNote)
                && Objects.equals(cofactorChebiEv, that.cofactorChebiEv)
                && Objects.equals(cofactorNoteEv, that.cofactorNoteEv)
                && Objects.equals(seqCaution, that.seqCaution)
                && Objects.equals(seqCautionFrameshift, that.seqCautionFrameshift)
                && Objects.equals(seqCautionErInit, that.seqCautionErInit)
                && Objects.equals(seqCautionErTerm, that.seqCautionErTerm)
                && Objects.equals(seqCautionErPred, that.seqCautionErPred)
                && Objects.equals(seqCautionErTran, that.seqCautionErTran)
                && Objects.equals(seqCautionMisc, that.seqCautionMisc)
                && Objects.equals(seqCautionEv, that.seqCautionEv)
                && Objects.equals(seqCautionMiscEv, that.seqCautionMiscEv)
                && Objects.equals(interactors, that.interactors)
                && Objects.equals(familyInfo, that.familyInfo)
                && Objects.equals(seqAA, that.seqAA)
                && Objects.equals(rcTissue, that.rcTissue)
                && Objects.equals(rcStrain, that.rcStrain)
                && Objects.equals(rcPlasmid, that.rcPlasmid)
                && Objects.equals(rcTransposon, that.rcTransposon)
                && Objects.equals(scopes, that.scopes)
                && Objects.equals(proteomes, that.proteomes)
                && Objects.equals(proteomeComponents, that.proteomeComponents)
                && Objects.equals(goes, that.goes)
                && Objects.equals(goIds, that.goIds)
                && Objects.equals(goWithEvidenceMaps, that.goWithEvidenceMaps)
                && Objects.equals(inactiveReason, that.inactiveReason)
                && Objects.equals(isIsoform, that.isIsoform)
                && Objects.equals(xrefCountMap, that.xrefCountMap)
                && Objects.equals(sources, that.sources)
                && Objects.equals(unirefCluster50, that.unirefCluster50)
                && Objects.equals(unirefCluster90, that.unirefCluster90)
                && Objects.equals(unirefCluster100, that.unirefCluster100)
                && Objects.equals(computationalPubmedIds, that.computationalPubmedIds)
                && Objects.equals(communityPubmedIds, that.communityPubmedIds)
                && Objects.equals(uniparc, that.uniparc)
                && Objects.equals(rheaIds, that.rheaIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
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
                referenceCitationIds,
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
                inactiveReason,
                isIsoform,
                xrefCountMap,
                sources,
                unirefCluster50,
                unirefCluster90,
                unirefCluster100,
                computationalPubmedIds,
                communityPubmedIds,
                uniparc,
                rheaIds);
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
                + ", referenceCitationIds="
                + referenceCitationIds
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
                + ", proteinExistence="
                + proteinExistence
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
                + ", inactiveReason='"
                + inactiveReason
                + '\''
                + ", isIsoform="
                + isIsoform
                + ", xrefCountMap="
                + xrefCountMap
                + ", sources="
                + sources
                + ", unirefCluster50='"
                + unirefCluster50
                + '\''
                + ", unirefCluster90='"
                + unirefCluster90
                + '\''
                + ", unirefCluster100='"
                + unirefCluster100
                + '\''
                + ", computationalPubmedIds="
                + computationalPubmedIds
                + '\''
                + ", communityPubmedIds="
                + communityPubmedIds
                + ", uniparc='"
                + uniparc
                + '\''
                + ", rheaIds='"
                + rheaIds
                + '\''
                + '}';
    }
}
