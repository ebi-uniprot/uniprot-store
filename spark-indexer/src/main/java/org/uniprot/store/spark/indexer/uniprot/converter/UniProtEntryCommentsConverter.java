package org.uniprot.store.spark.indexer.uniprot.converter;

import static org.uniprot.store.spark.indexer.uniprot.converter.UniProtEntryConverterUtil.canAddExperimentalByAnnotationText;
import static org.uniprot.store.spark.indexer.uniprot.converter.UniProtEntryConverterUtil.hasExperimentalEvidence;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.uniprot.core.CrossReference;
import org.uniprot.core.Value;
import org.uniprot.core.flatfile.parser.impl.cc.CCLineBuilderFactory;
import org.uniprot.core.flatfile.writer.FFLineBuilder;
import org.uniprot.core.uniprotkb.comment.*;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.evidence.EvidenceCode;
import org.uniprot.core.uniprotkb.evidence.EvidencedValue;
import org.uniprot.core.util.Utils;
import org.uniprot.store.search.document.uniprot.ProteinsWith;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

/**
 * @author lgonzales
 * @since 2019-09-04
 */
class UniProtEntryCommentsConverter implements Serializable {

    private static final long serialVersionUID = -7061951400700075623L;
    private final Map<String, String> pathwayRepo;
    private static final String COMMENT = "cc_";
    private static final String CC_EV = "ccev_";
    static final String EXPERIMENTAL = "_exp";
    static final String CC_PATHWAY_EXPERIMENTAL = "cc_pathway" + EXPERIMENTAL;
    static final String CC_SCL_TERM_EXPERIMENTAL = "cc_scl_term" + EXPERIMENTAL;
    static final String CC_SCL_NOTE_EXPERIMENTAL = "cc_scl_note" + EXPERIMENTAL;
    static final String CC_BPCP_KINETICS_EXPERIMENTAL = "cc_bpcp_kinetics" + EXPERIMENTAL;
    static final String CC_BPCP_ABSORPTION_EXPERIMENTAL = "cc_bpcp_absorption" + EXPERIMENTAL;
    static final String CC_BPCP_PH_DEP_EXPERIMENTAL = "cc_bpcp_ph_dependence" + EXPERIMENTAL;
    static final String CC_BPCP_EXPERIMENTAL = "cc_bpcp" + EXPERIMENTAL;
    static final String CC_BPCP_REDOX_POT_EXPERIMENTAL = "cc_bpcp_redox_potential" + EXPERIMENTAL;
    static final String CC_BPCP_TEMP_DEP_EXPERIMENTAL = "cc_bpcp_temp_dependence" + EXPERIMENTAL;
    static final String CC_SC_MISC_EXPERIMENTAL = "cc_sc_misc" + EXPERIMENTAL;
    static final String CC_SC_EXPERIMENTAL = "cc_sc" + EXPERIMENTAL;
    static final String CC_COFACTOR_CHEBI_EXPERIMENTAL = "cc_cofactor_chebi" + EXPERIMENTAL;
    static final String CC_COFACTOR_NOTE_EXPERIMENTAL = "cc_cofactor_note" + EXPERIMENTAL;
    static final String CC_AP_RF_EXPERIMENTAL = "cc_ap_rf" + EXPERIMENTAL;
    static final String CC_AP_AI_EXPERIMENTAL = "cc_ap_ai" + EXPERIMENTAL;
    static final String CC_AP_AS_EXPERIMENTAL = "cc_ap_as" + EXPERIMENTAL;
    static final String CC_AP_APU_EXPERIMENTAL = "cc_ap_apu" + EXPERIMENTAL;
    static final String CC_AP_EXPERIMENTAL = "cc_ap" + EXPERIMENTAL;
    private static final Pattern PATTERN_FAMILY =
            Pattern.compile(
                    "(?:In the .+? section; )?[Bb]elongs to the (.+?family)\\.(?: (.+?family)\\.)?(?: (.+?family)\\.)?(?: Highly divergent\\.)?");

    UniProtEntryCommentsConverter(Map<String, String> pathway) {
        this.pathwayRepo = pathway;
    }

    void convertCommentToDocument(List<Comment> comments, UniProtDocument document) {
        for (Comment comment : comments) {
            FFLineBuilder<Comment> fbuilder = CCLineBuilderFactory.create(comment);
            String commentField = getCommentField(comment);
            String evField = getCommentEvField(comment);
            Collection<String> commentValues =
                    document.commentMap.computeIfAbsent(commentField, k -> new ArrayList<>());

            String commentVal = fbuilder.buildString(comment);
            commentValues.add(commentVal);
            document.content.add(commentVal);
            Collection<String> evValues =
                    document.commentEvMap.computeIfAbsent(evField, k -> new HashSet<>());
            Set<String> evidences = fetchEvidences(comment);

            if (canAddExperimental(
                    comment.getCommentType(), commentVal, document.reviewed, evidences)) {
                String experimentalField = commentField + EXPERIMENTAL;
                Collection<String> experimentalComments =
                        document.commentMap.computeIfAbsent(
                                experimentalField, k -> new ArrayList<>());
                experimentalComments.add(commentVal);
                evidences.add(EvidenceCode.Category.EXPERIMENTAL.name().toLowerCase());
                document.evidenceExperimental = true;
            }
            evValues.addAll(evidences);

            ProteinsWith.from(comment.getCommentType())
                    .map(ProteinsWith::getValue)
                    .filter(
                            commentTypeValue ->
                                    !document.proteinsWith.contains(
                                            commentTypeValue)) // avoid duplicated
                    .ifPresent(commentTypeValue -> document.proteinsWith.add(commentTypeValue));

            switch (comment.getCommentType()) {
                case CATALYTIC_ACTIVITY:
                    convertCatalyticActivity(
                            (CatalyticActivityComment) comment, document, commentVal);
                    break;
                case COFACTOR:
                    convertFactor((CofactorComment) comment, document, commentVal);
                    break;
                case BIOPHYSICOCHEMICAL_PROPERTIES:
                    convertCommentBPCP((BPCPComment) comment, document);
                    break;
                case PATHWAY:
                    convertPathway((FreeTextComment) comment, document);
                    break;
                case INTERACTION:
                    convertCommentInteraction((InteractionComment) comment, document);
                    break;
                case SUBCELLULAR_LOCATION:
                    convertCommentSL((SubcellularLocationComment) comment, document);
                    break;
                case ALTERNATIVE_PRODUCTS:
                    convertCommentAP((AlternativeProductsComment) comment, document, commentVal);
                    break;
                case SIMILARITY:
                    convertCommentFamily((FreeTextComment) comment, document);
                    break;
                case SEQUENCE_CAUTION:
                    convertCommentSC((SequenceCautionComment) comment, document);
                    break;
                case DISEASE:
                    convertDiseaseComment((DiseaseComment) comment, document);
                    break;
                default:
                    break;
            }
        }
    }

    private String getCommentField(Comment c) {
        String field = COMMENT + c.getCommentType().name().toLowerCase();
        return field.replace(' ', '_');
    }

    private String getCommentEvField(Comment c) {
        String field = CC_EV + c.getCommentType().name().toLowerCase();
        return field.replace(' ', '_');
    }

    private void convertCommentAP(
            AlternativeProductsComment comment, UniProtDocument document, String commentVal) {

        List<String> values = new ArrayList<>();
        Set<String> evidences = new HashSet<>();
        if (comment.hasNote() && comment.getNote().hasTexts()) {
            values.addAll(getTextsValue(comment.getNote().getTexts()));
            evidences.addAll(getTextsEvidence(comment.getNote().getTexts()));
        }
        if (comment.hasIsoforms()) {
            comment.getIsoforms().stream()
                    .filter(APIsoform::hasNote)
                    .map(APIsoform::getNote)
                    .filter(Note::hasTexts)
                    .forEach(
                            note -> {
                                values.addAll(getTextsValue(note.getTexts()));
                                evidences.addAll(getTextsEvidence(note.getTexts()));
                            });

            comment.getIsoforms().stream()
                    .filter(APIsoform::hasIsoformSequenceStatus)
                    .map(APIsoform::getIsoformSequenceStatus)
                    .map(IsoformSequenceStatus::getName)
                    .forEach(values::add);
        }
        CommentType commentType = CommentType.ALTERNATIVE_PRODUCTS;
        List<String> events = new ArrayList<>();
        if (comment.hasEvents()) {
            comment.getEvents().stream().map(APEventType::getName).forEach(events::add);
            document.ap.addAll(events);
        }
        if (values.isEmpty()) {
            values.add(
                    "true"); // default value when we do not have note, so it can be searched with
            // '*'
        }
        document.ap.addAll(values);
        document.apEv.addAll(evidences);
        if (canAddExperimental(commentType, commentVal, document.reviewed, evidences)) {
            Collection<String> apExp =
                    document.commentMap.computeIfAbsent(CC_AP_EXPERIMENTAL, k -> new HashSet<>());
            apExp.addAll(values);
            apExp.addAll(events);
            document.apEv.add(EvidenceCode.Category.EXPERIMENTAL.name().toLowerCase());
            document.evidenceExperimental = true;
        }
        for (String event : events) {
            if ("alternative promoter usage".equalsIgnoreCase(event)) {
                document.apApu.addAll(values);
                addExperimentalEvidence(
                        commentType, document, values, evidences, CC_AP_APU_EXPERIMENTAL);
                document.apApuEv.addAll(evidences);
            }
            if ("alternative splicing".equalsIgnoreCase(event)) {
                document.apAs.addAll(values);
                addExperimentalEvidence(
                        commentType, document, values, evidences, CC_AP_AS_EXPERIMENTAL);
                document.apAsEv.addAll(evidences);
            }
            if ("alternative initiation".equalsIgnoreCase(event)) {
                document.apAi.addAll(values);
                addExperimentalEvidence(
                        commentType, document, values, evidences, CC_AP_AI_EXPERIMENTAL);
                document.apAiEv.addAll(evidences);
            }
            if ("ribosomal frameshifting".equalsIgnoreCase(event)) {
                document.apRf.addAll(values);
                addExperimentalEvidence(
                        commentType, document, values, evidences, CC_AP_RF_EXPERIMENTAL);
                document.apRfEv.addAll(evidences);
            }
        }
    }

    private void addExperimentalEvidence(
            CommentType commentType,
            UniProtDocument document,
            Collection<String> values,
            Set<String> evidences,
            String experimentalField) {
        String commentVal = String.join(" ", values);
        if (canAddExperimental(commentType, commentVal, document.reviewed, evidences)) {
            document.commentMap
                    .computeIfAbsent(experimentalField, k -> new HashSet<>())
                    .addAll(values);
            evidences.add(EvidenceCode.Category.EXPERIMENTAL.name().toLowerCase());
            document.evidenceExperimental = true;
        }
    }

    private void convertFactor(
            CofactorComment comment, UniProtDocument document, String commentVal) {
        if (comment.hasCofactors()) {
            comment.getCofactors()
                    .forEach(cofactor -> convertCofactor(document, commentVal, cofactor));
        }

        if ((comment.hasNote()) && (comment.getNote().hasTexts())) {
            List<String> noteValues = getTextsValue(comment.getNote().getTexts());
            document.cofactorNote.addAll(noteValues);
            Set<String> textEvidences = getTextsEvidence(comment.getNote().getTexts());
            addExperimentalEvidence(
                    CommentType.COFACTOR,
                    document,
                    noteValues,
                    textEvidences,
                    CC_COFACTOR_NOTE_EXPERIMENTAL);
            document.cofactorNoteEv.addAll(textEvidences);
        }
    }

    private void convertCofactor(UniProtDocument document, String commentVal, Cofactor cofactor) {
        List<String> cofactorValues = new ArrayList<>();
        cofactorValues.add(cofactor.getName());
        if (cofactor.getCofactorCrossReference().getDatabase() == CofactorDatabase.CHEBI) {
            cofactorValues.add(cofactor.getCofactorCrossReference().getId());
        }
        document.cofactorChebi.addAll(cofactorValues);
        Set<String> evidences = UniProtEntryConverterUtil.extractEvidence(cofactor.getEvidences());
        if (canAddExperimental(CommentType.COFACTOR, commentVal, document.reviewed, evidences)) {
            document.commentMap
                    .computeIfAbsent(CC_COFACTOR_CHEBI_EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(cofactorValues);
            document.cofactorChebiEv.add(EvidenceCode.Category.EXPERIMENTAL.name().toLowerCase());
            document.evidenceExperimental = true;
        }
        document.cofactorChebiEv.addAll(evidences);
    }

    private void convertCommentSC(SequenceCautionComment comment, UniProtDocument document) {
        Set<String> cautionValues = new HashSet<>();
        cautionValues.add(comment.getSequenceCautionType().getDisplayName());
        String val =
                "true"; // default value for the type when we do not have note, so the type can be
        // searched with '*'
        if (comment.hasNote()) {
            val = comment.getNote();
            cautionValues.add(comment.getNote());
        }

        document.seqCaution.addAll(cautionValues);
        Set<String> evidence = UniProtEntryConverterUtil.extractEvidence(comment.getEvidences());
        CommentType commentType = CommentType.SEQUENCE_CAUTION;
        addExperimentalEvidence(commentType, document, cautionValues, evidence, CC_SC_EXPERIMENTAL);
        document.seqCautionEv.addAll(evidence);
        switch (comment.getSequenceCautionType()) {
            case FRAMESHIFT:
                document.seqCautionFrameshift.add(val);
                break;
            case ERRONEOUS_INITIATION:
                document.seqCautionErInit.add(val);
                break;
            case ERRONEOUS_TERMINATION:
                document.seqCautionErTerm.add(val);
                break;
            case ERRONEOUS_PREDICTION:
                document.seqCautionErPred.add(val);
                break;
            case ERRONEOUS_TRANSLATION:
                document.seqCautionErTran.add(val);
                break;
            case MISCELLANEOUS_DISCREPANCY:
                document.seqCautionMisc.add(val);
                addExperimentalEvidence(
                        commentType, document, Set.of(val), evidence, CC_SC_MISC_EXPERIMENTAL);
                document.seqCautionMiscEv.addAll(evidence);
                break;
            default:
        }
    }

    private void convertDiseaseComment(DiseaseComment comment, UniProtDocument document) {
        if (comment.hasDefinedDisease()) {
            Disease disease = comment.getDisease();
            Set<String> evidences =
                    UniProtEntryConverterUtil.extractEvidence(disease.getEvidences());
            if (comment.hasNote() && comment.getNote().hasTexts()) {
                evidences.addAll(getTextsEvidence(comment.getNote().getTexts()));
            }
            String field = getCommentField(comment);
            String accession = comment.getDisease().getDiseaseAccession();
            document.content.add(accession);
            document.commentMap.get(field).add(accession);
            addExperimentalEvidence(
                    CommentType.DISEASE,
                    document,
                    Set.of(accession),
                    evidences,
                    field + EXPERIMENTAL);
        }
    }

    private void convertCommentBPCP(BPCPComment comment, UniProtDocument document) {
        if (comment.hasAbsorption()) {
            convertAbsorption(document, comment.getAbsorption());
        }
        if (comment.hasKineticParameters()) {
            convertKineticParameters(document, comment.getKineticParameters());
        }
        if (comment.hasPhDependence() && comment.getPhDependence().hasTexts()) {
            convertPhDependence(document, comment.getPhDependence());
        }
        if (comment.hasRedoxPotential() && comment.getRedoxPotential().hasTexts()) {
            convertRedoxPotential(document, comment.getRedoxPotential());
        }
        if (comment.hasTemperatureDependence() && comment.getTemperatureDependence().hasTexts()) {
            convertTemperatureDependence(document, comment.getTemperatureDependence());
        }
    }

    private void convertTemperatureDependence(
            UniProtDocument document, TemperatureDependence temperatureDependence) {
        Set<String> temperatureDependenceEvidences =
                getTextsEvidence(temperatureDependence.getTexts());
        List<String> temperatureDependenceValues = getTextsValue(temperatureDependence.getTexts());
        document.bpcpTempDependence.addAll(temperatureDependenceValues);
        document.bpcp.addAll(temperatureDependenceValues);
        String commentVal = String.join(" ", temperatureDependenceValues);
        if (canAddExperimental(
                CommentType.BIOPHYSICOCHEMICAL_PROPERTIES,
                commentVal,
                document.reviewed,
                temperatureDependenceEvidences)) {
            document.commentMap
                    .computeIfAbsent(CC_BPCP_TEMP_DEP_EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(temperatureDependenceValues);
            document.commentMap
                    .computeIfAbsent(CC_BPCP_EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(temperatureDependenceValues);
            temperatureDependenceEvidences.add(
                    EvidenceCode.Category.EXPERIMENTAL.name().toLowerCase());
            document.evidenceExperimental = true;
        }
        document.bpcpEv.addAll(temperatureDependenceEvidences);
        document.bpcpTempDependenceEv.addAll(temperatureDependenceEvidences);
    }

    private void convertRedoxPotential(UniProtDocument document, RedoxPotential redoxPotential) {
        Set<String> redoxPotentialEvidences = getTextsEvidence(redoxPotential.getTexts());
        List<String> redoxPotentialValues = getTextsValue(redoxPotential.getTexts());
        document.bpcpRedoxPotential.addAll(redoxPotentialValues);
        document.bpcp.addAll(redoxPotentialValues);
        String commentVal = String.join(" ", redoxPotentialValues);
        if (canAddExperimental(
                CommentType.BIOPHYSICOCHEMICAL_PROPERTIES,
                commentVal,
                document.reviewed,
                redoxPotentialEvidences)) {
            document.commentMap
                    .computeIfAbsent(CC_BPCP_REDOX_POT_EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(redoxPotentialValues);
            document.commentMap
                    .computeIfAbsent(CC_BPCP_EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(redoxPotentialValues);
            redoxPotentialEvidences.add(EvidenceCode.Category.EXPERIMENTAL.name().toLowerCase());
            document.evidenceExperimental = true;
        }
        document.bpcpEv.addAll(redoxPotentialEvidences);
        document.bpcpRedoxPotentialEv.addAll(redoxPotentialEvidences);
    }

    private void convertPhDependence(UniProtDocument document, PhDependence phDependence) {
        Set<String> phDependenceEvidences = getTextsEvidence(phDependence.getTexts());
        List<String> phDependenceValues = getTextsValue(phDependence.getTexts());
        document.bpcpPhDependence.addAll(phDependenceValues);
        document.bpcp.addAll(phDependenceValues);
        String commentVal = String.join(" ", phDependenceValues);
        if (canAddExperimental(
                CommentType.BIOPHYSICOCHEMICAL_PROPERTIES,
                commentVal,
                document.reviewed,
                phDependenceEvidences)) {
            document.commentMap
                    .computeIfAbsent(CC_BPCP_PH_DEP_EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(phDependenceValues);
            document.commentMap
                    .computeIfAbsent(CC_BPCP_EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(phDependenceValues);
            phDependenceEvidences.add(EvidenceCode.Category.EXPERIMENTAL.name().toLowerCase());
            document.evidenceExperimental = true;
        }
        document.bpcpPhDependenceEv.addAll(phDependenceEvidences);
        document.bpcpEv.addAll(phDependenceEvidences);
    }

    private void convertAbsorption(UniProtDocument document, Absorption absorption) {
        Set<String> absorptionValues = new HashSet<>();
        Set<String> absorptionEvidences = new HashSet<>();
        absorptionValues.add("" + absorption.getMax());
        absorptionEvidences.addAll(
                UniProtEntryConverterUtil.extractEvidence(absorption.getEvidences()));
        if (absorption.hasNote() && absorption.getNote().hasTexts()) {
            absorptionValues.addAll(getTextsValue(absorption.getNote().getTexts()));
            absorptionEvidences.addAll(getTextsEvidence(absorption.getNote().getTexts()));
        }
        document.bpcpAbsorption.addAll(absorptionValues);
        document.bpcp.addAll(absorptionValues);
        String commentVal = String.join(" ", absorptionValues);
        if (canAddExperimental(
                CommentType.BIOPHYSICOCHEMICAL_PROPERTIES,
                commentVal,
                document.reviewed,
                absorptionEvidences)) {
            document.commentMap
                    .computeIfAbsent(CC_BPCP_ABSORPTION_EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(absorptionValues);
            document.commentMap
                    .computeIfAbsent(CC_BPCP_EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(absorptionValues);
            absorptionEvidences.add(EvidenceCode.Category.EXPERIMENTAL.name().toLowerCase());
            document.evidenceExperimental = true;
        }
        document.bpcpAbsorptionEv.addAll(absorptionEvidences);
        document.bpcpEv.addAll(absorptionEvidences);
    }

    private void convertKineticParameters(UniProtDocument document, KineticParameters kp) {
        List<String> kineticValues = new ArrayList<>();
        Set<String> kineticEvidenceValues = new HashSet<>();
        if (kp.hasMaximumVelocities()) {
            for (MaximumVelocity maximumVelocity : kp.getMaximumVelocities()) {
                if (maximumVelocity.hasEnzyme()) {
                    kineticValues.add(maximumVelocity.getEnzyme());
                }
                if (maximumVelocity.hasVelocity()) {
                    kineticValues.add(String.valueOf(maximumVelocity.getVelocity()));
                }
                if (maximumVelocity.hasEvidences()) {
                    kineticEvidenceValues.addAll(
                            UniProtEntryConverterUtil.extractEvidence(
                                    maximumVelocity.getEvidences()));
                }
            }
        }
        if (kp.hasMichaelisConstants()) {
            for (MichaelisConstant michaelisConstant : kp.getMichaelisConstants()) {
                if (michaelisConstant.hasConstant()) {
                    kineticValues.add(String.valueOf(michaelisConstant.getConstant()));
                }
                if (michaelisConstant.hasSubstrate()) {
                    kineticValues.add(michaelisConstant.getSubstrate());
                }
                if (michaelisConstant.hasEvidences()) {
                    kineticEvidenceValues.addAll(
                            UniProtEntryConverterUtil.extractEvidence(
                                    michaelisConstant.getEvidences()));
                }
            }
        }
        if (kp.hasNote() && kp.getNote().hasTexts()) {
            kineticValues.addAll(getTextsValue(kp.getNote().getTexts()));
            kineticEvidenceValues.addAll(getTextsEvidence(kp.getNote().getTexts()));
        }

        document.bpcpKinetics.addAll(kineticValues);
        document.bpcp.addAll(kineticValues);

        String commentVal = String.join(" ", kineticValues);
        if (canAddExperimental(
                CommentType.BIOPHYSICOCHEMICAL_PROPERTIES,
                commentVal,
                document.reviewed,
                kineticEvidenceValues)) {
            document.commentMap
                    .computeIfAbsent(CC_BPCP_KINETICS_EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(kineticValues);
            document.commentMap
                    .computeIfAbsent(CC_BPCP_EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(kineticValues);
            kineticEvidenceValues.add(EvidenceCode.Category.EXPERIMENTAL.name().toLowerCase());
            document.evidenceExperimental = true;
        }
        document.bpcpKineticsEv.addAll(kineticEvidenceValues);
        document.bpcpEv.addAll(kineticEvidenceValues);
    }

    private List<String> getTextsValue(List<EvidencedValue> texts) {
        List<String> result = new ArrayList<>();
        if (Utils.notNullNotEmpty(texts)) {
            texts.stream().map(Value::getValue).forEach(result::add);
        }
        return result;
    }

    private Set<String> getTextsEvidence(List<EvidencedValue> texts) {
        Set<String> result = new HashSet<>();
        if (Utils.notNullNotEmpty(texts)) {
            List<Evidence> evidences =
                    texts.stream()
                            .flatMap(text -> text.getEvidences().stream())
                            .collect(Collectors.toList());

            result.addAll(UniProtEntryConverterUtil.extractEvidence(evidences));
        }
        return result;
    }

    private void convertCommentSL(SubcellularLocationComment comment, UniProtDocument document) {
        if (comment.hasSubcellularLocations()) {
            comment.getSubcellularLocations()
                    .forEach(
                            subcellularLocation -> {
                                if (subcellularLocation.hasLocation()) {
                                    SubcellularLocationValue location =
                                            subcellularLocation.getLocation();
                                    updateSubcellularLocation(document, location);
                                }
                                if (subcellularLocation.hasOrientation()) {
                                    SubcellularLocationValue orientation =
                                            subcellularLocation.getOrientation();
                                    updateSubcellularLocation(document, orientation);
                                }
                                if (subcellularLocation.hasTopology()) {
                                    SubcellularLocationValue topology =
                                            subcellularLocation.getTopology();
                                    updateSubcellularLocation(document, topology);
                                }
                            });
        }
        if (comment.hasNote() && comment.getNote().hasTexts()) {
            List<String> noteValues = getTextsValue(comment.getNote().getTexts());
            // Current legacy does not apply default experimental when we do not have any evidence
            // for subcellular location
            Set<String> noteEv = getTextsEvidence(comment.getNote().getTexts());
            document.subcellLocationNote.addAll(noteValues);
            addExperimentalEvidence(
                    CommentType.SUBCELLULAR_LOCATION,
                    document,
                    noteValues,
                    noteEv,
                    CC_SCL_NOTE_EXPERIMENTAL);
            document.subcellLocationNoteEv.addAll(noteEv);
        }
    }

    private void updateSubcellularLocation(
            UniProtDocument document, SubcellularLocationValue location) {
        Set<String> locationTerm = Set.of(location.getId(), location.getValue());
        document.subcellLocationTerm.addAll(locationTerm);
        Set<String> locationEv = UniProtEntryConverterUtil.extractEvidence(location.getEvidences());
        document.content.add(location.getId());
        addExperimentalEvidence(
                CommentType.SUBCELLULAR_LOCATION,
                document,
                locationTerm,
                locationEv,
                CC_SCL_TERM_EXPERIMENTAL);
        document.subcellLocationTermEv.addAll(locationEv);
    }

    private void updateFamily(String val, UniProtDocument document) {
        if (!val.endsWith(".")) {
            val += ".";
        }
        Matcher m = PATTERN_FAMILY.matcher(val);
        if (m.matches()) {
            StringBuilder line = new StringBuilder();
            line.append(m.group(1));
            if (Utils.notNull(m.group(2))) line.append(", ").append(m.group(2));
            if (Utils.notNull(m.group(3))) line.append(", ").append(m.group(3));
            document.familyInfo.add(line.toString());
        }
    }

    private void convertPathway(FreeTextComment comment, UniProtDocument document) {
        comment.getTexts().forEach(text -> updatePathway(text, document));
    }

    private void updatePathway(EvidencedValue text, UniProtDocument document) {
        if (Utils.notNull(pathwayRepo)) {
            String unipathwayAccession = pathwayRepo.get(text.getValue());
            if (Utils.notNull(unipathwayAccession)) {
                document.pathway.add(unipathwayAccession);
                document.content.add(unipathwayAccession);
            }
        }
    }

    private Set<String> fetchEvidences(Comment comment) {
        Set<String> evidences = new HashSet<>();
        if (comment instanceof FreeTextComment) {
            FreeTextComment toComment = (FreeTextComment) comment;
            if (toComment.hasTexts()) {
                evidences.addAll(getTextsEvidence(toComment.getTexts()));
            }
        }
        CommentType type = comment.getCommentType();
        switch (type) {
            case DISEASE:
                DiseaseComment diseaseComment = (DiseaseComment) comment;
                if (diseaseComment.hasDefinedDisease()) {
                    evidences.addAll(
                            UniProtEntryConverterUtil.extractEvidence(
                                    diseaseComment.getDisease().getEvidences()));
                    if (diseaseComment.hasNote() && diseaseComment.getNote().hasTexts()) {
                        evidences.addAll(getTextsEvidence(diseaseComment.getNote().getTexts()));
                    }
                }
                break;
            case RNA_EDITING:
                RnaEditingComment reComment = (RnaEditingComment) comment;
                if (reComment.hasPositions()) {
                    evidences.addAll(
                            UniProtEntryConverterUtil.extractEvidence(
                                    reComment.getPositions().stream()
                                            .flatMap(val -> val.getEvidences().stream())
                                            .collect(Collectors.toList())));
                }
                if (reComment.hasNote() && reComment.getNote().hasTexts()) {
                    evidences.addAll(getTextsEvidence(reComment.getNote().getTexts()));
                }
                break;
            case COFACTOR:
                CofactorComment cofactorComment = (CofactorComment) comment;
                if (cofactorComment.hasCofactors()) {
                    cofactorComment.getCofactors().stream()
                            .flatMap(
                                    cofactor ->
                                            UniProtEntryConverterUtil.extractEvidence(
                                                    cofactor.getEvidences())
                                                    .stream())
                            .forEach(evidences::add);
                }

                if ((cofactorComment.hasNote()) && (cofactorComment.getNote().hasTexts())) {
                    evidences.addAll(getTextsEvidence(cofactorComment.getNote().getTexts()));
                }
                break;
            case MASS_SPECTROMETRY:
                MassSpectrometryComment msComment = (MassSpectrometryComment) comment;
                evidences.addAll(
                        UniProtEntryConverterUtil.extractEvidence(msComment.getEvidences()));
                break;
            case CATALYTIC_ACTIVITY:
                CatalyticActivityComment caComment = (CatalyticActivityComment) comment;
                if (caComment.hasReaction()) {
                    evidences.addAll(
                            UniProtEntryConverterUtil.extractEvidence(
                                    caComment.getReaction().getEvidences()));
                }
                if (caComment.hasPhysiologicalReactions()) {
                    List<Evidence> physiologicalEvidences =
                            caComment.getPhysiologicalReactions().stream()
                                    .filter(PhysiologicalReaction::hasEvidences)
                                    .flatMap(reaction -> reaction.getEvidences().stream())
                                    .collect(Collectors.toList());
                    evidences.addAll(
                            UniProtEntryConverterUtil.extractEvidence(physiologicalEvidences));
                }
                break;
            case BIOPHYSICOCHEMICAL_PROPERTIES:
                BPCPComment bpcpComment = (BPCPComment) comment;
                if (bpcpComment.hasAbsorption() && bpcpComment.getAbsorption().hasEvidences()) {
                    evidences.addAll(
                            UniProtEntryConverterUtil.extractEvidence(
                                    bpcpComment.getAbsorption().getEvidences()));
                }
                if (bpcpComment.hasKineticParameters()) {
                    KineticParameters kinetics = bpcpComment.getKineticParameters();
                    if (kinetics.hasMaximumVelocities()) {
                        evidences.addAll(
                                kinetics.getMaximumVelocities().stream()
                                        .filter(MaximumVelocity::hasEvidences)
                                        .map(MaximumVelocity::getEvidences)
                                        .map(UniProtEntryConverterUtil::extractEvidence)
                                        .flatMap(Collection::stream)
                                        .collect(Collectors.toSet()));
                    }
                    if (kinetics.hasMichaelisConstants()) {
                        evidences.addAll(
                                kinetics.getMichaelisConstants().stream()
                                        .filter(MichaelisConstant::hasEvidences)
                                        .map(MichaelisConstant::getEvidences)
                                        .map(UniProtEntryConverterUtil::extractEvidence)
                                        .flatMap(Collection::stream)
                                        .collect(Collectors.toSet()));
                    }
                    if (kinetics.hasNote() && kinetics.getNote().hasTexts()) {
                        evidences.addAll(getTextsEvidence(kinetics.getNote().getTexts()));
                    }
                }
                if (bpcpComment.hasPhDependence() && bpcpComment.getPhDependence().hasTexts()) {
                    evidences.addAll(getTextsEvidence(bpcpComment.getPhDependence().getTexts()));
                }
                if (bpcpComment.hasRedoxPotential() && bpcpComment.getRedoxPotential().hasTexts()) {
                    evidences.addAll(getTextsEvidence(bpcpComment.getRedoxPotential().getTexts()));
                }
                if (bpcpComment.hasTemperatureDependence()
                        && bpcpComment.getTemperatureDependence().hasTexts()) {
                    evidences.addAll(
                            getTextsEvidence(bpcpComment.getTemperatureDependence().getTexts()));
                }
                break;
            default:
                break;
        }
        return evidences;
    }

    private void convertCommentInteraction(InteractionComment comment, UniProtDocument document) {
        comment.getInteractions()
                .forEach(
                        interaction -> {
                            document.interactors.add(interaction.getInteractantOne().getIntActId());
                            document.interactors.add(interaction.getInteractantTwo().getIntActId());
                            if (Utils.notNull(
                                    interaction.getInteractantTwo().getUniProtKBAccession())) {
                                document.interactors.add(
                                        interaction
                                                .getInteractantTwo()
                                                .getUniProtKBAccession()
                                                .getValue());
                            }
                        });
    }

    private void convertCatalyticActivity(
            CatalyticActivityComment comment, UniProtDocument doc, String commentVal) {
        Reaction reaction = comment.getReaction();
        String field = this.getCommentField(comment);
        if (reaction.hasReactionCrossReferences()) {
            Set<String> evidences =
                    UniProtEntryConverterUtil.extractEvidence(reaction.getEvidences());
            boolean experimental =
                    canAddExperimental(
                            CommentType.CATALYTIC_ACTIVITY, commentVal, doc.reviewed, evidences);
            convertReactionInformation(
                    doc, reaction.getReactionCrossReferences(), field, experimental);
        }

        if (comment.hasPhysiologicalReactions()) {
            List<Evidence> evidences =
                    comment.getPhysiologicalReactions().stream()
                            .filter(PhysiologicalReaction::hasEvidences)
                            .flatMap(phyReaction -> phyReaction.getEvidences().stream())
                            .collect(Collectors.toList());
            Set<String> evidenceSet = UniProtEntryConverterUtil.extractEvidence(evidences);
            boolean experimental =
                    canAddExperimental(
                            CommentType.CATALYTIC_ACTIVITY, commentVal, doc.reviewed, evidenceSet);

            var physiologicalReactionCrossReferences =
                    comment.getPhysiologicalReactions().stream()
                            .filter(PhysiologicalReaction::hasReactionCrossReference)
                            .map(PhysiologicalReaction::getReactionCrossReference)
                            .collect(Collectors.toList());
            convertReactionInformation(
                    doc, physiologicalReactionCrossReferences, field, experimental);
        }

        if (reaction.hasEcNumber()) {
            doc.ecNumbers.add(reaction.getEcNumber().getValue());
        }
    }

    private void convertReactionInformation(
            UniProtDocument doc,
            List<CrossReference<ReactionDatabase>> reactionReferences,
            String field,
            boolean experimental) {
        List<String> reactionIds =
                reactionReferences.stream().map(CrossReference::getId).collect(Collectors.toList());
        doc.commentMap.computeIfAbsent(field, k -> new ArrayList<>()).addAll(reactionIds);
        if (experimental) {
            doc.commentMap
                    .computeIfAbsent(field + EXPERIMENTAL, k -> new ArrayList<>())
                    .addAll(reactionIds);
            doc.evidenceExperimental = true;
        }

        // add rhea ids
        List<String> rheaIds =
                reactionReferences.stream()
                        .filter(rr -> ReactionDatabase.RHEA.equals(rr.getDatabase()))
                        .map(CrossReference::getId)
                        .filter(id -> !id.toUpperCase().startsWith("RHEA-COMP"))
                        .collect(Collectors.toList());

        if (Utils.notNullNotEmpty(rheaIds)) {
            doc.rheaIds.addAll(rheaIds);
        }
    }

    private void convertCommentFamily(FreeTextComment comment, UniProtDocument document) {
        comment.getTexts().stream()
                .map(Value::getValue)
                .forEach(val -> updateFamily(val, document));
    }

    private boolean canAddExperimental(
            CommentType commentType, String commentVal, Boolean reviewed, Set<String> evidences) {
        return hasExperimentalEvidence(evidences)
                || (evidences.isEmpty()
                        && commentType.isAddExperimental()
                        && (reviewed != null && reviewed)
                        && canAddExperimentalByAnnotationText(commentVal));
    }
}
