package org.uniprot.store.spark.indexer.uniprot.converter;

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
    private static final Pattern PATTERN_FAMILY =
            Pattern.compile(
                    "(?:In the .+? section; )?[Bb]elongs to the (.+?family)\\.(?: (.+?family)\\.)?(?: (.+?family)\\.)?(?: Highly divergent\\.)?");

    UniProtEntryCommentsConverter(Map<String, String> pathway) {
        this.pathwayRepo = pathway;
    }

    void convertCommentToDocument(List<Comment> comments, UniProtDocument document) {
        for (Comment comment : comments) {
            FFLineBuilder<Comment> fbuilder = CCLineBuilderFactory.create(comment);
            // TODO field is too generic, we can rename it to commentField
            String field = getCommentField(comment);
            String evField = getCommentEvField(comment);
            // TODO value is too vague, commentValues will give more hint about the type values
            // why are we using Collection type not List or Set
            Collection<String> value =
                    document.commentMap.computeIfAbsent(field, k -> new ArrayList<>());

            String commentVal = fbuilder.buildString(comment);
            value.add(commentVal);
            document.content.add(commentVal);
            // TODO the name of the field evValue should be plural since this is a collection
            Collection<String> evValue =
                    document.commentEvMap.computeIfAbsent(evField, k -> new HashSet<>());
            Set<String> evidences = fetchEvidences(comment);
            evValue.addAll(evidences);

            if (hasExperimentalEvidence(evidences)) {
                String experimentalField = field + EXPERIMENTAL;
                //  TODO the name should be plural
                Collection<String> experimentalComment =
                        document.commentMap.computeIfAbsent(
                                experimentalField, k -> new ArrayList<>());
                experimentalComment.add(commentVal);
            }

            ProteinsWith.from(comment.getCommentType())
                    .map(ProteinsWith::getValue)
                    .filter(
                            commentTypeValue ->
                                    !document.proteinsWith.contains(
                                            commentTypeValue)) // avoid duplicated
                    .ifPresent(commentTypeValue -> document.proteinsWith.add(commentTypeValue));

            switch (comment.getCommentType()) {
                case CATALYTIC_ACTIVITY:
                    convertCatalyticActivity((CatalyticActivityComment) comment, document);
                    break;
                case COFACTOR:
                    convertFactor((CofactorComment) comment, document);
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
                    convertCommentAP((AlternativeProductsComment) comment, document);
                    break;
                case SIMILARITY:
                    convertCommentFamily((FreeTextComment) comment, document);
                    break;
                case SEQUENCE_CAUTION:
                    convertCommentSC((SequenceCautionComment) comment, document);
                    break;
                case DISEASE:
                    convertDiseaseComment((DiseaseComment) comment, document, evidences);
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

    private void convertCommentAP(AlternativeProductsComment comment, UniProtDocument document) {
        List<String> values = new ArrayList<>();
        //TODO name should be plural
        Set<String> evidence = new HashSet<>();
        if (comment.hasNote() && comment.getNote().hasTexts()) {
            values.addAll(getTextsValue(comment.getNote().getTexts()));
            evidence.addAll(getTextsEvidence(comment.getNote().getTexts()));
        }
        if (comment.hasIsoforms()) {
            comment.getIsoforms().stream()
                    .filter(APIsoform::hasNote)
                    .map(APIsoform::getNote)
                    .filter(Note::hasTexts)
                    .forEach(
                            note -> {
                                values.addAll(getTextsValue(note.getTexts()));
                                evidence.addAll(getTextsEvidence(note.getTexts()));
                            });

            comment.getIsoforms().stream()
                    .filter(APIsoform::hasIsoformSequenceStatus)
                    .map(APIsoform::getIsoformSequenceStatus)
                    .map(IsoformSequenceStatus::getName)
                    .forEach(values::add);
        }

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
        document.apEv.addAll(evidence);
        if (hasExperimentalEvidence(evidence)) { // TODO repeated code, we can take out in a method
            Collection<String> apExp =
                    document.commentMap.computeIfAbsent(//TODO we should take out cc_ap_exp to a const, see other places
                            "cc_ap" + EXPERIMENTAL, k -> new HashSet<>());
            apExp.addAll(values);
            apExp.addAll(events);
        }
        for (String event : events) {
            if ("alternative promoter usage".equalsIgnoreCase(event)) {
                document.apApu.addAll(values);
                document.apApuEv.addAll(evidence);
                if (hasExperimentalEvidence(evidence)) {// TODO repeated code, we can take out in a method
                    document.commentMap
                            .computeIfAbsent("cc_ap_apu" + EXPERIMENTAL, k -> new HashSet<>())
                            .addAll(values);
                }
            }
            if ("alternative splicing".equalsIgnoreCase(event)) {
                document.apAs.addAll(values);
                document.apAsEv.addAll(evidence);
                if (hasExperimentalEvidence(evidence)) {// TODO repeated code, we can take out in a method
                    document.commentMap
                            .computeIfAbsent("cc_ap_as" + EXPERIMENTAL, k -> new HashSet<>())
                            .addAll(values);
                }
            }
            if ("alternative initiation".equalsIgnoreCase(event)) {
                document.apAi.addAll(values);
                document.apAiEv.addAll(evidence);
                if (hasExperimentalEvidence(evidence)) {// TODO repeated code, we can take out in a method
                    document.commentMap
                            .computeIfAbsent("cc_ap_ai" + EXPERIMENTAL, k -> new HashSet<>())
                            .addAll(values);
                }
            }
            if ("ribosomal frameshifting".equalsIgnoreCase(event)) {
                document.apRf.addAll(values);
                document.apRfEv.addAll(evidence);
                if (hasExperimentalEvidence(evidence)) {// TODO repeated code, we can take out in a method
                    document.commentMap
                            .computeIfAbsent("cc_ap_rf" + EXPERIMENTAL, k -> new HashSet<>())
                            .addAll(values);
                }
            }
        }
    }

    private void convertFactor(CofactorComment comment, UniProtDocument document) {
        if (comment.hasCofactors()) {
            comment.getCofactors().forEach(cofactor -> convertCofactor(document, cofactor));
        }

        if ((comment.hasNote()) && (comment.getNote().hasTexts())) {
            List<String> noteValues = getTextsValue(comment.getNote().getTexts());
            document.cofactorNote.addAll(noteValues);
            Set<String> textEvidences = getTextsEvidence(comment.getNote().getTexts());
            document.cofactorNoteEv.addAll(textEvidences);
            if (hasExperimentalEvidence(textEvidences)) {
                document.commentMap
                        .computeIfAbsent("cc_cofactor_note" + EXPERIMENTAL, k -> new ArrayList<>())
                        .addAll(noteValues);
            }
        }
    }

    private void convertCofactor(UniProtDocument document, Cofactor cofactor) {
        List<String> cofactorValues = new ArrayList<>();
        cofactorValues.add(cofactor.getName());
        if (cofactor.getCofactorCrossReference().getDatabase() == CofactorDatabase.CHEBI) {
            cofactorValues.add(cofactor.getCofactorCrossReference().getId());
        }
        document.cofactorChebi.addAll(cofactorValues);
        if (hasExperimentalEvidence(cofactor.getEvidences())) {
            document.commentMap
                    .computeIfAbsent("cc_cofactor_chebi" + EXPERIMENTAL, k -> new ArrayList<>())
                    .addAll(cofactorValues);
        }
        document.cofactorChebiEv.addAll(
                UniProtEntryConverterUtil.extractEvidence(cofactor.getEvidences()));
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
        document.seqCautionEv.addAll(evidence);
        if (hasExperimentalEvidence(evidence)) {
            document.commentMap
                    .computeIfAbsent("cc_sc" + EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(cautionValues);
        }
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
                document.seqCautionMiscEv.addAll(evidence);
                if (hasExperimentalEvidence(evidence)) {
                    document.commentMap
                            .computeIfAbsent("cc_sc_misc" + EXPERIMENTAL, k -> new HashSet<>())
                            .add(val);
                }
                break;
            default:
        }
    }

    private void convertDiseaseComment(
            DiseaseComment comment, UniProtDocument document, Set<String> evidences) {
        if (comment.hasDefinedDisease()) {
            String field = getCommentField(comment);
            String accession = comment.getDisease().getDiseaseAccession();
            document.content.add(accession);
            document.commentMap.get(field).add(accession);
            if (hasExperimentalEvidence(evidences)) {
                document.commentMap
                        .computeIfAbsent(field + EXPERIMENTAL, k -> new HashSet<>())
                        .add(accession);
            }
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
        document.bpcpTempDependenceEv.addAll(temperatureDependenceEvidences);
        document.bpcp.addAll(temperatureDependenceValues);
        document.bpcpEv.addAll(temperatureDependenceEvidences);
        if (hasExperimentalEvidence(temperatureDependenceEvidences)) {
            document.commentMap
                    .computeIfAbsent("cc_bpcp_temp_dependence" + EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(temperatureDependenceValues);
            document.commentMap
                    .computeIfAbsent("cc_bpcp" + EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(temperatureDependenceValues);
        }
    }

    private void convertRedoxPotential(UniProtDocument document, RedoxPotential redoxPotential) {
        Set<String> redoxPotentialEvidences = getTextsEvidence(redoxPotential.getTexts());
        List<String> redoxPotentialValues = getTextsValue(redoxPotential.getTexts());
        document.bpcpRedoxPotential.addAll(redoxPotentialValues);
        document.bpcpRedoxPotentialEv.addAll(redoxPotentialEvidences);
        document.bpcp.addAll(redoxPotentialValues);
        document.bpcpEv.addAll(redoxPotentialEvidences);
        if (hasExperimentalEvidence(redoxPotentialEvidences)) {
            document.commentMap
                    .computeIfAbsent("cc_bpcp_redox_potential" + EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(redoxPotentialValues);
            document.commentMap
                    .computeIfAbsent("cc_bpcp" + EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(redoxPotentialValues);
        }
    }

    private void convertPhDependence(UniProtDocument document, PhDependence phDependence) {
        Set<String> phDependenceEvidences = getTextsEvidence(phDependence.getTexts());
        List<String> phDependenceValues = getTextsValue(phDependence.getTexts());
        document.bpcpPhDependence.addAll(phDependenceValues);
        document.bpcpPhDependenceEv.addAll(phDependenceEvidences);
        document.bpcp.addAll(phDependenceValues);
        document.bpcpEv.addAll(phDependenceEvidences);
        if (hasExperimentalEvidence(phDependenceEvidences)) {
            document.commentMap
                    .computeIfAbsent("cc_bpcp_ph_dependence" + EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(phDependenceValues);
            document.commentMap
                    .computeIfAbsent("cc_bpcp" + EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(phDependenceValues);
        }
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
        document.bpcpAbsorptionEv.addAll(absorptionEvidences);
        document.bpcp.addAll(absorptionValues);
        document.bpcpEv.addAll(absorptionEvidences);
        if (hasExperimentalEvidence(absorptionEvidences)) {
            document.commentMap
                    .computeIfAbsent("cc_bpcp_absorption" + EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(absorptionValues);
            document.commentMap
                    .computeIfAbsent("cc_bpcp" + EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(absorptionValues);
        }
    }

    private void convertKineticParameters(UniProtDocument document, KineticParameters kp) {
        List<String> kineticValues = new ArrayList<>();
        List<String> kineticEvidenceValues = new ArrayList<>();
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
        document.bpcpKineticsEv.addAll(kineticEvidenceValues);

        document.bpcp.addAll(kineticValues);
        document.bpcpEv.addAll(kineticEvidenceValues);

        if (hasExperimentalEvidence(kineticEvidenceValues)) {
            document.commentMap
                    .computeIfAbsent("cc_bpcp_kinetics" + EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(kineticValues);
            document.commentMap
                    .computeIfAbsent("cc_bpcp" + EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(kineticValues);
        }
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
            Set<String> noteEv = getTextsEvidence(comment.getNote().getTexts());
            document.subcellLocationNote.addAll(noteValues);
            document.subcellLocationNoteEv.addAll(noteEv);
            if (hasExperimentalEvidence(noteEv)) {
                document.commentMap
                        .computeIfAbsent("cc_scl_note" + EXPERIMENTAL, k -> new HashSet<>())
                        .addAll(noteValues);
            }
        }
    }

    private void updateSubcellularLocation(
            UniProtDocument document, SubcellularLocationValue location) {
        Set<String> locationTerm = Set.of(location.getId(), location.getValue());
        document.subcellLocationTerm.addAll(locationTerm);

        Set<String> locationEv = UniProtEntryConverterUtil.extractEvidence(location.getEvidences());
        document.subcellLocationTermEv.addAll(locationEv);
        document.content.add(location.getId());
        if (hasExperimentalEvidence(locationEv)) {
            document.commentMap
                    .computeIfAbsent("cc_scl_term" + EXPERIMENTAL, k -> new HashSet<>())
                    .addAll(locationTerm);
        }
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
                if (hasExperimentalEvidence(text.getEvidences())) {
                    document.commentMap
                            .computeIfAbsent("cc_pathway" + EXPERIMENTAL, k -> new ArrayList<>())
                            .add(unipathwayAccession);
                }
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

    private void convertCatalyticActivity(CatalyticActivityComment comment, UniProtDocument doc) {
        Reaction reaction = comment.getReaction();
        String field = this.getCommentField(comment);
        if (reaction.hasReactionCrossReferences()) {
            boolean experimental = hasExperimentalEvidence(reaction.getEvidences());
            convertReactionInformation(
                    doc, reaction.getReactionCrossReferences(), field, experimental);
        }

        if (comment.hasPhysiologicalReactions()) {
            List<Evidence> evidences =
                    comment.getPhysiologicalReactions().stream()
                            .filter(PhysiologicalReaction::hasEvidences)
                            .flatMap(phyReaction -> phyReaction.getEvidences().stream())
                            .collect(Collectors.toList());
            boolean experimental = hasExperimentalEvidence(evidences);

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

    private boolean hasExperimentalEvidence(List<Evidence> evidences) {
        boolean experimental = false;
        if (Utils.notNullNotEmpty(evidences)) {
            experimental =
                    evidences.stream()
                            .map(Evidence::getEvidenceCode)
                            .filter(Objects::nonNull)
                            .anyMatch(code -> code == EvidenceCode.ECO_0000269);
        }
        return experimental;
    }

    // TODO we are relying on a string to decide if it is experimental, isn't it shaky?
    // TODO we need to document this since it is very critical logic and we need to revisit when implemented using parent child
    private boolean hasExperimentalEvidence(Collection<String> evidences) {
        return evidences.contains("experimental");
    }
}
