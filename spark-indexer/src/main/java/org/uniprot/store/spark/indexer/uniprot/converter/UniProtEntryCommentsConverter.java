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
    private static final Pattern PATTERN_FAMILY =
            Pattern.compile(
                    "(?:In the .+? section; )?[Bb]elongs to the (.+?family)\\.(?: (.+?family)\\.)?(?: (.+?family)\\.)?(?: Highly divergent\\.)?");

    UniProtEntryCommentsConverter(Map<String, String> pathway) {
        this.pathwayRepo = pathway;
    }

    void convertCommentToDocument(List<Comment> comments, UniProtDocument document) {
        for (Comment comment : comments) {
            FFLineBuilder<Comment> fbuilder = CCLineBuilderFactory.create(comment);
            String field = getCommentField(comment);
            String evField = getCommentEvField(comment);
            Collection<String> value =
                    document.commentMap.computeIfAbsent(field, k -> new ArrayList<>());

            String commentVal = fbuilder.buildString(comment);
            value.add(commentVal);
            document.content.add(commentVal);

            Collection<String> evValue =
                    document.commentEvMap.computeIfAbsent(evField, k -> new HashSet<>());
            Set<String> evidences = fetchEvidences(comment);
            evValue.addAll(evidences);

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

    private void convertCommentAP(AlternativeProductsComment comment, UniProtDocument document) {
        List<String> values = new ArrayList<>();
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
        for (String event : events) {
            if ("alternative promoter usage".equalsIgnoreCase(event)) {
                document.apApu.addAll(values);
                document.apApuEv.addAll(evidence);
            }
            if ("alternative splicing".equalsIgnoreCase(event)) {
                document.apAs.addAll(values);
                document.apAsEv.addAll(evidence);
            }
            if ("alternative initiation".equalsIgnoreCase(event)) {
                document.apAi.addAll(values);
                document.apAiEv.addAll(evidence);
            }
            if ("ribosomal frameshifting".equalsIgnoreCase(event)) {
                document.apRf.addAll(values);
                document.apRfEv.addAll(evidence);
            }
        }
    }

    private void convertFactor(CofactorComment comment, UniProtDocument document) {
        if (comment.hasCofactors()) {
            comment.getCofactors()
                    .forEach(
                            val -> {
                                document.cofactorChebi.add(val.getName());
                                if (val.getCofactorCrossReference().getDatabase()
                                        == CofactorDatabase.CHEBI) {
                                    String chebiId = val.getCofactorCrossReference().getId();
                                    document.cofactorChebi.add(chebiId);
                                }
                                document.cofactorChebiEv.addAll(
                                        UniProtEntryConverterUtil.extractEvidence(
                                                val.getEvidences()));
                            });
        }

        if ((comment.hasNote()) && (comment.getNote().hasTexts())) {
            document.cofactorNote.addAll(getTextsValue(comment.getNote().getTexts()));
            document.cofactorNoteEv.addAll(getTextsEvidence(comment.getNote().getTexts()));
        }
    }

    private void convertCommentSC(SequenceCautionComment comment, UniProtDocument document) {
        document.seqCaution.add(comment.getSequenceCautionType().getDisplayName());
        String val =
                "true"; // default value for the type when we do not have note, so the type can be
        // searched with '*'
        if (comment.hasNote()) {
            val = comment.getNote();
            document.seqCaution.add(comment.getNote());
        }

        Set<String> evidence = UniProtEntryConverterUtil.extractEvidence(comment.getEvidences());
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
                document.seqCautionMiscEv.addAll(evidence);
                break;
            default:
        }
    }

    private void convertDiseaseComment(DiseaseComment comment, UniProtDocument document) {
        if (comment.hasDefinedDisease()) {
            String field = getCommentField(comment);
            document.content.add(comment.getDisease().getDiseaseAccession());
            document.commentMap.get(field).add(comment.getDisease().getDiseaseAccession());
        }
    }

    private void convertCommentBPCP(BPCPComment comment, UniProtDocument document) {
        if (comment.hasAbsorption()) {
            Absorption absorption = comment.getAbsorption();
            document.bpcpAbsorption.add("" + absorption.getMax());
            document.bpcpAbsorptionEv.addAll(
                    UniProtEntryConverterUtil.extractEvidence(absorption.getEvidences()));
            if (absorption.hasNote() && absorption.getNote().hasTexts()) {
                document.bpcpAbsorption.addAll(getTextsValue(absorption.getNote().getTexts()));
                document.bpcpAbsorptionEv.addAll(getTextsEvidence(absorption.getNote().getTexts()));
            }
            document.bpcp.addAll(document.bpcpAbsorption);
            document.bpcpEv.addAll(document.bpcpAbsorptionEv);
        }
        if (comment.hasKineticParameters()) {
            convertKineticParameters(document, comment.getKineticParameters());
        }
        if (comment.hasPhDependence() && comment.getPhDependence().hasTexts()) {
            document.bpcpPhDependence.addAll(getTextsValue(comment.getPhDependence().getTexts()));
            document.bpcpPhDependenceEv.addAll(
                    getTextsEvidence(comment.getPhDependence().getTexts()));
            document.bpcp.addAll(document.bpcpPhDependence);
            document.bpcpEv.addAll(document.bpcpPhDependenceEv);
        }
        if (comment.hasRedoxPotential() && comment.getRedoxPotential().hasTexts()) {
            document.bpcpRedoxPotential.addAll(
                    getTextsValue(comment.getRedoxPotential().getTexts()));
            document.bpcpRedoxPotentialEv.addAll(
                    getTextsEvidence(comment.getRedoxPotential().getTexts()));
            document.bpcp.addAll(document.bpcpRedoxPotential);
            document.bpcpEv.addAll(document.bpcpRedoxPotentialEv);
        }
        if (comment.hasTemperatureDependence() && comment.getTemperatureDependence().hasTexts()) {
            document.bpcpTempDependence.addAll(
                    getTextsValue(comment.getTemperatureDependence().getTexts()));
            document.bpcpTempDependenceEv.addAll(
                    getTextsEvidence(comment.getTemperatureDependence().getTexts()));
            document.bpcp.addAll(document.bpcpTempDependence);
            document.bpcpEv.addAll(document.bpcpTempDependenceEv);
        }
    }

    private void convertKineticParameters(UniProtDocument document, KineticParameters kp) {
        if (kp.hasMaximumVelocities()) {
            convertCommentBPCPMaximumVelocity(document, kp.getMaximumVelocities());
        }
        if (kp.hasMichaelisConstants()) {
            convertCommentBPCPMichaelisConstant(document, kp.getMichaelisConstants());
        }
        if (kp.hasNote() && kp.getNote().hasTexts()) {
            document.bpcpKinetics.addAll(getTextsValue(kp.getNote().getTexts()));
            document.bpcpKineticsEv.addAll(getTextsEvidence(kp.getNote().getTexts()));
        }
        document.bpcp.addAll(document.bpcpKinetics);
        document.bpcpEv.addAll(document.bpcpKineticsEv);
    }

    private void convertCommentBPCPMichaelisConstant(
            UniProtDocument document, List<MichaelisConstant> michaelisConstants) {
        michaelisConstants.forEach(
                michaelisConstant -> {
                    if (michaelisConstant.hasConstant()) {
                        document.bpcpKinetics.add(String.valueOf(michaelisConstant.getConstant()));
                    }
                    if (michaelisConstant.hasSubstrate()) {
                        document.bpcpKinetics.add(michaelisConstant.getSubstrate());
                    }
                    if (michaelisConstant.hasEvidences()) {
                        document.bpcpKineticsEv.addAll(
                                UniProtEntryConverterUtil.extractEvidence(
                                        michaelisConstant.getEvidences()));
                    }
                });
    }

    private void convertCommentBPCPMaximumVelocity(
            UniProtDocument document, List<MaximumVelocity> maximumVelocities) {
        maximumVelocities.forEach(
                maximumVelocity -> {
                    if (maximumVelocity.hasEnzyme()) {
                        document.bpcpKinetics.add(maximumVelocity.getEnzyme());
                    }

                    if (maximumVelocity.hasVelocity()) {
                        document.bpcpKinetics.add(String.valueOf(maximumVelocity.getVelocity()));
                    }

                    if (maximumVelocity.hasEvidences()) {
                        document.bpcpKineticsEv.addAll(
                                UniProtEntryConverterUtil.extractEvidence(
                                        maximumVelocity.getEvidences()));
                    }
                });
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
            document.subcellLocationNote.addAll(getTextsValue(comment.getNote().getTexts()));
            document.subcellLocationNoteEv.addAll(getTextsEvidence(comment.getNote().getTexts()));
        }
    }

    private void updateSubcellularLocation(
            UniProtDocument document, SubcellularLocationValue location) {
        document.subcellLocationTerm.add(location.getValue());

        Set<String> locationEv = UniProtEntryConverterUtil.extractEvidence(location.getEvidences());
        document.subcellLocationTermEv.addAll(locationEv);
        document.subcellLocationTerm.add(location.getId());
        document.content.add(location.getId());
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
        comment.getTexts().stream()
                .map(Value::getValue)
                .forEach(val -> updatePathway(val, document));
    }

    private void updatePathway(String val, UniProtDocument document) {
        if (Utils.notNull(pathwayRepo)) {
            String unipathwayAccession = pathwayRepo.get(val);
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

        if (reaction.hasReactionCrossReferences()) {
            String field = this.getCommentField(comment);
            List<CrossReference<ReactionDatabase>> reactionReferences =
                    reaction.getReactionCrossReferences();
            reactionReferences.forEach(
                    val -> {
                        Collection<String> value =
                                doc.commentMap.computeIfAbsent(field, k -> new ArrayList<>());
                        value.add(val.getId());
                    });

            // add rhea ids
            List<String> rheaIds =
                    reactionReferences.stream()
                            .filter(rr -> ReactionDatabase.RHEA.equals(rr.getDatabase()))
                            .map(CrossReference::getId)
                            .collect(Collectors.toList());

            if (Utils.notNullNotEmpty(rheaIds)) {
                doc.rheaIds.addAll(rheaIds);
            }
        }
    }

    private void convertCommentFamily(FreeTextComment comment, UniProtDocument document) {
        comment.getTexts().stream()
                .map(Value::getValue)
                .forEach(val -> updateFamily(val, document));
    }
}
