package org.uniprot.store.indexer.common.aa;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.uniprot.core.CrossReference;
import org.uniprot.core.Value;
import org.uniprot.core.uniprotkb.comment.CatalyticActivityComment;
import org.uniprot.core.uniprotkb.comment.Cofactor;
import org.uniprot.core.uniprotkb.comment.CofactorComment;
import org.uniprot.core.uniprotkb.comment.Comment;
import org.uniprot.core.uniprotkb.comment.FreeTextComment;
import org.uniprot.core.uniprotkb.comment.Reaction;
import org.uniprot.core.uniprotkb.comment.SubcellularLocation;
import org.uniprot.core.uniprotkb.comment.SubcellularLocationComment;
import org.uniprot.core.uniprotkb.evidence.EvidencedValue;
import org.uniprot.core.unirule.Annotation;
import org.uniprot.core.unirule.UniRuleEntry;
import org.uniprot.core.util.Utils;

/**
 * @author sahmad
 * @date: 14 May 2020 Converts the {@link Comment} to intermediate form of {@link
 *     AARuleDocumentComment} before being set to {@link
 *     org.uniprot.store.search.document.unirule.UniRuleDocument}'s commentTypeValues
 */
public class AARuleCommentConverter {
    private AARuleCommentConverter() {}

    public static List<AARuleDocumentComment> convertToAARuleDocumentComments(UniRuleEntry uniObj) {
        List<AARuleDocumentComment> docComments = new ArrayList<>();
        List<Annotation> annotations = uniObj.getMainRule().getAnnotations();
        if (Utils.notNullNotEmpty(annotations)) {
            docComments =
                    annotations.stream()
                            .map(Annotation::getComment)
                            .filter(Objects::nonNull)
                            .map(AARuleCommentConverter::convertToDocumentComment)
                            .collect(Collectors.toList());
        }
        return docComments;
    }

    public static AARuleDocumentComment convertToDocumentComment(Comment comment) {
        AARuleDocumentComment docComment;

        switch (comment.getCommentType()) {
            case ACTIVITY_REGULATION:
            case CAUTION:
            case DOMAIN:
            case FUNCTION:
            case INDUCTION:
            case MISCELLANEOUS:
            case PATHWAY:
            case PTM:
            case SIMILARITY:
            case SUBUNIT:
                docComment = convertFreeTextComment((FreeTextComment) comment);
                break;
            case CATALYTIC_ACTIVITY:
                docComment = convertCatalyticActivity((CatalyticActivityComment) comment);
                break;
            case COFACTOR:
                docComment = convertCofactor((CofactorComment) comment);
                break;
            case SUBCELLULAR_LOCATION:
                docComment = convertSubcellLocation((SubcellularLocationComment) comment);
                break;
            default:
                throw new IllegalArgumentException(
                        "Comment type '" + comment.getCommentType() + "' not supported.");
        }
        return docComment;
    }

    private static AARuleDocumentComment convertFreeTextComment(FreeTextComment comment) {
        Set<String> values =
                comment.getTexts().stream().map(Value::getValue).collect(Collectors.toSet());
        return createDocumentComment(comment, values);
    }

    private static AARuleDocumentComment convertCatalyticActivity(
            CatalyticActivityComment comment) {
        Reaction reaction = comment.getReaction();
        Set<String> values = new HashSet<>();
        if (reaction.hasReactionCrossReferences()) {
            values =
                    reaction.getReactionCrossReferences().stream()
                            .map(CrossReference::getId)
                            .collect(Collectors.toSet());
        }

        if (reaction.hasName()) {
            values.add(reaction.getName());
        }

        return createDocumentComment(comment, values);
    }

    private static AARuleDocumentComment convertCofactor(CofactorComment comment) {
        Set<String> values = new HashSet<>();

        if (comment.hasCofactors()) {
            values =
                    comment.getCofactors().stream()
                            .map(AARuleCommentConverter::extractCofactorValues)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toSet());
        }

        if ((comment.hasNote()) && (comment.getNote().hasTexts())) {
            Set<String> notes =
                    comment.getNote().getTexts().stream()
                            .map(EvidencedValue::getValue)
                            .collect(Collectors.toSet());
            values.addAll(notes);
        }
        return createDocumentComment(comment, values);
    }

    private static Set<String> extractCofactorValues(Cofactor cofactor) {
        Set<String> nameRefs = new HashSet<>();
        if (cofactor.hasName()) {
            nameRefs.add(cofactor.getName());
        }

        if (cofactor.hasCofactorCrossReference()) {
            nameRefs.add(cofactor.getCofactorCrossReference().getId());
        }
        return nameRefs;
    }

    private static AARuleDocumentComment convertSubcellLocation(
            SubcellularLocationComment comment) {
        AARuleDocumentComment docComment = null;
        if (comment.hasSubcellularLocations()) {
            Set<String> values =
                    comment.getSubcellularLocations().stream()
                            .map(AARuleCommentConverter::extractSubcellLocationValues)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toSet());
            docComment = createDocumentComment(comment, values);
        }
        return docComment;
    }

    private static Set<String> extractSubcellLocationValues(SubcellularLocation subcellLocation) {
        Set<String> values = new HashSet<>();
        if (subcellLocation.hasLocation()) {
            values.add(subcellLocation.getLocation().getValue());
        }
        if (subcellLocation.hasOrientation()) {
            values.add(subcellLocation.getOrientation().getValue());
        }
        if (subcellLocation.hasTopology()) {
            values.add(subcellLocation.getTopology().getValue());
        }
        return values;
    }

    private static AARuleDocumentComment createDocumentComment(
            Comment comment, Set<String> values) {
        String name = comment.getCommentType().toXmlDisplayName();
        AARuleDocumentComment.AARuleDocumentCommentBuilder builder =
                AARuleDocumentComment.builder();
        builder.name(name);
        builder.values(values);
        return builder.build();
    }
}
