package org.uniprot.store.indexer.unirule;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.uniprot.core.CrossReference;
import org.uniprot.core.Value;
import org.uniprot.core.uniprotkb.comment.*;
import org.uniprot.core.uniprotkb.evidence.EvidencedValue;

/**
 * @author sahmad
 * @date: 14 May 2020 Converts the {@link Comment} to intermediate form of {@link
 *     UniRuleDocumentComment} before being set to {@link
 *     org.uniprot.store.search.document.unirule.UniRuleDocument}'s commentTypeValues
 */
public class UniRuleCommentConverter {
    private static final String CC_UNDERSCORE = "cc_";
    private static final String SINGLE_SPACE = " ";
    private static final String UNDERSCORE = "_";

    private UniRuleCommentConverter() {}

    public static UniRuleDocumentComment convertToDocumentComment(Comment comment) {
        UniRuleDocumentComment docComment;

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

    private static UniRuleDocumentComment convertFreeTextComment(FreeTextComment comment) {
        Set<String> values =
                comment.getTexts().stream().map(Value::getValue).collect(Collectors.toSet());
        return createDocumentComment(comment, values);
    }

    private static UniRuleDocumentComment convertCatalyticActivity(
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

    private static UniRuleDocumentComment convertCofactor(CofactorComment comment) {
        Set<String> values = new HashSet<>();

        if (comment.hasCofactors()) {
            values =
                    comment.getCofactors().stream()
                            .map(UniRuleCommentConverter::extractCofactorValues)
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

    private static UniRuleDocumentComment convertSubcellLocation(
            SubcellularLocationComment comment) {
        UniRuleDocumentComment docComment = null;
        if (comment.hasSubcellularLocations()) {
            Set<String> values =
                    comment.getSubcellularLocations().stream()
                            .map(UniRuleCommentConverter::extractSubcellLocationValues)
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

    private static UniRuleDocumentComment createDocumentComment(
            Comment comment, Set<String> values) {
        String field = getCommentField(comment);
        UniRuleDocumentComment.UniRuleDocumentCommentBuilder builder =
                UniRuleDocumentComment.builder();
        builder.name(field);
        builder.values(values);
        return builder.build();
    }

    private static String getCommentField(Comment c) {
        StringBuilder builder = new StringBuilder(CC_UNDERSCORE);
        builder.append(c.getCommentType().toXmlDisplayName());
        return builder.toString().replace(SINGLE_SPACE, UNDERSCORE);
    }
}
