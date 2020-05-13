package org.uniprot.store.indexer.unirule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.uniprot.core.CrossReference;
import org.uniprot.core.Value;
import org.uniprot.core.flatfile.parser.impl.cc.CCLineBuilderFactory;
import org.uniprot.core.flatfile.writer.FFLineBuilder;
import org.uniprot.core.uniprotkb.comment.*;
import org.uniprot.core.uniprotkb.evidence.EvidencedValue;
import org.uniprot.core.util.Utils;

public class UniRuleCommentConverter {
    private static final String CC_UNDERSCORE = "cc_";
    private static final String SINGLE_SPACE = " ";
    private static final String UNDERSCORE = "_";

    public static List<UniRuleDocumentComment> convertToDocumentComments(List<Comment> comments) {
        List<UniRuleDocumentComment> docComments = new ArrayList<>();
        if (Utils.notNullNotEmpty(comments)) {
            docComments =
                    comments.stream()
                            .map(UniRuleCommentConverter::convertToDocumentComment)
                            .collect(Collectors.toList());
        }

        return docComments;
    }

    public static UniRuleDocumentComment convertToDocumentComment(Comment comment) {
        UniRuleDocumentComment docComment;

        switch (comment.getCommentType()) {
            case ACTIVITY_REGULATION:
            case CAUTION:
            case DOMAIN:
            case FUNCTION:
            case INDUCTION:
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
        Collection<String> values =
                comment.getTexts().stream().map(Value::getValue).collect(Collectors.toList());
        return createDocumentComment(comment, values);
    }

    private static UniRuleDocumentComment convertCatalyticActivity(
            CatalyticActivityComment comment) {
        Reaction reaction = comment.getReaction();
        List<String> values = new ArrayList<>();
        if (reaction.hasReactionCrossReferences()) {
            values =
                    reaction.getReactionCrossReferences().stream()
                            .map(CrossReference::getId)
                            .collect(Collectors.toList());
        }

        if (reaction.hasName()) {
            values.add(reaction.getName());
        }

        return createDocumentComment(comment, values);
    }

    private static UniRuleDocumentComment convertCofactor(CofactorComment comment) {
        List<String> values = new ArrayList<>();

        if (comment.hasCofactors()) {
            values =
                    comment.getCofactors().stream()
                            .map(UniRuleCommentConverter::extractCofactorValues)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());
        }

        if ((comment.hasNote()) && (comment.getNote().hasTexts())) {
            List<String> notes =
                    comment.getNote().getTexts().stream()
                            .map(EvidencedValue::getValue)
                            .collect(Collectors.toList());
            values.addAll(notes);
        }
        return createDocumentComment(comment, values);
    }

    private static List<String> extractCofactorValues(Cofactor cofactor) {
        List<String> nameRefs = new ArrayList<>();
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
            Collection<String> values =
                    comment.getSubcellularLocations().stream()
                            .map(UniRuleCommentConverter::extractSubcellLocationValues)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());
            docComment = createDocumentComment(comment, values);
        }
        return docComment;
    }

    private static List<String> extractSubcellLocationValues(SubcellularLocation subcellLocation) {
        List<String> values = new ArrayList<>();
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
            Comment comment, Collection<String> values) {
        String stringValue = getStringValue(comment);
        String field = getCommentField(comment);
        UniRuleDocumentComment.UniRuleDocumentCommentBuilder builder =
                UniRuleDocumentComment.builder();
        builder.name(field);
        builder.values(values).stringValue(stringValue);
        return builder.build();
    }

    private static String getCommentField(Comment c) {
        StringBuilder builder = new StringBuilder(CC_UNDERSCORE);
        builder.append(c.getCommentType().toXmlDisplayName());
        return builder.toString().replace(SINGLE_SPACE, UNDERSCORE);
    }

    private static String getStringValue(Comment comment) {
        FFLineBuilder<Comment> builder = CCLineBuilderFactory.create(comment);
        return builder.buildString(comment);
    }
}
