package org.uniprot.store.indexer.arba;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.uniprot.core.CrossReference;
import org.uniprot.core.Value;
import org.uniprot.core.uniprotkb.comment.*;
import org.uniprot.core.uniprotkb.evidence.EvidencedValue;

/**
 * @author lgonzales
 * @since 16/07/2021
 */
public class ArbaCommentConverter {
    private ArbaCommentConverter() {}

    public static ArbaDocumentComment convertToDocumentComment(Comment comment) {
        ArbaDocumentComment docComment;

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

    private static ArbaDocumentComment convertFreeTextComment(FreeTextComment comment) {
        Set<String> values =
                comment.getTexts().stream().map(Value::getValue).collect(Collectors.toSet());
        return createDocumentComment(comment, values);
    }

    private static ArbaDocumentComment convertCatalyticActivity(CatalyticActivityComment comment) {
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

    private static ArbaDocumentComment convertCofactor(CofactorComment comment) {
        Set<String> values = new HashSet<>();

        if (comment.hasCofactors()) {
            values =
                    comment.getCofactors().stream()
                            .map(ArbaCommentConverter::extractCofactorValues)
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

    private static ArbaDocumentComment convertSubcellLocation(SubcellularLocationComment comment) {
        ArbaDocumentComment docComment = null;
        if (comment.hasSubcellularLocations()) {
            Set<String> values =
                    comment.getSubcellularLocations().stream()
                            .map(ArbaCommentConverter::extractSubcellLocationValues)
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

    private static ArbaDocumentComment createDocumentComment(Comment comment, Set<String> values) {
        String name = comment.getCommentType().toXmlDisplayName();
        ArbaDocumentComment.ArbaDocumentCommentBuilder builder = ArbaDocumentComment.builder();
        builder.name(name);
        builder.values(values);
        return builder.build();
    }
}
