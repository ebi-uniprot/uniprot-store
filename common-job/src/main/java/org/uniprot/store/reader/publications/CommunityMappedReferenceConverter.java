package org.uniprot.store.reader.publications;

import org.uniprot.core.publication.CommunityAnnotation;
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.core.publication.impl.CommunityAnnotationBuilder;
import org.uniprot.core.publication.impl.CommunityMappedReferenceBuilder;
import org.uniprot.core.publication.impl.MappedSourceBuilder;
import org.uniprot.core.util.Utils;

import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created 02/12/2020
 *
 * @author Edd
 */
public class CommunityMappedReferenceConverter
        extends AbstractMappedReferenceConverter<CommunityMappedReference> {
    private static final String PROTEIN_GENE_DELIMITER = "Protein/gene_name: ";
    private static final String FUNCTION_DELIMITER = "Function: ";
    private static final String DISEASE_DELIMITER = "Disease: ";
    private static final String COMMENT_DELIMITER = "Comments: ";
    private static final Pattern SECTION_DELIMITER_PATTERN =
            Pattern.compile(
                    "(("
                            + PROTEIN_GENE_DELIMITER
                            + ")|("
                            + FUNCTION_DELIMITER
                            + ")|("
                            + DISEASE_DELIMITER
                            + ")|("
                            + COMMENT_DELIMITER
                            + "))");

    @Override
    CommunityMappedReference convertRawMappedReference(RawMappedReference reference) {
        return new CommunityMappedReferenceBuilder()
                .uniProtKBAccession(reference.accession)
                .source(
                        new MappedSourceBuilder()
                                .name(reference.source)
                                .id(reference.sourceId)
                                .build())
                .pubMedId(reference.pubMedId)
                .sourceCategoriesSet(reference.categories)
                .communityAnnotation(convertAnnotation(reference.annotation))
                .build();
    }

    private CommunityAnnotation convertAnnotation(String rawAnnotation) {
        Matcher matcher = SECTION_DELIMITER_PATTERN.matcher(rawAnnotation);
        int prevMatchEnd = 0;

        CommunityAnnotationBuilder builder = new CommunityAnnotationBuilder();
        CommunityAnnotationCommentType commentType = null;
        String commentValue;

        while (matcher.find()) {
            if (prevMatchEnd != 0) {
                commentValue = rawAnnotation.substring(prevMatchEnd, matcher.start());
                updateCommunityAnnotationBuilder(commentType, builder, commentValue);
            }
            commentType =
                    CommunityAnnotationCommentType.getCommentType(
                            rawAnnotation.substring(matcher.start(), matcher.end()));

            prevMatchEnd = matcher.end();
        }
        updateCommunityAnnotationBuilder(
                commentType, builder, rawAnnotation.substring(prevMatchEnd));

        return builder.build();
    }

    private void updateCommunityAnnotationBuilder(
            CommunityAnnotationCommentType commentType,
            CommunityAnnotationBuilder builder,
            String value) {
        if (commentType != null) {
            String trimmed = value.trim();
            if (Utils.notNullNotEmpty(trimmed)
                    && trimmed.indexOf(' ') == -1
                    && trimmed.charAt(trimmed.length() - 1) == '.') {
                trimmed = trimmed.substring(0, trimmed.length() - 1);
            }

            commentType.updateCommunityAnnotationBuilder(builder, trimmed);
        }
    }

    private enum CommunityAnnotationCommentType {
        PROTEIN_GENE(CommunityAnnotationBuilder::proteinOrGene),
        FUNCTION(CommunityAnnotationBuilder::function),
        DISEASE(CommunityAnnotationBuilder::disease),
        COMMENT(CommunityAnnotationBuilder::comment);
        private final BiConsumer<CommunityAnnotationBuilder, String> annotationBuilderSetter;

        CommunityAnnotationCommentType(BiConsumer<CommunityAnnotationBuilder, String> annotation) {
            this.annotationBuilderSetter = annotation;
        }

        static CommunityAnnotationCommentType getCommentType(String typeAsString) {
            switch (typeAsString) {
                case PROTEIN_GENE_DELIMITER:
                    return PROTEIN_GENE;
                case FUNCTION_DELIMITER:
                    return FUNCTION;
                case DISEASE_DELIMITER:
                    return DISEASE;
                case COMMENT_DELIMITER:
                    return COMMENT;
                default:
                    throw new IllegalArgumentException("Unknown comment type: " + typeAsString);
            }
        }

        void updateCommunityAnnotationBuilder(
                CommunityAnnotationBuilder builder, String annotation) {
            annotationBuilderSetter.accept(builder, annotation);
        }
    }
}
