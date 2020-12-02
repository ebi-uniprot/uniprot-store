package org.uniprot.store.reader.publications;

import org.uniprot.core.publication.CommunityAnnotation;
import org.uniprot.core.publication.CommunityMappedReference;
import org.uniprot.core.publication.impl.CommunityMappedReferenceBuilder;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created 02/12/2020
 *
 * @author Edd
 */
public class CommunityMappedReferenceMapper
        extends AbstractMappedReferenceMapper<CommunityMappedReference> {
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
                            + "))(.*)");

    @Override
    CommunityMappedReference convertRawMappedReference(RawMappedReference reference) {
        return new CommunityMappedReferenceBuilder()
                .uniProtKBAccession(reference.accession)
                .source(reference.source)
                .sourceId(reference.sourceId)
                .pubMedId(reference.pubMedId)
                .sourceCategoriesSet(reference.categories)
                .communityAnnotation(convertAnnotation(reference.annotation))
                .build();
    }

    private CommunityAnnotation convertAnnotation(String rawAnnotation) {
        // Protein/gene_name: BraC3; RL3540. Function: BraC3 is an alternative substrate binding
        // component of the ABC transporter braDEFGC. BraC3 supports the transport of leucine,
        // isoleucine, valine, or alanine, but not glutamate or aspartate. Comments: Transport of
        // branched amino acids by either BraC3 (with BraDEFG) or AapJQMP is required for symbiosis
        // with peas.
        rawAnnotation.indexOf("");
        Matcher matcher = SECTION_DELIMITER_PATTERN.matcher(rawAnnotation);
//        matcher.matches()
        return null;
    }
}
