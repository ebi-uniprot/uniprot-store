package org.uniprot.store.spark.indexer.suggest;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.comment.Comment;
import org.uniprot.core.uniprotkb.comment.CommentType;

/**
 * @author lgonzales
 * @since 2020-01-24
 */
class SuggesterUtilTest {

    @Test
    void getCommentLinesByTypeWithValidComentType() {
        String input =
                "CC   -!- FUNCTION: May be involved in the fusion of the spermatozoa with the\n"
                        + "CC       oocyte during fertilization. {ECO:0000269|PubMed:12640142}.\n"
                        + "CC   -!- SUBUNIT: Interacts with C3b. Interacts with C4b. Interacts with\n"
                        + "CC       moesin/MSN. {ECO:0000250|UniProtKB:P15529}.\n"
                        + "CC   -!- SUBCELLULAR LOCATION: [Isoform 2]: Secreted {ECO:0000305}.";
        String result =
                SuggesterUtil.getCommentLinesByType(input, CommentType.SUBCELLULAR_LOCATION);
        assertNotNull(result);
        assertEquals("CC   -!- SUBCELLULAR LOCATION: [Isoform 2]: Secreted {ECO:0000305}.", result);
    }

    @Test
    void getCommentLinesByTypeWithInvalidComentType() {
        String input =
                "CC   -!- FUNCTION: May be involved in the fusion of the spermatozoa with the\n"
                        + "CC       oocyte during fertilization. {ECO:0000269|PubMed:12640142}.\n"
                        + "CC   -!- SUBUNIT: Interacts with C3b. Interacts with C4b. Interacts with\n"
                        + "CC       moesin/MSN. {ECO:0000250|UniProtKB:P15529}.\n"
                        + "CC   -!- SUBCELLULAR LOCATION: [Isoform 2]: Secreted {ECO:0000305}.\n"
                        + "CC   ---------------------------------------------------------------------------\n"
                        + "CC   Copyrighted by the UniProt Consortium, see https://www.uniprot.org/terms\n"
                        + "CC   Distributed under the Creative Commons Attribution (CC BY 4.0) License\n"
                        + "CC   ---------------------------------------------------------------------------";
        String result = SuggesterUtil.getCommentLinesByType(input, CommentType.COFACTOR);
        assertNotNull(result);
        assertEquals("", result);
    }

    @Test
    void getCommentsWithValidCommentLines() {
        String input =
                "CC   -!- FUNCTION: May be involved in the fusion of the spermatozoa with the\n"
                        + "CC       oocyte during fertilization. {ECO:0000269|PubMed:12640142}.\n"
                        + "CC   -!- SUBUNIT: Interacts with C3b. Interacts with C4b. Interacts with\n"
                        + "CC       moesin/MSN. {ECO:0000250|UniProtKB:P15529}.\n"
                        + "CC   -!- SUBCELLULAR LOCATION: [Isoform 2]: Secreted {ECO:0000305}.";
        List<Comment> result = SuggesterUtil.getComments(input);
        assertNotNull(result);
        assertEquals(3, result.size());

        Comment comment = result.get(0);
        assertEquals(CommentType.FUNCTION, comment.getCommentType());

        comment = result.get(2);
        assertEquals(CommentType.SUBCELLULAR_LOCATION, comment.getCommentType());
    }
}
