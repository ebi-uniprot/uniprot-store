package org.uniprot.store.search.document.uniprot;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import org.uniprot.core.uniprotkb.comment.CommentType;
import org.uniprot.core.uniprotkb.feature.UniprotKBFeatureType;
import org.uniprot.core.util.EnumDisplay;

/**
 * @author lgonzales
 * @since 07/06/2021
 */
class ProteinsWithTest {

    @Test
    void invalidFromString() {
        Optional<ProteinsWith> proteinsWith = ProteinsWith.from("INVALID");
        assertFalse(proteinsWith.isPresent());
    }

    @Test
    void validFromString() {
        String nameValue = "ACTIVITY REGULATION";
        ProteinsWith proteinsWith =
                ProteinsWith.from(nameValue).orElseThrow(AssertionFailedError::new);
        assertEquals(3, proteinsWith.getValue());
        assertEquals(nameValue, proteinsWith.getEnumDisplay().getName());
    }

    @Test
    void valid3DStructureFromString() {
        String nameValue = "3D structure";
        ProteinsWith proteinsWith =
                ProteinsWith.from(nameValue).orElseThrow(AssertionFailedError::new);
        assertEquals(1, proteinsWith.getValue());
        assertEquals(nameValue, proteinsWith.getEnumDisplay().getName());
    }

    @Test
    void validFeatureFromEnumDisplay() {
        EnumDisplay enumDisplay = UniprotKBFeatureType.CARBOHYD;
        ProteinsWith proteinsWith =
                ProteinsWith.from(enumDisplay).orElseThrow(AssertionFailedError::new);
        assertEquals(26, proteinsWith.getValue());
        assertEquals(enumDisplay.getName(), proteinsWith.getEnumDisplay().getName());
    }

    @Test
    void invalidFeatureFromEnumDisplay() {
        EnumDisplay enumDisplay = UniprotKBFeatureType.SITE;
        Optional<ProteinsWith> proteinsWith = ProteinsWith.from(enumDisplay);
        assertFalse(proteinsWith.isPresent());
    }

    @Test
    void validCommentFromEnumDisplay() {
        EnumDisplay enumDisplay = CommentType.CATALYTIC_ACTIVITY;
        ProteinsWith proteinsWith =
                ProteinsWith.from(enumDisplay).orElseThrow(AssertionFailedError::new);
        assertEquals(13, proteinsWith.getValue());
        assertEquals(enumDisplay.getName(), proteinsWith.getEnumDisplay().getName());
    }

    @Test
    void invalidCommentFrom() {
        EnumDisplay enumDisplay = CommentType.CAUTION;
        Optional<ProteinsWith> proteinsWith = ProteinsWith.from(enumDisplay);
        assertFalse(proteinsWith.isPresent());
    }
}
