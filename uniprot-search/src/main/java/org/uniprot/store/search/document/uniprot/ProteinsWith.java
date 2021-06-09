package org.uniprot.store.search.document.uniprot;

import java.util.Arrays;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.uniprot.core.uniprotkb.comment.CommentType;
import org.uniprot.core.uniprotkb.feature.UniprotKBFeatureType;
import org.uniprot.core.util.EnumDisplay;

/**
 * @author lgonzales
 * @since 03/06/2021
 */
public enum ProteinsWith {
    // IMPORTANT: Make sure you also change uniprotkb.facet.properties
    D3_STRUCTURE(1, "3D structure"), // from CrossReference
    ACT_SITE(2, UniprotKBFeatureType.ACT_SITE),
    ACTIVITY_REGULATION(3, CommentType.ACTIVITY_REGULATION),
    ALLERGEN(4, CommentType.ALLERGEN),
    ALTERNATIVE_PRODUCTS(5, CommentType.ALTERNATIVE_PRODUCTS),
    ALTERNATIVE_SPLICING(6, UniprotKBFeatureType.VAR_SEQ),
    BETA_STRAND(7, UniprotKBFeatureType.STRAND),
    BINARY_INTERACTION(8, CommentType.INTERACTION),
    BINDING_SITE(9, UniprotKBFeatureType.BINDING),
    BIOPHYSICOCHEMICAL_PROPERTIES(10, CommentType.BIOPHYSICOCHEMICAL_PROPERTIES),
    BIOTECHNOLOGICAL_USE(11, CommentType.BIOTECHNOLOGY),
    CALCIUM_BINDING(12, UniprotKBFeatureType.CA_BIND),
    CATALYTIC_ACTIVITY(13, CommentType.CATALYTIC_ACTIVITY),
    CHAIN(14, UniprotKBFeatureType.CHAIN),
    COFACTORS(15, CommentType.COFACTOR),
    COILED_COIL(16, UniprotKBFeatureType.COILED),
    COMPOSITIONAL_BIAS(17, UniprotKBFeatureType.COMPBIAS),
    CROSS_LINK(18, UniprotKBFeatureType.CROSSLNK),
    DEVELOPMENTAL_STAGE(19, CommentType.DEVELOPMENTAL_STAGE),
    DISEASE(20, CommentType.DISEASE),
    DISRUPTION_PHENOTYPE(21, CommentType.DISRUPTION_PHENOTYPE),
    DISULFIDE_BOND(22, UniprotKBFeatureType.DISULFID),
    DNA_BINDING(23, UniprotKBFeatureType.DNA_BIND),
    DOMAIN(24, UniprotKBFeatureType.DOMAIN),
    FUNCTION(25, CommentType.FUNCTION),
    GLYCOSYLATION(26, UniprotKBFeatureType.CARBOHYD),
    HELIX(27, UniprotKBFeatureType.HELIX),
    INDUCTION(28, CommentType.INDUCTION),
    INITIATOR_METHIONINE(29, UniprotKBFeatureType.INIT_MET),
    INTRAMEMBRANE(30, UniprotKBFeatureType.INTRAMEM),
    LIPIDATION(31, UniprotKBFeatureType.LIPID),
    MASS_SPECTROMETRY(32, CommentType.MASS_SPECTROMETRY),
    METAL_BINDING(33, UniprotKBFeatureType.METAL),
    MODIFIED_RESIDUE(34, UniprotKBFeatureType.MOD_RES),
    MOTIF(35, UniprotKBFeatureType.MOTIF),
    MUTAGENESIS(36, UniprotKBFeatureType.MUTAGEN),
    NATURAL_VARIANT(37, UniprotKBFeatureType.VARIANT),
    NON_STANDARD_RESIDUE(38, UniprotKBFeatureType.NON_STD),
    NUCLEOTIDE_BINDING(39, UniprotKBFeatureType.NP_BIND),
    PATHWAY(40, CommentType.PATHWAY),
    PEPTIDE(41, UniprotKBFeatureType.PEPTIDE),
    PHARMACEUTICAL_USE(42, CommentType.PHARMACEUTICAL),
    POLYMORPHISM(43, CommentType.POLYMORPHISM),
    PROPEPTIDE(44, UniprotKBFeatureType.PROPEP),
    PTM_COMMENTS(45, CommentType.PTM),
    REGION(46, UniprotKBFeatureType.REGION),
    REPEAT(47, UniprotKBFeatureType.REPEAT),
    RNA_EDITING(48, CommentType.RNA_EDITING),
    SIGNAL_PEPTIDE(49, UniprotKBFeatureType.SIGNAL),
    SUBCELLULAR_LOCATION(50, CommentType.SUBCELLULAR_LOCATION),
    SUBUNIT_STRUCTURE(51, CommentType.SUBUNIT),
    TISSUE_SPECIFICITY(52, CommentType.TISSUE_SPECIFICITY),
    TOPOLOGICAL_DOMAIN(53, UniprotKBFeatureType.TOPO_DOM),
    TOXIC_DOSE(54, CommentType.TOXIC_DOSE),
    TRANSIT_PEPTIDE(55, UniprotKBFeatureType.TRANSIT),
    TRANSMEMBRANE(56, UniprotKBFeatureType.TRANSMEM),
    TURN(57, UniprotKBFeatureType.TURN),
    ZINC_FINGER(58, UniprotKBFeatureType.ZN_FING);

    private final int value;
    private final EnumDisplay enumDisplay;

    ProteinsWith(int value, String enumLabel) {
        this.value = value;
        this.enumDisplay =
                new EnumDisplay() {
                    @Nonnull
                    @Override
                    public String getName() {
                        return enumLabel;
                    }
                };
    }

    ProteinsWith(int value, EnumDisplay enumDisplay) {
        this.value = value;
        this.enumDisplay = enumDisplay;
    }

    public EnumDisplay getEnumDisplay() {
        return enumDisplay;
    }

    public int getValue() {
        return value;
    }

    public static Optional<ProteinsWith> from(EnumDisplay enumDisplay) {
        return Arrays.stream(ProteinsWith.values())
                .filter(
                        proteinsWith ->
                                proteinsWith
                                        .getEnumDisplay()
                                        .getName()
                                        .equals(enumDisplay.getName()))
                .findAny();
    }

    public static Optional<ProteinsWith> from(String enumDisplayName) {
        return Arrays.stream(ProteinsWith.values())
                .filter(
                        proteinsWith ->
                                proteinsWith.getEnumDisplay().getName().equals(enumDisplayName))
                .findAny();
    }
}
