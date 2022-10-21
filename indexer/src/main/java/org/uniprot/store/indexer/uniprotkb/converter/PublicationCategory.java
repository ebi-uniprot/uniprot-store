package org.uniprot.store.indexer.uniprotkb.converter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author lgonzales
 * @since 2019-12-12
 */
public enum PublicationCategory {
    sequence(
            "Sequences",
            "NUCLEOTIDE SEQUENCE",
            "PROTEIN SEQUENCE",
            "GENOME REANNOTATION",
            "VARIANT",
            "VARIANTS",
            "SEQUENCE REVISION",
            "MASS SPECTROMETRY",
            "ALTERNATIVE SPLICING",
            "RNA EDITING",
            "POLYMORPHISM",
            "GENE MODEL",
            "COMPOSITION",
            "ALTERNATIVE INITIATION",
            "SEQUENCE",
            "ALTERNATIVE PROMOTER USAGE",
            "RECONSTITUTION",
            "COMPLETE GENOME",
            "COMPLETE PLASTID GENOME",
            "GENE STRUCTURE",
            "CONCEPTUAL TRANSLATION",
            "RIBOSOMAL FRAMESHIFT",
            "GENE DUPLICATION",
            "AMINO-ACID COMPOSITION",
            "CHROMOSOMAL REARRANGEMENT",
            "SELENOCYSTEINE",
            "SPLICE ISOFORM(S) THAT ARE POTENTIAL NMD TARGET(S)"),

    function(
            "Function",
            "FUNCTION",
            "CATALYTIC ACTIVITY",
            "BIOPHYSICOCHEMICAL PROPERTIES",
            "ENZYME REGULATION",
            "COFACTOR",
            "SUBSTRATE SPECIFICITY",
            "RNA-BINDING",
            "ROLE",
            "REACTION MECHANISM",
            "ATP-BINDING",
            "PATHWAY",
            "DNA-BINDING",
            "ACTIVE SITE",
            "PH DEPENDENCE",
            "ENZYME ACTIVITY",
            "REGULATION",
            "KINETIC PARAMETERS",
            "INHIBITION",
            "FUNCTION (MICROBIAL INFECTION)",
            "BINDING",
            "CALCIUM-BINDING",
            "TRANSCRIPTIONAL REGULATION",
            "ACTIVE SITES",
            "ZINC-BINDING",
            "ACTIVATION",
            "ZINC-BINDING SITES",
            "REPRESSION",
            "METAL-BINDING SITES",
            "LIPID-BINDING",
            "CALCIUM-BINDING SITES",
            "ATPASE ACTIVITY",
            "SUBSTRATES",
            "HEME-BINDING",
            "LIGAND-BINDING",
            "ADP-RIBOSYLATION",
            "GTP-BINDING",
            "MECHANISM",
            "CATALYTIC MECHANISM",
            "METAL-BINDING",
            "SUBSTRATE-BINDING SITES",
            "ACTIVITY PROFILE",
            "METHYLTRANSFERASE ACTIVITY",
            "SITE",
            "BIOSYNTHESIS",
            "COPPER-BINDING",
            "ACTIN-BINDING",
            "ENZYMATIC ACTIVITY",
            "IRON-BINDING SITES",
            "HEPARIN-BINDING"),

    subcell(
            "Subcellular Location",
            "SUBCELLULAR LOCATION",
            "TOPOLOGY",
            "MEMBRANE TOPOLOGY",
            "SECRETION VIA TYPE III SECRETION SYSTEM"),

    interaction(
            "Interaction",
            "INTERACTION",
            "SUBUNIT",
            "SELF-ASSOCIATION",
            "HOMODIMERIZATION",
            "COMPLEX",
            "DIMERIZATION",
            "OLIGOMERIZATION",
            "HETERODIMERIZATION",
            "HOMODIMER",
            "HOMOOLIGOMERIZATION",
            "UBIQUITIN-BINDING",
            "COMPLEX FORMATION",
            "STOICHIOMETRY",
            "HETERODIMER"),

    ptm(
            "PTM / Processing",
            "PHOSPHORYLATION",
            "ACETYLATION",
            "GLYCOSYLATION",
            "CLEAVAGE",
            "DISULFIDE BONDS",
            "DISULFIDE BOND",
            "METHYLATION",
            "PROTEOLYTIC PROCESSING",
            "SUMOYLATION",
            "AUTOPHOSPHORYLATION",
            "PYROGLUTAMATE FORMATION",
            "SUCCINYLATION",
            "PALMITOYLATION",
            "HYDROXYLATION",
            "MYRISTOYLATION",
            "SULFATION",
            "PROTEOLYTIC CLEAVAGE",
            "ISOPRENYLATION",
            "DEPHOSPHORYLATION",
            "PUPYLATION",
            "GAMMA-CARBOXYGLUTAMATION",
            "DEUBIQUITINATION",
            "DEGRADATION",
            "AUTOUBIQUITINATION",
            "S-NITROSYLATION",
            "BLOCKAGE",
            "AUTOCATALYTIC CLEAVAGE",
            "CITRULLINATION",
            "PTM",
            "OXIDATION",
            "CROSS-LINKING",
            "ISGYLATION",
            "DEACETYLATION",
            "CLEAVAGE SITE",
            "PYRIDOXAL PHOSPHATE",
            "DEAMIDATION",
            "FORMYLATION",
            "CROTONYLATION",
            "CARBAMYLATION",
            "NITRATION",
            "CONJUGATION",
            "D-AMINO ACID",
            "SIGNAL SEQUENCE CLEAVAGE SITE",
            "PHOSPHOPANTETHEINYLATION",
            "DIACYLGLYCEROL",
            "PROTEASOMAL DEGRADATION",
            "POST-TRANSLATIONAL MODIFICATIONS",
            "GLYCYLATION",
            "PROCESSING",
            "INTERCHAIN DISULFIDE BOND",
            "TRANSIT PEPTIDE CLEAVAGE SITE",
            "GLYCATION",
            "CROSS-LINK",
            "UBIQUITINATION",
            "AMIDATION",
            "GPI-ANCHOR",
            "NEDDYLATION"),

    expression(
            "Expression",
            "TISSUE SPECIFICITY",
            "INDUCTION",
            "DEVELOPMENTAL",
            "STAGE",
            "LEVEL OF TISSUE EXPRESSION",
            "EXPRESSION"),

    structure(
            "Structure",
            "X-RAY CRYSTALLOGRAPHY",
            "STRUCTURE",
            "3D-STRUCTURE MODELING",
            "CRYSTALLIZATION",
            "EPR SPECTROSCOPY",
            "CIRCULAR DICHROISM ANALYSIS",
            "CIRCULAR DICHROISM",
            "MODELING"),

    pathol(
            "Phenotypes & Variants",
            "Disease & Variants",
            List.of(
                    "MUTAGENESIS",
                    "DISRUPTION PHENOTYPE",
                    "INVOLVEMENT",
                    "CHROMOSOMAL TRANSLOCATION",
                    "LETHAL DOSE",
                    "BIOTECHNOLOGY",
                    "DISEASE",
                    "ALLERGEN",
                    "MUTANT",
                    "MUTANTS",
                    "TOXIC DOSE",
                    "TOXIN TARGET",
                    "PARALYTIC DOSE")),

    names("Names", "GENE FAMILY", "NOMENCLATURE", "GENE NAME", "GENE FAMILY AND NOMENCLATURE"),

    family(
            "Family & Domains",
            "DOMAIN",
            "GENE FAMILY ORGANIZATION",
            "SIMILARITY",
            "DOMAINS",
            "NUCLEAR LOCALIZATION SIGNAL",
            "REGION",
            "NUCLEAR EXPORT SIGNAL",
            "COILED-COIL DOMAIN",
            "MOTIF",
            "REPEATS");

    private List<String> functionTexts;
    private String label;
    private String alternativeLabel;

    private PublicationCategory(String label, String... functionTexts) {
        this.label = label;
        this.functionTexts = Arrays.stream(functionTexts).collect(Collectors.toList());
    }

    private PublicationCategory(String label, String alternativeLabel, List<String> functionTexts) {
        this.label = label;
        this.alternativeLabel = alternativeLabel;
        this.functionTexts = functionTexts;
    }

    public List<String> getFunctionTexts() {
        return functionTexts;
    }

    public String getLabel() {
        return label;
    }

    public String getAlternativeLabel() {
        return alternativeLabel;
    }
}
