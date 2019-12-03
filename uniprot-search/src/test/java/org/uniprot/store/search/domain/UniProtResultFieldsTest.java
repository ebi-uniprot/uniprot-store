package org.uniprot.store.search.domain;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.store.search.domain.impl.FieldGroupImpl;
import org.uniprot.store.search.domain.impl.FieldImpl;
import org.uniprot.store.search.domain.impl.UniProtResultFields;

import com.google.common.base.Strings;

class UniProtResultFieldsTest {
    private static UniProtResultFields instance;

    @BeforeAll
    static void initAll() {
        instance = UniProtResultFields.INSTANCE;
    }

    @Test
    void fieldUniqueness() {
        Map<String, List<Field>> result =
                instance.getResultFields().stream()
                        .flatMap(val -> val.getFields().stream())
                        .collect(Collectors.groupingBy(Field::getName));

        assertFalse(result.entrySet().stream().anyMatch(val -> val.getValue().size() > 1));
    }

    @Test
    void testGetField() {
        assertTrue(instance.getField("accession").isPresent());
        assertTrue(instance.getField("protein_name").isPresent());
        assertFalse(instance.getField("protein").isPresent());
    }

    @Test
    void testResultFieldSize() {
        List<FieldGroup> groups = instance.getResultFields();
        assertEquals(15, groups.size());
        verifyGroupSize(groups, "Names & Taxonomy", 13);
        verifyGroupSize(groups, "Sequences", 19);
        verifyGroupSize(groups, "Function", 18);
        verifyGroupSize(groups, "Miscellaneous", 11);
        verifyGroupSize(groups, "Interaction", 2);
        verifyGroupSize(groups, "Expression", 3);
        verifyGroupSize(groups, "Gene Ontology (GO)", 5);
        verifyGroupSize(groups, "Pathology & Biotech", 7);
        verifyGroupSize(groups, "Subcellular location", 4);
        verifyGroupSize(groups, "PTM / Processing", 12);
        verifyGroupSize(groups, "Structure", 4);
        verifyGroupSize(groups, "Publications", 2);
        verifyGroupSize(groups, "Date of", 4);
        verifyGroupSize(groups, "Family & Domains", 10);
        verifyGroupSize(groups, "Taxonomic identifier", 1);
    }

    private void verifyGroupSize(List<FieldGroup> groups, String groupName, int size) {
        Optional<FieldGroup> group = getGroup(groups, groupName);
        assertTrue(group.isPresent());
        assertEquals(size, group.orElse(new FieldGroupImpl()).getFields().size());
    }

    @Test
    void testResultFieldGroup() {
        List<FieldGroup> groups = instance.getResultFields();
        assertEquals(15, groups.size());
        System.out.println(
                groups.stream()
                        .flatMap(val -> val.getFields().stream())
                        .map(Field::getName)
                        .filter(val -> val.startsWith("ft_"))
                        .map(val -> "\"" + val + "\"")
                        .collect(Collectors.joining(", ")));

        Optional<FieldGroup> seqGroup = getGroup(groups, "Sequences");
        assertTrue(seqGroup.isPresent());
        assertEquals(19, seqGroup.orElse(new FieldGroupImpl()).getFields().size());
        Optional<Field> massField = getField(seqGroup.orElse(new FieldGroupImpl()), "Mass");
        assertTrue(massField.isPresent());
        assertEquals("mass", massField.orElse(new FieldImpl()).getName());
    }

    @Test
    void testResultField() {
        List<FieldGroup> groups = instance.getResultFields();
        verifyField(groups, "Names & Taxonomy", "Gene Names", "gene_names");
        verifyField(groups, "Sequences", "Alternative sequence", "ft_var_seq");
        verifyField(groups, "Function", "Kinetics", "kinetics");
        verifyField(groups, "Miscellaneous", "Caution", "cc_caution");
        verifyField(groups, "Interaction", "Subunit structure", "cc_subunit");
        verifyField(groups, "Expression", "Induction", "cc_induction");
        verifyField(groups, "Gene Ontology (GO)", "Gene Ontology (cellular component)", "go_c");
        verifyField(groups, "Pathology & Biotech", "Mutagenesis", "ft_mutagen");
        verifyField(
                groups,
                "Subcellular location",
                "Subcellular location [CC]",
                "cc_subcellular_location");
        verifyField(groups, "PTM / Processing", "Cross-link", "ft_crosslnk");
        verifyField(groups, "Structure", "3D", "3d");
        verifyField(groups, "Publications", "PubMed ID", "pm_id");
        verifyField(groups, "Date of", "Date of creation", "date_create");
        verifyField(groups, "Family & Domains", "Compositional bias", "ft_compbias");
        verifyField(groups, "Taxonomic identifier", "Taxonomic lineage IDs", "tax_id");
    }

    @Test
    void allFields() {
        List<FieldGroup> groups = instance.getResultFields();
        groups.stream()
                .flatMap(val -> val.getFields().stream())
                .map(val -> val.getName())
                .distinct()
                .filter(val -> !val.startsWith("ft_"))
                .filter(val -> !val.startsWith("cc_"))
                .filter(val -> !val.startsWith("dr_"))
                .filter(val -> !Strings.isNullOrEmpty(val))
                .forEach(System.out::println);
    }

    @Test
    void testOrganelle() {
        Optional<Field> field = instance.getField("organelle");
        System.out.println(field.get().getJavaFieldName());
    }

    void testDatabaseFieldSize() {
        List<FieldGroup> groups = instance.getDatabaseFields();
        assertEquals(19, groups.size());
        verifyGroupSize(groups, "SEQ", 4);
        verifyGroupSize(groups, "3DS", 3);
        verifyGroupSize(groups, "PPI", 8);
        verifyGroupSize(groups, "CHEMISTRY", 5);
        verifyGroupSize(groups, "PFAM", 12);
        verifyGroupSize(groups, "PTM", 7);
        verifyGroupSize(groups, "PMD", 3);
        verifyGroupSize(groups, "2DG", 7);
        verifyGroupSize(groups, "PROTEOMIC", 11);
        verifyGroupSize(groups, "PAM", 2);
        verifyGroupSize(groups, "GMA", 14);
        verifyGroupSize(groups, "ORG", 38);
        verifyGroupSize(groups, "PLG", 9);
        verifyGroupSize(groups, "EAP", 7);
        verifyGroupSize(groups, "OTHER", 7);
        verifyGroupSize(groups, "GEP", 5);
        verifyGroupSize(groups, "FMD", 15);
        verifyGroupSize(groups, "OTG", 1);
        verifyGroupSize(groups, "PRM", 0);
    }

    void testDatabaseField() {
        List<FieldGroup> groups = instance.getDatabaseFields();
        assertEquals(19, groups.size());
        verifyField(groups, "SEQ", "EMBL", "dr_embl");
        verifyField(groups, "3DS", "PDB", "dr_pdb");
        verifyField(groups, "PPI", "CORUM", "dr_corum");
        verifyField(groups, "CHEMISTRY", "ChEMBL", "dr_chembl");
        verifyField(groups, "PFAM", "IMGT_GENE-DB", "dr_imgt_gene-db");
        verifyField(groups, "PTM", "GlyConnect", "dr_glyconnect");
        verifyField(groups, "PMD", "dbSNP", "dr_dbsnp");
        verifyField(groups, "2DG", "SWISS-2DPAGE", "dr_swiss-2dpage");
        verifyField(groups, "PROTEOMIC", "PRIDE", "dr_pride");
        verifyField(groups, "PAM", "DNASU", "dr_dnasu");
        verifyField(groups, "GMA", "Ensembl", "dr_ensembl");
        verifyField(groups, "ORG", "DisGeNET", "dr_disgenet");
        verifyField(groups, "PLG", "KO", "dr_ko");
        verifyField(groups, "EAP", "BRENDA", "dr_brenda");
        verifyField(groups, "OTHER", "GeneWiki", "dr_genewiki");
        verifyField(groups, "GEP", "Bgee", "dr_bgee");
        verifyField(groups, "FMD", "HAMAP", "dr_hamap");
    }

    private void verifyField(List<FieldGroup> groups, String groupName, String label, String name) {
        Optional<FieldGroup> group = getGroup(groups, groupName);
        assertTrue(group.isPresent());
        Optional<Field> field = getField(group.orElse(new FieldGroupImpl()), label);
        assertTrue(field.isPresent());
        assertEquals(name, field.orElse(new FieldImpl()).getName());
    }

    private Optional<FieldGroup> getGroup(List<FieldGroup> groups, String groupName) {
        return groups.stream().filter(group -> group.getGroupName().equals(groupName)).findFirst();
    }

    private Optional<Field> getField(FieldGroup group, String label) {
        return group.getFields().stream().filter(val -> val.getLabel().equals(label)).findFirst();
    }
}
