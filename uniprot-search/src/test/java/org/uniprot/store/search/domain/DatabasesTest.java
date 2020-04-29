package org.uniprot.store.search.domain;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.xdb.UniProtDatabaseCategory;
import org.uniprot.core.cv.xdb.UniProtDatabaseDetail;
import org.uniprot.cv.xdb.UniProtDatabaseTypes;
import org.uniprot.store.search.domain.impl.Databases;

class DatabasesTest {
    private static Databases instance;

    @BeforeAll
    static void initAll() {
        instance = Databases.INSTANCE;
    }

    @Test
    void fieldUniqueness() {
        Map<String, List<Field>> result =
                instance.getDatabaseFields().stream()
                        .flatMap(val -> val.getFields().stream())
                        .collect(Collectors.groupingBy(Field::getName));

        assertFalse(result.entrySet().stream().anyMatch(val -> val.getValue().size() > 1));
    }

    @Test
    void testField() {
        assertTrue(instance.getField("dr_embl").isPresent());
        assertTrue(instance.getField("dr_ensembl").isPresent());
        assertFalse(instance.getField("embl").isPresent());
    }

    @Test
    void testHasCorrectKnownCrossReferencesSize() {
        List<UniProtDatabaseDetail> allKnownCrossReferences =
                UniProtDatabaseTypes.INSTANCE.getAllDbTypes().stream()
                        .filter(dbd -> !dbd.getCategory().equals(UniProtDatabaseCategory.UNKNOWN))
                        .filter(dbd -> !dbd.isImplicit())
                        .collect(Collectors.toList());

        List<DatabaseGroup> groups = instance.getDatabases();
        assertEquals(20, groups.size());
        List<Tuple> databaseGroupItems =
                groups.stream()
                        .flatMap(g -> g.getItems().stream())
                        .filter(t -> !t.getValue().equals("any"))
                        .collect(Collectors.toList());
        assertEquals(allKnownCrossReferences.size(), databaseGroupItems.size());
        int nDb = groups.stream().mapToInt(val -> val.getItems().size()).sum();
        assertTrue(nDb >= 159);
    }

    @Test
    void testGroup() {
        List<DatabaseGroup> groups = instance.getDatabases();
        assertTrue(
                groups.stream()
                        .anyMatch(
                                val ->
                                        val.getGroupName()
                                                .equals(
                                                        UniProtDatabaseCategory.typeOf("SEQ")
                                                                .getDisplayName())));
        assertFalse(groups.stream().anyMatch(val -> val.getGroupName().equals("GMA")));
        assertTrue(
                groups.stream().anyMatch(val -> val.getGroupName().equals("Proteomic databases")));
    }

    @Test
    void testDatabase() {
        List<DatabaseGroup> groups = instance.getDatabases();
        Optional<DatabaseGroup> ppGroup =
                groups.stream()
                        .filter(
                                val ->
                                        val.getGroupName()
                                                .equals(
                                                        UniProtDatabaseCategory.typeOf("PFAM")
                                                                .getDisplayName()))
                        .findFirst();

        assertTrue(ppGroup.isPresent());
        assertEquals(13, ppGroup.get().getItems().size());
        Optional<Tuple> item =
                ppGroup.get().getItems().stream()
                        .filter(val -> val.getName().equals("MoonProt"))
                        .findFirst();
        assertTrue(item.isPresent());
        assertEquals("moonprot", item.get().getValue());
    }
}
