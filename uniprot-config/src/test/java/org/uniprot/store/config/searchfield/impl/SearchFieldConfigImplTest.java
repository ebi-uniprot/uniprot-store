package org.uniprot.store.config.searchfield.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.UniProtDataType;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;
import org.uniprot.store.config.searchfield.model.SearchFieldDataType;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.config.searchfield.model.SearchFieldItemType;

class SearchFieldConfigImplTest {
    private static final String CONTEXT_PATH = "/uniprot/api";
    private static SearchFieldConfig searchFieldConfig;

    @BeforeAll
    static void initAll() {
        searchFieldConfig =
                new SearchFieldConfigImpl(
                        UniProtDataType.UNIPROTKB, SearchFieldConfigFactory.UNIPROTKB_CONFIG_FILE);
    }

    @Test
    void testSize() {
        List<SearchFieldItem> items = searchFieldConfig.getSearchFieldItems();
        Assertions.assertNotNull(items);
        assertEquals(467, items.size());
    }

    @Test
    void testXrefCountFields() {
        List<SearchFieldItem> items = searchFieldConfig.getSearchFieldItems();
        Assertions.assertNotNull(items);
        // search fields with xref_count_ prefix
        long xrefCountFieldsCount =
                items.stream()
                        .filter(
                                i ->
                                        StringUtils.isNotEmpty(i.getFieldName())
                                                && i.getFieldName().startsWith("xref_count_"))
                        .count();
        Assertions.assertEquals(191, xrefCountFieldsCount);
    }

    @Test
    void testNonXrefCountFields() {
        List<SearchFieldItem> items = searchFieldConfig.getSearchFieldItems();
        Assertions.assertNotNull(items);
        // search fields with xref_count_ prefix
        long xrefCountFieldsCount =
                items.stream()
                        .filter(
                                i ->
                                        StringUtils.isNotEmpty(i.getFieldName())
                                                && !(i.getFieldName().startsWith("xref_count_")))
                        .count();
        Assertions.assertEquals((452 - 176), xrefCountFieldsCount);
    }

    @Test
    void testSingleGeneNameItem() {
        Optional<SearchFieldItem> item =
                searchFieldConfig.getSearchFieldItems().stream()
                        .filter(val -> "Gene Name [GN]".equals(val.getLabel()))
                        .findFirst();
        assertTrue(item.isPresent());
        assertEquals("gene", item.orElse(new SearchFieldItem()).getFieldName());
        assertEquals(SearchFieldDataType.STRING, item.orElse(new SearchFieldItem()).getDataType());
    }

    @Test
    void testSingleOrganismItem() {
        Optional<SearchFieldItem> item =
                searchFieldConfig.getSearchFieldItems().stream()
                        .filter(val -> "Organism [OS]".equals(val.getLabel()))
                        .findFirst();
        assertTrue(item.isPresent());
        assertEquals("organism_name", item.orElse(new SearchFieldItem()).getFieldName());
        assertEquals(SearchFieldDataType.STRING, item.orElse(new SearchFieldItem()).getDataType());
        assertNotNull(item.orElse(new SearchFieldItem()).getAutoComplete());
        assertEquals(
                CONTEXT_PATH + "/suggester?dict=organism&query=?",
                item.orElse(new SearchFieldItem()).getAutoComplete(CONTEXT_PATH));
    }

    @Test
    void testSingleProteinExistenceItem() {
        Optional<SearchFieldItem> item =
                searchFieldConfig.getSearchFieldItems().stream()
                        .filter(val -> "Protein Existence [PE]".equals(val.getLabel()))
                        .findFirst();
        assertTrue(item.isPresent());
        assertEquals("existence", item.orElse(new SearchFieldItem()).getFieldName());
        assertEquals(SearchFieldDataType.ENUM, item.orElse(new SearchFieldItem()).getDataType());
        assertNull(item.orElse(new SearchFieldItem()).getAutoComplete());
        assertNotNull(item.orElse(new SearchFieldItem()).getValues());
        assertEquals(5, item.orElse(new SearchFieldItem()).getValues().size());
        Optional<SearchFieldItem.Value> tuple =
                item.orElse(new SearchFieldItem()).getValues().stream()
                        .filter(val -> val.getName().equals("Inferred from homology"))
                        .findFirst();
        assertTrue(tuple.isPresent());
        assertEquals("3", tuple.orElse(new SearchFieldItem.Value()).getValue());
    }

    @Test
    void testEncodedInSearchItem() {
        Optional<SearchFieldItem> optionalItem =
                searchFieldConfig.getSearchFieldItems().stream()
                        .filter(val -> "Encoded in".equals(val.getLabel()))
                        .findFirst();
        assertTrue(optionalItem.isPresent());
        SearchFieldItem item = optionalItem.get();
        assertEquals("organelle", item.getFieldName());
        assertEquals(SearchFieldDataType.ENUM, item.getDataType());
        assertNull(item.getAutoComplete());
        assertNotNull(item.getValues());
        assertEquals(10, item.getValues().size());
        Optional<SearchFieldItem.Value> plasmidValue =
                item.getValues().stream()
                        .filter(val -> val.getName().equals("Plasmid"))
                        .findFirst();
        assertTrue(plasmidValue.isPresent());
        assertEquals("plasmid", plasmidValue.get().getValue());
    }

    @Test
    void testFunctionCatalyticActivity() {
        Optional<SearchFieldItem> functionItem =
                searchFieldConfig.getAllFieldItems().stream()
                        .filter(val -> "Function".equals(val.getLabel()))
                        .findFirst();
        assertTrue(functionItem.isPresent());
        assertEquals(
                SearchFieldItemType.GROUP,
                functionItem.orElse(new SearchFieldItem()).getItemType());

        // get function's child
        Optional<SearchFieldItem> catAct =
                searchFieldConfig.getAllFieldItems().stream()
                        .filter(val -> "Catalytic Activity".equals(val.getLabel()))
                        .findFirst();
        assertTrue(catAct.isPresent());
        assertEquals(functionItem.get().getId(), catAct.get().getParentId());

        // get Catalytic Activity children
        List<SearchFieldItem> catActChildren =
                searchFieldConfig.getAllFieldItems().stream()
                        .filter(fi -> "catalytic_activity".equals(fi.getParentId()))
                        .collect(Collectors.toList());
        assertEquals(2, catActChildren.size());
    }

    @Test
    void testDatabase() {
        Optional<SearchFieldItem> item =
                searchFieldConfig.getAllFieldItems().stream()
                        .filter(val -> "Cross-references".equals(val.getLabel()))
                        .findFirst();
        assertTrue(item.isPresent());
        assertEquals(SearchFieldItemType.GROUP, item.orElse(new SearchFieldItem()).getItemType());
    }

    @Test
    void testSingleSecondaryAccessionItem() {
        Optional<SearchFieldItem> item =
                searchFieldConfig.getSearchFieldItems().stream()
                        .filter(val -> "Secondary Accession".equals(val.getLabel()))
                        .findFirst();
        assertTrue(item.isPresent());
        assertEquals("sec_acc", item.get().getFieldName());
        assertEquals(SearchFieldDataType.STRING, item.orElse(new SearchFieldItem()).getDataType());
        assertEquals(2, item.get().getSeqNumber());
    }
}
