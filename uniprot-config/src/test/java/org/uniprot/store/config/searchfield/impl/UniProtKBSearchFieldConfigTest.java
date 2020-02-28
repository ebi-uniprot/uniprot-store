package org.uniprot.store.config.searchfield.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;
import org.uniprot.store.config.searchfield.factory.UniProtDataType;
import org.uniprot.store.config.searchfield.model.SearchFieldDataType;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.config.searchfield.model.SearchFieldItemType;

public class UniProtKBSearchFieldConfigTest {
    private static SearchFieldConfig searchFieldConfig;

    @BeforeAll
    static void initAll() {
        searchFieldConfig =
                SearchFieldConfigFactory.getSearchFieldConfig(UniProtDataType.uniprotkb);
    }

    @Test
    void testSize() {
        List<SearchFieldItem> items = searchFieldConfig.getSearchFieldItems();
        assertEquals(492, items.size());
    }

    @Test
    void testSingleGeneNameItem() {
        Optional<SearchFieldItem> item =
                searchFieldConfig.getSearchFieldItems().stream()
                        .filter(val -> "Gene Name [GN]".equals(val.getLabel()))
                        .findFirst();
        assertTrue(item.isPresent());
        assertEquals("gene", item.orElse(new SearchFieldItem()).getFieldName());
        assertEquals(SearchFieldDataType.string, item.orElse(new SearchFieldItem()).getDataType());
    }

    @Test
    void testSingleOrganismItem() {
        Optional<SearchFieldItem> item =
                searchFieldConfig.getSearchFieldItems().stream()
                        .filter(val -> "Organism [OS]".equals(val.getLabel()))
                        .findFirst();
        assertTrue(item.isPresent());
        assertEquals("organism_name", item.orElse(new SearchFieldItem()).getFieldName());
        assertEquals(SearchFieldDataType.string, item.orElse(new SearchFieldItem()).getDataType());
        assertNotNull(item.orElse(new SearchFieldItem()).getAutoComplete());
        assertEquals(
                "/uniprot/api/suggester?dict=organism&query=?",
                item.orElse(new SearchFieldItem()).getAutoComplete());
    }

    @Test
    void testSingleProteinExistenceItem() {
        Optional<SearchFieldItem> item =
                searchFieldConfig.getSearchFieldItems().stream()
                        .filter(val -> "Protein Existence [PE]".equals(val.getLabel()))
                        .findFirst();
        assertTrue(item.isPresent());
        assertEquals("existence", item.orElse(new SearchFieldItem()).getFieldName());
        assertEquals(
                SearchFieldDataType.enumeration, item.orElse(new SearchFieldItem()).getDataType());
        assertNull(item.orElse(new SearchFieldItem()).getAutoComplete());
        assertNotNull(item.orElse(new SearchFieldItem()).getValues());
        assertEquals(5, item.orElse(new SearchFieldItem()).getValues().size());
        Optional<SearchFieldItem.Value> tuple =
                item.orElse(new SearchFieldItem()).getValues().stream()
                        .filter(val -> val.getName().equals("Inferred from homology"))
                        .findFirst();
        assertTrue(tuple.isPresent());
        assertEquals("homology", tuple.orElse(new SearchFieldItem.Value()).getValue());
    }

    @Test
    void testFunctionCatalyticActivity() {
        Optional<SearchFieldItem> functionItem =
                searchFieldConfig.getAllFieldItems().stream()
                        .filter(val -> "Function".equals(val.getLabel()))
                        .findFirst();
        assertTrue(functionItem.isPresent());
        assertEquals(
                SearchFieldItemType.group,
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

        //        assertNotNull(item.orElse(new UniProtSearchItem()).getItems());
        //        Optional<SearchItem> subItem =
        //                item.orElse(new UniProtSearchItem()).getItems().stream()
        //                        .filter(val -> val.getLabel().equals("Catalytic Activity"))
        //                        .findFirst();
        //        assertTrue(subItem.isPresent());
        //        assertEquals(SearchDataType.STRING, subItem.orElse(new
        // UniProtSearchItem()).getDataType());
        //        assertEquals(SearchItemType.COMMENT, subItem.orElse(new
        // UniProtSearchItem()).getItemType());
        //        assertEquals("catalytic_activity", subItem.orElse(new
        // UniProtSearchItem()).getTerm());
        //        assertTrue(subItem.orElse(new UniProtSearchItem()).isHasEvidence());
    }

    @Test
    void testFunction() {}

    @Test
    void testDatabase() {
        Optional<SearchFieldItem> item =
                searchFieldConfig.getAllFieldItems().stream()
                        .filter(val -> "Cross-references".equals(val.getLabel()))
                        .findFirst();
        assertTrue(item.isPresent());
        assertEquals(SearchFieldItemType.group, item.orElse(new SearchFieldItem()).getItemType());
    }

    //    @Test
    //    void testFunctionChebiTerm() {
    //        Optional<SearchItem> item =
    //                searchItems.getSearchItems().stream()
    //                        .filter(val -> val.getLabel().equals("Function"))
    //                        .findFirst();
    //        assertTrue(item.isPresent());
    //        assertEquals(SearchItemType.GROUP, item.orElse(new
    // UniProtSearchItem()).getItemType());
    //        assertNotNull(item.orElse(new UniProtSearchItem()).getItems());
    //        Optional<SearchItem> subItem =
    //                item.orElse(new UniProtSearchItem()).getItems().stream()
    //                        .filter(val -> val.getLabel().equals("Cofactors"))
    //                        .findFirst();
    //        assertTrue(subItem.isPresent());
    //        assertEquals(SearchItemType.GROUP, subItem.orElse(new
    // UniProtSearchItem()).getItemType());
    //        assertNotNull(subItem.orElse(new UniProtSearchItem()).getItems());
    //
    //        Optional<SearchItem> subSubItem =
    //                subItem.orElse(new UniProtSearchItem()).getItems().stream()
    //                        .filter(val -> val.getLabel().equals("ChEBI term"))
    //                        .findFirst();
    //        assertTrue(subSubItem.isPresent());
    //        assertEquals(
    //                SearchDataType.STRING, subSubItem.orElse(new
    // UniProtSearchItem()).getDataType());
    //        assertEquals(
    //                SearchItemType.COMMENT, subSubItem.orElse(new
    // UniProtSearchItem()).getItemType());
    //        assertEquals(
    //                "/uniprot/api/suggester?dict=chebi&query=?",
    //                subSubItem.orElse(new UniProtSearchItem()).getAutoComplete());
    //        assertEquals("cofactor_chebi", subSubItem.orElse(new UniProtSearchItem()).getTerm());
    //        assertTrue(subSubItem.orElse(new UniProtSearchItem()).isHasEvidence());
    //    }
    //
    //    @Test
    //    void testStructureTurn() {
    //        Optional<SearchItem> item =
    //                searchItems.getSearchItems().stream()
    //                        .filter(val -> val.getLabel().equals("Structure"))
    //                        .findFirst();
    //        assertTrue(item.isPresent());
    //        assertEquals(SearchItemType.GROUP, item.orElse(new
    // UniProtSearchItem()).getItemType());
    //        assertNotNull(item.orElse(new UniProtSearchItem()).getItems());
    //        Optional<SearchItem> subItem =
    //                item.orElse(new UniProtSearchItem()).getItems().stream()
    //                        .filter(val -> val.getLabel().equals("Secondary structure"))
    //                        .findFirst();
    //        assertTrue(subItem.isPresent());
    //        assertNotNull(subItem.orElse(new UniProtSearchItem()).getItems());
    //        assertEquals(SearchItemType.GROUP, subItem.orElse(new
    // UniProtSearchItem()).getItemType());
    //
    //        Optional<SearchItem> subSubItem =
    //                subItem.orElse(new UniProtSearchItem()).getItems().stream()
    //                        .filter(val -> val.getLabel().equals("Turn"))
    //                        .findFirst();
    //        assertTrue(subSubItem.isPresent());
    //        assertEquals(
    //                SearchItemType.FEATURE, subSubItem.orElse(new
    // UniProtSearchItem()).getItemType());
    //        assertEquals(
    //                SearchDataType.STRING, subSubItem.orElse(new
    // UniProtSearchItem()).getDataType());
    //
    //        assertEquals("turn", subSubItem.orElse(new UniProtSearchItem()).getTerm());
    //        assertTrue(subSubItem.orElse(new UniProtSearchItem()).isHasRange());
    //        assertTrue(subSubItem.orElse(new UniProtSearchItem()).isHasEvidence());
    //    }
    //

    //
    //    @Test
    //    void testDateType() {
    //        Optional<SearchItem> item =
    //                searchItems.getSearchItems().stream()
    //                        .filter(val -> val.getLabel().equals("Date Of"))
    //                        .findFirst();
    //        assertTrue(item.isPresent());
    //        assertEquals(SearchItemType.GROUP, item.orElse(new
    // UniProtSearchItem()).getItemType());
    //        assertNotNull(item.orElse(new UniProtSearchItem()).getItems());
    //        Optional<SearchItem> subItem =
    //                item.orElse(new UniProtSearchItem()).getItems().stream()
    //                        .filter(val -> val.getLabel().equals("Date of last entry
    // modification"))
    //                        .findFirst();
    //        assertTrue(subItem.isPresent());
    //        assertEquals("modified", subItem.orElse(new UniProtSearchItem()).getTerm());
    //        assertEquals(SearchDataType.DATE, subItem.orElse(new
    // UniProtSearchItem()).getDataType());
    //        assertEquals(SearchItemType.SINGLE, subItem.orElse(new
    // UniProtSearchItem()).getItemType());
    //        assertTrue(subItem.orElse(new UniProtSearchItem()).isHasRange());
    //    }
    //
    //    @Test
    //    void testGroupDisplay() {
    //        Optional<SearchItem> item =
    //                searchItems.getSearchItems().stream()
    //                        .filter(val -> val.getLabel().equals("Literature Citation"))
    //                        .findFirst();
    //        assertTrue(item.isPresent());
    //        assertEquals(
    //                SearchItemType.GROUP_DISPLAY, item.orElse(new
    // UniProtSearchItem()).getItemType());
    //        assertNotNull(item.orElse(new UniProtSearchItem()).getItems());
    //        Optional<SearchItem> subItem =
    //                item.orElse(new UniProtSearchItem()).getItems().stream()
    //                        .filter(val -> val.getLabel().equals("Published"))
    //                        .findFirst();
    //        assertTrue(subItem.isPresent());
    //        assertEquals(SearchDataType.DATE, subItem.orElse(new
    // UniProtSearchItem()).getDataType());
    //        assertEquals(SearchItemType.SINGLE, subItem.orElse(new
    // UniProtSearchItem()).getItemType());
    //        assertEquals("lit_pubdate", subItem.orElse(new UniProtSearchItem()).getTerm());
    //        assertTrue(subItem.orElse(new UniProtSearchItem()).isHasRange());
    //    }
}
