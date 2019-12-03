package org.uniprot.store.indexer.uniprot.go;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.uniprot.store.indexer.uniprot.go.GoRelationFileRepo.Relationship.IS_A;
import static org.uniprot.store.indexer.uniprot.go.GoRelationFileRepo.Relationship.PART_OF;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Created 05/07/19
 *
 * @author Edd
 */
class GoRelationFileRepoTest {
    private GoRelationFileReader relationsFileReader;
    private GoTermFileReader termsFileReader;
    private GoRelationFileRepo repo;

    @BeforeEach
    void beforeEach() {
        this.relationsFileReader = mock(GoRelationFileReader.class);
        addRelations();
        this.termsFileReader = mock(GoTermFileReader.class);
        addTerms();

        this.repo = new GoRelationFileRepo(relationsFileReader, termsFileReader);
    }

    @Test
    void canCreateRepo() {
        assertThat(repo, is(notNullValue()));
    }

    @Test
    void canGetIsAs() {
        assertThat(extractIds(repo.getIsA("1")), contains("2", "3"));
    }

    @Test
    void noIsAsReturnsEmpty() {
        assertThat(extractIds(repo.getIsA("7")), is(empty()));
    }

    @Test
    void canGetPartOfs() {
        assertThat(extractIds(repo.getPartOf("1")), containsInAnyOrder("4"));
    }

    @Test
    void noPartOfsReturnsEmpty() {
        assertThat(extractIds(repo.getIsA("7")), is(empty()));
    }

    @Test
    void ancestorsOfNullGivesEmptySet() {
        assertThat(repo.getAncestors(null, singletonList(IS_A)), is(empty()));
    }

    @Test
    void ancestorsWhenNoRelationsGivenUsesDefaultIsA() {
        assertThat(
                extractIds(repo.getAncestors("1", null)),
                containsInAnyOrder("2", "3", "5", "6", "7"));
    }

    @Test
    void findsIsAAncestors() {
        assertThat(
                extractIds(repo.getAncestors("1", singletonList(IS_A))),
                containsInAnyOrder("2", "3", "5", "6", "7"));
    }

    @Test
    void findsPartOfAncestors() {
        assertThat(
                extractIds(repo.getAncestors("1", singletonList(PART_OF))),
                containsInAnyOrder("4", "6", "7"));
    }

    @Test
    void findsAllAncestors() {
        assertThat(
                extractIds(repo.getAncestors("1", asList(IS_A, PART_OF))),
                containsInAnyOrder("2", "3", "4", "5", "6", "7"));
    }

    @SuppressWarnings("unchecked")
    private static <T> Set<T> asSet(T... items) {
        return new HashSet<>(asList(items));
    }

    private void addTerms() {
        Map<String, GoTerm> termMap = new HashMap<>();
        termMap.put("1", go("1", "name 1"));
        termMap.put("2", go("2", "name 2"));
        termMap.put("3", go("3", "name 3"));
        termMap.put("4", go("4", "name 4"));
        termMap.put("5", go("5", "name 5"));
        termMap.put("6", go("6", "name 6"));
        termMap.put("7", go("7", "name 7"));

        when(termsFileReader.read()).thenReturn(termMap);
    }

    private GoTerm go(String id, String name) {
        return new GoTermFileReader.GoTermImpl(id, name);
    }

    private void addRelations() {
        Map<String, Set<String>> isAMap = new HashMap<>();
        isAMap.put("1", asSet("2", "3"));
        isAMap.put("2", asSet("5"));
        isAMap.put("3", asSet("6"));
        isAMap.put("5", asSet("7"));
        when(relationsFileReader.getIsAMap()).thenReturn(isAMap);

        Map<String, Set<String>> partOfMap = new HashMap<>();
        partOfMap.put("1", asSet("4"));
        partOfMap.put("2", asSet("6"));
        partOfMap.put("4", asSet("6"));
        partOfMap.put("6", asSet("7"));
        when(relationsFileReader.getIsPartMap()).thenReturn(partOfMap);
    }

    private Set<String> extractIds(Set<GoTerm> terms) {
        return terms.stream().map(GoTerm::getId).collect(Collectors.toSet());
    }
}
