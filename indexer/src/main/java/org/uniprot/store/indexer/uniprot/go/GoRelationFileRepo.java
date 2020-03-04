package org.uniprot.store.indexer.uniprot.go;

import static java.util.Collections.*;
import static org.uniprot.store.indexer.uniprot.go.GoRelationFileRepo.Relationship.IS_A;
import static org.uniprot.store.indexer.uniprot.go.GoRelationFileRepo.Relationship.PART_OF;

import java.util.*;
import java.util.stream.Collectors;

import org.springframework.cache.annotation.Cacheable;
import org.uniprot.core.cv.go.GeneOntologyEntry;
import org.uniprot.core.util.Utils;
import org.uniprot.store.indexer.uniprotkb.UniProtKBJob;

public class GoRelationFileRepo implements GoRelationRepo {
    private final Map<String, Set<GeneOntologyEntry>> isAMap;
    private final Map<String, Set<GeneOntologyEntry>> partOfMap;

    public static GoRelationFileRepo create(
            GoRelationFileReader goRelationReader, GoTermFileReader goTermReader) {
        return new GoRelationFileRepo(goRelationReader, goTermReader);
    }

    public GoRelationFileRepo(
            GoRelationFileReader goRelationReader, GoTermFileReader goTermReader) {
        goRelationReader.read();

        Map<String, Set<String>> isAMapStr = goRelationReader.getIsAMap();
        Map<String, Set<String>> isPartMapStr = goRelationReader.getIsPartMap();
        Map<String, GeneOntologyEntry> gotermMap = goTermReader.read();
        isAMap =
                isAMapStr.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        val -> convert(val.getValue(), gotermMap)));

        partOfMap =
                isPartMapStr.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        val -> convert(val.getValue(), gotermMap)));
    }

    public Set<GeneOntologyEntry> getIsA(String goId) {
        return isAMap.getOrDefault(goId, Collections.emptySet());
    }

    public Set<GeneOntologyEntry> getPartOf(String goId) {
        return partOfMap.getOrDefault(goId, Collections.emptySet());
    }

    @Cacheable(value = UniProtKBJob.GO_ANCESTORS_CACHE)
    public Set<GeneOntologyEntry> getAncestors(
            String fromGoTerm, List<Relationship> relationships) {
        List<Relationship> relationshipsToUse = relationships;
        if (Utils.nullOrEmpty(relationshipsToUse)) {
            relationshipsToUse = singletonList(IS_A);
        }

        if (Utils.notNull(fromGoTerm)) {
            Set<GeneOntologyEntry> ancestorsFound = new HashSet<>();
            addAncestors(singleton(fromGoTerm), ancestorsFound, relationshipsToUse);
            return ancestorsFound;
        } else {
            return emptySet();
        }
    }

    private void addAncestors(
            Set<String> baseGoIds,
            Set<GeneOntologyEntry> ancestors,
            List<Relationship> relationships) {
        for (String base : baseGoIds) {
            Set<GeneOntologyEntry> parents = new HashSet<>();
            if (relationships.contains(IS_A)) {
                parents.addAll(getIsA(base));
            }

            if (relationships.contains(PART_OF)) {
                parents.addAll(getPartOf(base));
            }

            ancestors.addAll(parents);
            addAncestors(
                    parents.stream().map(GeneOntologyEntry::getId).collect(Collectors.toSet()),
                    ancestors,
                    relationships);
        }
    }

    private Set<GeneOntologyEntry> convert(
            Set<String> goIds, Map<String, GeneOntologyEntry> gotermMap) {
        return goIds.stream()
                .map(gotermMap::get)
                .filter(Utils::notNull)
                .collect(Collectors.toSet());
    }

    public enum Relationship {
        IS_A,
        PART_OF
    }
}
