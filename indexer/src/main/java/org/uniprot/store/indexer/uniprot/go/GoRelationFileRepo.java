package org.uniprot.store.indexer.uniprot.go;

import org.springframework.cache.annotation.Cacheable;
import org.uniprot.core.util.Utils;
import org.uniprot.store.indexer.uniprotkb.UniProtKBJob;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.*;
import static org.uniprot.store.indexer.uniprot.go.GoRelationFileRepo.Relationship.IS_A;
import static org.uniprot.store.indexer.uniprot.go.GoRelationFileRepo.Relationship.PART_OF;

public class GoRelationFileRepo implements GoRelationRepo {
    private final Map<String, Set<GoTerm>> isAMap;
    private final Map<String, Set<GoTerm>> partOfMap;

    public static GoRelationFileRepo create(GoRelationFileReader goRelationReader, GoTermFileReader goTermReader) {
        return new GoRelationFileRepo(goRelationReader, goTermReader);
    }

    public GoRelationFileRepo(GoRelationFileReader goRelationReader, GoTermFileReader goTermReader) {
        goRelationReader.read();

        Map<String, Set<String>> isAMapStr = goRelationReader.getIsAMap();
        Map<String, Set<String>> isPartMapStr = goRelationReader.getIsPartMap();
        Map<String, GoTerm> gotermMap = goTermReader.read();
        isAMap = isAMapStr.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, val -> convert(val.getValue(), gotermMap)));

        partOfMap = isPartMapStr.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, val -> convert(val.getValue(), gotermMap)));
    }

    public Set<GoTerm> getIsA(String goId) {
        return isAMap.getOrDefault(goId, Collections.emptySet());
    }

    public Set<GoTerm> getPartOf(String goId) {
        return partOfMap.getOrDefault(goId, Collections.emptySet());
    }

    @Cacheable(value = UniProtKBJob.GO_ANCESTORS_CACHE)
    public Set<GoTerm> getAncestors(String fromGoTerm, List<Relationship> relationships) {
        List<Relationship> relationshipsToUse = relationships;
        if (Utils.nullOrEmpty(relationshipsToUse)) {
            relationshipsToUse = singletonList(IS_A);
        }

        if (Utils.nonNull(fromGoTerm)) {
            Set<GoTerm> ancestorsFound = new HashSet<>();
            addAncestors(singleton(fromGoTerm), ancestorsFound, relationshipsToUse);
            return ancestorsFound;
        } else {
            return emptySet();
        }
    }

    private void addAncestors(Set<String> baseGoIds, Set<GoTerm> ancestors, List<Relationship> relationships) {
        for (String base : baseGoIds) {
            Set<GoTerm> parents = new HashSet<>();
            if (relationships.contains(IS_A)) {
                parents.addAll(getIsA(base));
            }

            if (relationships.contains(PART_OF)) {
                parents.addAll(getPartOf(base));
            }

            ancestors.addAll(parents);
            addAncestors(parents.stream()
                                 .map(GoTerm::getId)
                                 .collect(Collectors.toSet()),
                         ancestors, relationships);
        }
    }

    private Set<GoTerm> convert(Set<String> goIds, Map<String, GoTerm> gotermMap) {
        return goIds.stream().map(gotermMap::get).filter(Utils::nonNull).collect(Collectors.toSet());
    }

    public enum Relationship {
        IS_A, PART_OF
    }
}
