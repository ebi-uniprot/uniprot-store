package uk.ac.ebi.uniprot.indexer.uniprot.go;

import uk.ac.ebi.uniprot.common.Utils;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.*;
import static uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileRepo.Relationship.IS_A;
import static uk.ac.ebi.uniprot.indexer.uniprot.go.GoRelationFileRepo.Relationship.PART_OF;

// TODO: 04/07/19 https://www.baeldung.com/spring-boot-ehcache 
// TODO: 04/07/19 http://www.ehcache.org/documentation/3.6/expiry.html
// // TODO: 04/07/19 test this class
public final class GoRelationFileRepo implements GoRelationRepo {
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

    public Set<GoTerm> getAncestors(GoTerm fromGoTerm, List<Relationship> relationships) {
        List<Relationship> relationshipsToUse = relationships;
        if (relationships.isEmpty()) {
            relationshipsToUse = singletonList(IS_A);
        }

        Set<GoTerm> ancestorsFound = new HashSet<>();
        addAncestors(singleton(fromGoTerm), ancestorsFound, relationshipsToUse);
        return ancestorsFound;
    }

    private void addAncestors(Set<GoTerm> baseGoIds, Set<GoTerm> ancestors, List<Relationship> relationships) {
        for (GoTerm base : baseGoIds) {
            Set<GoTerm> parents = emptySet();
            if (relationships.contains(IS_A)) {
                parents = getIsA(base.getId());
            } else if (relationships.contains(PART_OF)) {
                parents = getPartOf(base.getId());
            }
            ancestors.addAll(parents);
            addAncestors(parents, ancestors, relationships);
        }
    }

    private Set<GoTerm> convert(Set<String> goIds, Map<String, GoTerm> gotermMap) {
        return goIds.stream().map(gotermMap::get).filter(Utils::nonNull).collect(Collectors.toSet());
    }

    public enum Relationship {
        IS_A, PART_OF
    }
}
