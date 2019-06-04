package uk.ac.ebi.uniprot.indexer.uniprot.go;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public final class GoRelationFileRepo implements GoRelationRepo {

    private final Map<String, List<GoTerm>> isAMap;
    private final Map<String, List<GoTerm>> isPartMap;

    public static GoRelationFileRepo create(GoRelationFileReader goRelationReader, GoTermFileReader goTermReader) {
        return new GoRelationFileRepo(goRelationReader, goTermReader);
    }

    public GoRelationFileRepo(GoRelationFileReader goRelationReader, GoTermFileReader goTermReader) {
        goRelationReader.read();

        Map<String, List<String>> isAMapStr = goRelationReader.getIsAMap();
        Map<String, List<String>> isPartMapStr = goRelationReader.getIsPartMap();
        Map<String, GoTerm> gotermMap = goTermReader.read();
        isAMap = isAMapStr.entrySet().stream()
                .collect(Collectors.toMap(val -> val.getKey(), val -> convert(val.getValue(), gotermMap)));

        isPartMap = isPartMapStr.entrySet().stream()
                .collect(Collectors.toMap(val -> val.getKey(), val -> convert(val.getValue(), gotermMap)));
    }

    private List<GoTerm> convert(List<String> goIds, Map<String, GoTerm> gotermMap) {
        return goIds.stream().map(val -> gotermMap.get(val)).filter(val -> val != null).collect(Collectors.toList());
    }

    public List<GoTerm> getIsA(String goId) {
        return isAMap.getOrDefault(goId, Collections.emptyList());
    }

    public List<GoTerm> getPartOf(String goId) {
        return isPartMap.getOrDefault(goId, Collections.emptyList());
    }

}
