package org.uniprot.store.indexer.uniprot.go;

import java.util.List;
import java.util.Set;

public interface GoRelationRepo {
    Set<GoTerm> getIsA(String goId);

    Set<GoTerm> getPartOf(String goId);

    Set<GoTerm> getAncestors(String goId, List<GoRelationFileRepo.Relationship> relationships);
}
