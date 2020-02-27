package org.uniprot.store.indexer.uniprot.go;

import java.util.List;
import java.util.Set;

import org.uniprot.core.cv.go.GeneOntologyEntry;

public interface GoRelationRepo {
    Set<GeneOntologyEntry> getIsA(String goId);

    Set<GeneOntologyEntry> getPartOf(String goId);

    Set<GeneOntologyEntry> getAncestors(
            String goId, List<GoRelationFileRepo.Relationship> relationships);
}
