package org.uniprot.store.indexer.uniprot.mockers;

import org.uniprot.store.indexer.uniprot.pathway.PathwayFileRepo;
import org.uniprot.store.indexer.uniprot.pathway.PathwayRepo;

public class PathwayRepoMocker {
    public static PathwayRepo getPathwayRepo() {
        return new PathwayFileRepo("unipathway.txt");
    }
}
