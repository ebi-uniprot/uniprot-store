package org.uniprot.store.indexer.uniprot.pathway;

import org.uniprot.core.cv.pathway.UniPathway;
import org.uniprot.cv.pathway.UniPathwayRepo;
import org.uniprot.cv.pathway.impl.UniPathwayRepoImpl;

public class PathwayFileRepo implements PathwayRepo {
    private final UniPathwayRepo unipathwayRepo;

    public PathwayFileRepo(String filename) {
        unipathwayRepo = new UniPathwayRepoImpl(filename);
    }

    @Override
    public UniPathway getFromName(String name) {
        return unipathwayRepo.getByName(name);
    }
}
