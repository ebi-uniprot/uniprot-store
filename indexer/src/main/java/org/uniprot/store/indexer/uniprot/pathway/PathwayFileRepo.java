package org.uniprot.store.indexer.uniprot.pathway;

import org.uniprot.core.cv.pathway.UniPathway;
import org.uniprot.cv.pathway.UniPathwayService;
import org.uniprot.cv.pathway.impl.UniPathwayServiceImpl;

public class PathwayFileRepo implements PathwayRepo {
    private final UniPathwayService unipathwayService;

    public PathwayFileRepo(String filename) {
        unipathwayService = new UniPathwayServiceImpl(filename);
    }

    @Override
    public UniPathway getFromName(String name) {
        return unipathwayService.getByName(name);
    }
}
