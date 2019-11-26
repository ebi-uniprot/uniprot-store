package org.uniprot.store.indexer.uniprot.mockers;

import org.uniprot.core.cv.taxonomy.FileNodeIterable;
import org.uniprot.core.cv.taxonomy.TaxonomyRepo;
import org.uniprot.core.cv.taxonomy.impl.TaxonomyMapRepo;

import java.io.File;

public class TaxonomyRepoMocker {

    public static TaxonomyRepo getTaxonomyRepo() {
        String filePath =
                Thread.currentThread()
                        .getContextClassLoader()
                        .getResource("taxonomy/taxonomy.dat")
                        .getFile();
        File taxonomicFile = new File(filePath);

        FileNodeIterable taxonomicNodeIterable = new FileNodeIterable(taxonomicFile);
        return new TaxonomyMapRepo(taxonomicNodeIterable);
    }
}
