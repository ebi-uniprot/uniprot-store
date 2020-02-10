package org.uniprot.store.indexer.uniprot.mockers;

import java.io.File;

import org.uniprot.cv.taxonomy.FileNodeIterable;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.cv.taxonomy.impl.TaxonomyMapRepo;

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
