package org.uniprot.store.indexer.disease;

import java.io.IOException;
import java.util.Iterator;

import org.springframework.batch.item.ItemReader;
import org.uniprot.core.cv.disease.DiseaseEntry;
import org.uniprot.cv.disease.DiseaseFileReader;

public class DiseaseItemReader implements ItemReader<DiseaseEntry> {
    private Iterator<DiseaseEntry> diseaseIterator;

    public DiseaseItemReader(String filePath) throws IOException {
        DiseaseFileReader diseaseFileReader = new DiseaseFileReader();
        this.diseaseIterator = diseaseFileReader.getDiseaseIterator(filePath);
    }

    @Override
    public DiseaseEntry read() {

        if (this.diseaseIterator.hasNext()) {
            return this.diseaseIterator.next();
        }

        return null;
    }
}
