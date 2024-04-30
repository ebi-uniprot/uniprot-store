package org.uniprot.store.indexer.literature.reader;

import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.store.reader.literature.LiteratureConverter;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lgonzales
 */
@Slf4j
public class LiteratureLineMapper extends DefaultLineMapper<LiteratureEntry> {

    @Override
    public LiteratureEntry mapLine(String entryString, int lineNumber) throws Exception {
        LiteratureConverter converter = new LiteratureConverter();
        Literature literature = converter.convert(entryString);
        return new LiteratureEntryBuilder().citation(literature).build();
    }
}
