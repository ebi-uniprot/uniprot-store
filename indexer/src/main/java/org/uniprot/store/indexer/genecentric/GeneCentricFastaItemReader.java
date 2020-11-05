package org.uniprot.store.indexer.genecentric;

import org.springframework.batch.item.*;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.core.io.Resource;
import org.uniprot.core.fasta.UniProtKBFasta;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.genecentric.impl.GeneCentricEntryBuilder;
import org.uniprot.core.genecentric.impl.ProteinBuilder;
import org.uniprot.core.parser.fasta.uniprot.UniProtKBFastaParser;
import org.uniprot.store.indexer.common.config.PeekableResourceAwareItemReader;

/**
 * @author lgonzales
 * @since 03/11/2020
 */
public class GeneCentricFastaItemReader
        implements ItemReader<GeneCentricEntry>,
                ResourceAwareItemReaderItemStream<GeneCentricEntry> {

    private PeekableResourceAwareItemReader<String> delegate;

    @Override
    public GeneCentricEntry read() throws Exception {
        String fastaEntry = "";
        for (String nextLine; (nextLine = delegate.peek()) != null; ) {
            if (nextLine.startsWith(">") && !fastaEntry.isEmpty()) {
                return buildGeneCentricEntry(fastaEntry);
            } else {
                fastaEntry += this.delegate.read() + "\n";
            }
        }
        if (!fastaEntry.isEmpty()) {
            return buildGeneCentricEntry(fastaEntry);
        }
        return null;
    }

    public void setDelegate(PeekableResourceAwareItemReader<String> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void setResource(Resource resource) {
        delegate.setResource(resource);
    }

    @Override
    public void open(ExecutionContext executionContext) {
        delegate.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) {
        delegate.update(executionContext);
    }

    @Override
    public void close() {
        delegate.close();
    }

    private GeneCentricEntry buildGeneCentricEntry(String fastaEntry) {
        String fileName = this.delegate.getResourceFileName();
        String upId = fileName.substring(0, fileName.indexOf("_"));
        UniProtKBFasta fasta = UniProtKBFastaParser.fromFasta(fastaEntry);
        return new GeneCentricEntryBuilder()
                .proteomeId(upId)
                .canonicalProtein(ProteinBuilder.from(fasta).build())
                .build();
    }
}
