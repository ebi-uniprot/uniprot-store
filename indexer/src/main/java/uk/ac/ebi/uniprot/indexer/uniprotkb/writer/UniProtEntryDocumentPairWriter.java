package uk.ac.ebi.uniprot.indexer.uniprotkb.writer;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.impl.UniProtFlatfileWriter;
import uk.ac.ebi.uniprot.indexer.common.config.UniProtSolrOperations;
import uk.ac.ebi.uniprot.indexer.common.writer.EntryDocumentPairRetryWriter;
import uk.ac.ebi.uniprot.indexer.uniprotkb.model.UniProtEntryDocumentPair;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;

/**
 * Created 12/04/19
 *
 * @author Edd
 */
@Slf4j
public class UniProtEntryDocumentPairWriter extends EntryDocumentPairRetryWriter<UniProtEntry, UniProtDocument, UniProtEntryDocumentPair> {
    public UniProtEntryDocumentPairWriter(UniProtSolrOperations solrOperations, SolrCollection collection, RetryPolicy<Object> retryPolicy) {
        super(solrOperations, collection, retryPolicy);
    }

    @Override
    public String extractDocumentId(UniProtDocument document) {
        return document.accession;
    }

    @Override
    public String entryToString(UniProtEntry entry) {
        return UniProtFlatfileWriter.write(entry);
    }
}
