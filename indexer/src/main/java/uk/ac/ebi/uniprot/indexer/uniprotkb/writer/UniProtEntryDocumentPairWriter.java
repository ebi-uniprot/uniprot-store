package uk.ac.ebi.uniprot.indexer.uniprotkb.writer;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;
import org.springframework.data.solr.core.SolrTemplate;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.impl.UniProtFlatfileWriter;
import uk.ac.ebi.uniprot.indexer.common.writer.EntryDocumentPairWriter;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.uniprot.UniProtDocument;



/**
 * Created 12/04/19
 *
 * @author Edd
 */
@Slf4j
public class UniProtEntryDocumentPairWriter extends EntryDocumentPairWriter<UniProtEntry, UniProtDocument> {
    public UniProtEntryDocumentPairWriter(SolrTemplate solrTemplate, SolrCollection collection, RetryPolicy<Object> retryPolicy) {
        super(solrTemplate, collection, retryPolicy);
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
