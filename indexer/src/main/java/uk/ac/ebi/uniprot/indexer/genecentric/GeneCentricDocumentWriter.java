package uk.ac.ebi.uniprot.indexer.genecentric;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.ItemWriter;
import uk.ac.ebi.uniprot.domain.proteome.CanonicalProtein;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntryType;
import uk.ac.ebi.uniprot.indexer.common.config.UniProtSolrOperations;
import uk.ac.ebi.uniprot.json.parser.proteome.ProteomeJsonConfig;
import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.search.document.proteome.GeneCentricDocument;
import uk.ac.ebi.uniprot.search.document.proteome.GeneCentricDocument.GeneCentricDocumentBuilder;
import uk.ac.ebi.uniprot.xml.jaxb.proteome.Proteome;
import uk.ac.ebi.uniprot.xml.proteome.ProteomeConverter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author jluo
 * @date: 16 May 2019
 */

public class GeneCentricDocumentWriter implements ItemWriter<Proteome> {
    private final UniProtSolrOperations solrOperations;
    private final SolrCollection collection;
    private final ProteomeConverter proteomeConverter;
    private final ObjectMapper objectMapper;

    public GeneCentricDocumentWriter(UniProtSolrOperations solrOperations) {
        this.solrOperations = solrOperations;
        this.collection = SolrCollection.genecentric;
        this.proteomeConverter = new ProteomeConverter();
        this.objectMapper = ProteomeJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public void write(List<? extends Proteome> items) throws Exception {
        for (Proteome proteome : items) {
            List<CanonicalProtein> results = convert(proteome);
            List<GeneCentricDocument> documents = results.stream()
                    .map(val -> convert(val, proteome.getUpid(), proteome.getTaxonomy().intValue()))
                    .collect(Collectors.toList());
            if (!documents.isEmpty())
                this.solrOperations.saveBeans(collection.name(), documents);
        }
        this.solrOperations.softCommit(collection.name());

    }

    private GeneCentricDocument convert(CanonicalProtein protein, String upid, int taxid) {
        GeneCentricDocumentBuilder builder = GeneCentricDocument.builder();
        List<String> accessions = new ArrayList<>();
        accessions.add(protein.getCanonicalProtein().getAccession().getValue());
        protein.getRelatedProteins().stream().map(val -> val.getAccession().getValue())
                .forEach(val -> accessions.add(val));
        List<String> genes = new ArrayList<>();
        genes.add(protein.getCanonicalProtein().getGeneName());
        protein.getRelatedProteins().stream().map(val -> val.getGeneName()).forEach(val -> genes.add(val));

        builder.accession(protein.getCanonicalProtein().getAccession().getValue()).accessions(accessions)
                .geneNames(genes).reviewed(protein.getCanonicalProtein().getEntryType() == UniProtEntryType.SWISSPROT)
                .upid(upid).organismTaxId(taxid);
        byte[] binaryEntry;
        try {
            binaryEntry = objectMapper.writeValueAsBytes(protein);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse proteome to binary json: ", e);
        }
        builder.geneCentricStored(ByteBuffer.wrap(binaryEntry));

        return builder.build();
    }

    private List<CanonicalProtein> convert(Proteome proteome) {
        return this.proteomeConverter.fromXml(proteome).getCanonicalProteins();
    }

}
