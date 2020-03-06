package org.uniprot.store.indexer.uniparc;

import java.nio.ByteBuffer;
import java.util.List;

import org.uniprot.core.json.parser.uniparc.UniParcJsonConfig;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniprot.taxonomy.Taxonomy;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.core.xml.uniparc.UniParcEntryConverter;
import org.uniprot.cv.taxonomy.TaxonomicNode;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.store.indexer.util.TaxonomyRepoUtil;
import org.uniprot.store.job.common.converter.DocumentConverter;
import org.uniprot.store.search.document.uniparc.UniParcDocument;
import org.uniprot.store.search.document.uniparc.UniParcDocument.UniParcDocumentBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author jluo
 * @date: 19 Jun 2019
 */
public class UniParcDocumentConverter implements DocumentConverter<Entry, UniParcDocument> {
    private final UniParcEntryConverter converter;
    private final ObjectMapper objectMapper;
    private final TaxonomyRepo taxonomyRepo;

    public UniParcDocumentConverter(TaxonomyRepo taxonomyRepo) {
        this.converter = new UniParcEntryConverter(taxonomyRepo);
        this.objectMapper = UniParcJsonConfig.getInstance().getFullObjectMapper();
        this.taxonomyRepo = taxonomyRepo;
    }

    @Override
    public UniParcDocument convert(Entry item) {
        UniParcEntry uniparcEntry = converter.fromXml(item);
        UniParcDocumentBuilder builder = UniParcDocument.builder();
        builder.upi(item.getAccession())
                .seqLength(item.getSequence().getLength())
                .sequenceChecksum(item.getSequence().getChecksum());
        uniparcEntry.getUniParcCrossReferences().forEach(val -> processDbReference(val, builder));
        builder.entryStored(ByteBuffer.wrap(getBinaryObject(uniparcEntry)));
        uniparcEntry.getTaxonomies().stream().forEach(taxon -> processTaxonomy(taxon, builder));
        return builder.build();
    }

    private void processDbReference(UniParcCrossReference xref, UniParcDocumentBuilder builder) {
        UniParcDatabase type = xref.getDatabase();
        if (xref.isActive()) {
            builder.active(type.toDisplayName());
        }
        builder.database(type.toDisplayName());
        if ((type == UniParcDatabase.SWISSPROT) || (type == UniParcDatabase.TREMBL)) {
            builder.uniprotAccession(xref.getId());
            builder.uniprotIsoform(xref.getId());
        }

        if (type == UniParcDatabase.SWISSPROT_VARSPLIC) {
            builder.uniprotIsoform(xref.getId());
        }
        xref.getProperties().stream()
                .filter(val -> val.getKey().equals(UniParcCrossReference.PROPERTY_PROTEOME_ID))
                .map(val -> val.getValue())
                .forEach(val -> builder.upid(val));

        xref.getProperties().stream()
                .filter(val -> val.getKey().equals(UniParcCrossReference.PROPERTY_PROTEIN_NAME))
                .map(val -> val.getValue())
                .forEach(val -> builder.proteinName(val));

        xref.getProperties().stream()
                .filter(val -> val.getKey().equals(UniParcCrossReference.PROPERTY_GENE_NAME))
                .map(val -> val.getValue())
                .forEach(val -> builder.geneName(val));
    }

    private void processTaxonomy(Taxonomy taxon, UniParcDocumentBuilder builder) {

        builder.taxLineageId((int) taxon.getTaxonId());
        builder.organismTaxon(taxon.getScientificName());
        List<TaxonomicNode> nodes =
                TaxonomyRepoUtil.getTaxonomyLineage(taxonomyRepo, (int) taxon.getTaxonId());
        nodes.forEach(
                node -> {
                    builder.taxLineageId(node.id());
                    List<String> names = TaxonomyRepoUtil.extractTaxonFromNode(node);
                    names.forEach(val -> builder.organismTaxon(val));
                });
    }

    private byte[] getBinaryObject(UniParcEntry uniparcEntry) {
        byte[] binaryEntry;
        try {
            binaryEntry = objectMapper.writeValueAsBytes(uniparcEntry);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse proteome to binary json: ", e);
        }
        return binaryEntry;
    }
}
