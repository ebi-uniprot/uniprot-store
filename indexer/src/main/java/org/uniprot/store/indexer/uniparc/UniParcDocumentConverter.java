package org.uniprot.store.indexer.uniparc;

import java.util.List;

import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.core.xml.uniparc.UniParcEntryConverter;
import org.uniprot.cv.taxonomy.TaxonomicNode;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.store.indexer.util.TaxonomyRepoUtil;
import org.uniprot.store.job.common.converter.DocumentConverter;
import org.uniprot.store.search.document.uniparc.UniParcDocument;
import org.uniprot.store.search.document.uniparc.UniParcDocument.UniParcDocumentBuilder;

/**
 * @author jluo
 * @date: 19 Jun 2019
 */
public class UniParcDocumentConverter implements DocumentConverter<Entry, UniParcDocument> {
    private final UniParcEntryConverter converter;
    private final TaxonomyRepo taxonomyRepo;

    public UniParcDocumentConverter(TaxonomyRepo taxonomyRepo) {
        this.converter = new UniParcEntryConverter(taxonomyRepo);
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
        uniparcEntry.getTaxonomies().stream().forEach(taxon -> processTaxonomy(taxon, builder));
        return builder.build();
    }

    private void processDbReference(UniParcCrossReference xref, UniParcDocumentBuilder builder) {
        UniParcDatabase type = xref.getDatabase();

        builder.dbId(xref.getId());

        if (xref.isActive()) {
            builder.active(type.getDisplayName());
        }
        builder.database(type.getDisplayName());
        if (xref.isActive()
                && (type == UniParcDatabase.SWISSPROT || type == UniParcDatabase.TREMBL)) {
            builder.uniprotAccession(xref.getId());
            builder.uniprotIsoform(xref.getId());
        }

        if (xref.isActive() && type == UniParcDatabase.SWISSPROT_VARSPLIC) {
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
}
