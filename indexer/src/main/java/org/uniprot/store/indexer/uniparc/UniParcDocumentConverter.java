package org.uniprot.store.indexer.uniparc;

import java.util.List;

import org.uniprot.core.Property;
import org.uniprot.core.uniparc.SequenceFeature;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.util.Utils;
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
                .sequenceChecksum(item.getSequence().getChecksum())
                .sequenceMd5(uniparcEntry.getSequence().getMd5());
        uniparcEntry.getUniParcCrossReferences().forEach(val -> processDbReference(val, builder));
        uniparcEntry.getTaxonomies().forEach(taxon -> processTaxonomy(taxon, builder));
        uniparcEntry
                .getSequenceFeatures()
                .forEach(sequenceFeature -> processSequenceFeature(sequenceFeature, builder));
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
                .map(Property::getValue)
                .forEach(builder::upid);

        xref.getProperties().stream()
                .filter(val -> val.getKey().equals(UniParcCrossReference.PROPERTY_PROTEIN_NAME))
                .map(Property::getValue)
                .forEach(builder::proteinName);

        xref.getProperties().stream()
                .filter(val -> val.getKey().equals(UniParcCrossReference.PROPERTY_GENE_NAME))
                .map(Property::getValue)
                .forEach(builder::geneName);
    }

    private void processSequenceFeature(
            SequenceFeature sequenceFeature, UniParcDocument.UniParcDocumentBuilder builder) {
        if (Utils.notNull(sequenceFeature.getSignatureDbId())) {
            builder.featureId(sequenceFeature.getSignatureDbId());
        }
        if (Utils.notNull(sequenceFeature.getInterProDomain())) {
            builder.featureId(sequenceFeature.getInterProDomain().getId());
        }
    }

    private void processTaxonomy(Taxonomy taxon, UniParcDocumentBuilder builder) {

        builder.taxLineageId((int) taxon.getTaxonId());
        builder.organismTaxon(taxon.getScientificName());
        builder.organismName(taxon.getScientificName());
        List<TaxonomicNode> nodes =
                TaxonomyRepoUtil.getTaxonomyLineage(taxonomyRepo, (int) taxon.getTaxonId());
        nodes.forEach(
                node -> {
                    builder.taxLineageId(node.id());
                    List<String> names = TaxonomyRepoUtil.extractTaxonFromNode(node);
                    names.forEach(builder::organismTaxon);
                });
    }
}
