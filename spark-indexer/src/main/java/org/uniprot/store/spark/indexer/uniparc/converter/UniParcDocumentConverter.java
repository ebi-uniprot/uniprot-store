package org.uniprot.store.spark.indexer.uniparc.converter;

import java.util.List;
import java.util.stream.Collectors;

import org.uniprot.core.Property;
import org.uniprot.core.uniparc.UniParcDBCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniprot.taxonomy.Taxonomy;
import org.uniprot.store.job.common.converter.DocumentConverter;
import org.uniprot.store.search.document.uniparc.UniParcDocument;

/**
 * This class convert an UniParcEntry to UniParcDocument
 *
 * @author lgonzales
 * @since 2020-02-15
 */
public class UniParcDocumentConverter implements DocumentConverter<UniParcEntry, UniParcDocument> {

    @Override
    public UniParcDocument convert(UniParcEntry uniparcEntry) {
        UniParcDocument.UniParcDocumentBuilder builder = UniParcDocument.builder();
        builder.upi(uniparcEntry.getUniParcId().getValue())
                .contentAdd(uniparcEntry.getUniParcId().getValue())
                .seqLength(uniparcEntry.getSequence().getLength())
                .sequenceChecksum(uniparcEntry.getSequence().getCrc64())
                .taxLineageIds(getTaxonomies(uniparcEntry));
        getTaxonomies(uniparcEntry).stream().map(String::valueOf).forEach(builder::contentAdd);
        uniparcEntry.getDbXReferences().forEach(val -> processDbReference(val, builder));
        return builder.build();
    }

    private List<Integer> getTaxonomies(UniParcEntry uniparcEntry) {
        return uniparcEntry.getTaxonomies().stream()
                .map(Taxonomy::getTaxonId)
                .map(Long::intValue)
                .collect(Collectors.toList());
    }

    private void processDbReference(
            UniParcDBCrossReference xref, UniParcDocument.UniParcDocumentBuilder builder) {
        UniParcDatabase type = xref.getDatabaseType();
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
                .filter(val -> val.getKey().equals(UniParcDBCrossReference.PROPERTY_PROTEOME_ID))
                .map(Property::getValue)
                .forEach(builder::upid);

        xref.getProperties().stream()
                .filter(val -> val.getKey().equals(UniParcDBCrossReference.PROPERTY_PROTEIN_NAME))
                .map(Property::getValue)
                .forEach(builder::proteinName);

        xref.getProperties().stream()
                .filter(val -> val.getKey().equals(UniParcDBCrossReference.PROPERTY_GENE_NAME))
                .map(Property::getValue)
                .forEach(builder::geneName);

        builder.contentAdd(type.toDisplayName());
        builder.contentAdd(xref.getId());
        xref.getProperties().stream().map(Property::getValue).forEach(builder::contentAdd);
    }
}
