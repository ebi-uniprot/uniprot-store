package org.uniprot.store.indexer.converters;

import static org.uniprot.store.indexer.uniprotkb.converter.UniProtEntryConverterUtil.createSuggestionMapKey;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.uniprot.core.uniparc.SequenceFeature;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniprotkb.taxonomy.Organism;
import org.uniprot.core.util.Utils;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.core.xml.uniparc.UniParcEntryConverter;
import org.uniprot.cv.taxonomy.TaxonomicNode;
import org.uniprot.cv.taxonomy.TaxonomyRepo;
import org.uniprot.cv.taxonomy.impl.TaxonomicNodeImpl;
import org.uniprot.store.config.uniparc.UniParcConfigUtil;
import org.uniprot.store.indexer.util.TaxonomyRepoUtil;
import org.uniprot.store.search.document.DocumentConverter;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.search.document.uniparc.UniParcDocument;
import org.uniprot.store.search.document.uniparc.UniParcDocument.UniParcDocumentBuilder;

/**
 * @author jluo
 * @date: 19 Jun 2019 FIXME Ideally, we should UniParcDocumentConverter of spark-indexer module.
 *     Once we migrate spring batch job to spark, we should start using spark-indexer module'S
 *     UniParcDocumentConverter
 */
public class UniParcDocumentConverter implements DocumentConverter<Entry, UniParcDocument> {
    private final UniParcEntryConverter converter;
    private final TaxonomyRepo taxonomyRepo;
    private Map<String, SuggestDocument> suggestions;

    public UniParcDocumentConverter(
            TaxonomyRepo taxonomyRepo, Map<String, SuggestDocument> suggestions) {
        this.converter = new UniParcEntryConverter(taxonomyRepo);
        this.taxonomyRepo = taxonomyRepo;
        this.suggestions = suggestions;
    }

    @Override
    public UniParcDocument convert(Entry item) {
        UniParcEntry uniparcEntry = converter.fromXml(item);
        UniParcDocumentBuilder builder = UniParcDocument.builder();
        builder.upi(item.getAccession())
                .seqLength(item.getSequence().getLength())
                .sequenceChecksum(item.getSequence().getChecksum())
                .sequenceChecksum(uniparcEntry.getSequence().getMd5());
        uniparcEntry.getUniParcCrossReferences().forEach(val -> processDbReference(val, builder));
        uniparcEntry
                .getSequenceFeatures()
                .forEach(sequenceFeature -> processSequenceFeature(sequenceFeature, builder));

        populateSuggestions(uniparcEntry);

        return builder.build();
    }

    public Object getSuggestions() {
        return this.suggestions;
    }

    private void processDbReference(UniParcCrossReference xref, UniParcDocumentBuilder builder) {
        UniParcDatabase type = xref.getDatabase();

        String dbId = xref.getId();
        if (Objects.nonNull(xref.getVersion()) && xref.getVersion() > 0) {
            dbId += "." + xref.getVersion();
        }
        builder.dbId(dbId);

        Map.Entry<String, String> dbTypeData = UniParcConfigUtil.getDBNameValue(type);
        if (xref.isActive()) {
            builder.active(dbTypeData.getValue());
        }
        builder.database(dbTypeData.getValue());
        builder.notDuplicatedDatabasesFacet(type.getIndex());
        if (xref.isActive() && type == UniParcDatabase.SWISSPROT_VARSPLIC) {
            builder.uniprotIsoform(xref.getId());
        }

        if (xref.isActive()
                && (type == UniParcDatabase.SWISSPROT || type == UniParcDatabase.TREMBL)) {
            builder.uniprotAccession(xref.getId());
            builder.uniprotIsoform(xref.getId());
        }

        if (Utils.notNull(xref.getOrganism())) {
            processOrganism(xref.getOrganism(), builder);
        }

        if (Utils.notNullNotEmpty(xref.getProteinName())) {
            builder.proteinName(xref.getProteinName());
        }

        if (Utils.notNullNotEmpty(xref.getGeneName())) {
            builder.geneName(xref.getGeneName());
        }

        if (Utils.notNullNotEmpty(xref.getProteomeId())) {
            builder.proteome(xref.getProteomeId());
        }
        if (Utils.notNullNotEmpty(xref.getComponent())) {
            builder.proteomeComponent(xref.getComponent());
        }
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

    private void processOrganism(Organism taxon, UniParcDocumentBuilder builder) {

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
        int taxonId = Math.toIntExact(taxon.getTaxonId());
        builder.organismId(taxonId);
    }

    private void populateSuggestions(UniParcEntry uniparcEntry) {
        uniparcEntry.getUniParcCrossReferences().stream()
                .filter(xref -> Utils.notNull(xref.getOrganism()))
                .map(UniParcCrossReference::getOrganism)
                .flatMap(
                        taxon -> {
                            List<TaxonomicNode> nodes =
                                    TaxonomyRepoUtil.getTaxonomyLineage(
                                            taxonomyRepo, (int) taxon.getTaxonId());
                            if (nodes.isEmpty()) { // if taxonomy is not in db
                                TaxonomicNodeImpl.Builder builder =
                                        new TaxonomicNodeImpl.Builder(
                                                (int) taxon.getTaxonId(), null);
                                nodes.add(builder.build());
                            }
                            return nodes.stream();
                        })
                .forEach(
                        node -> {
                            String key =
                                    createSuggestionMapKey(
                                            SuggestDictionary.UNIPARC_TAXONOMY,
                                            String.valueOf(node.id()));
                            this.suggestions.putIfAbsent(key, createSuggestDoc(node));
                        });
    }

    private SuggestDocument createSuggestDoc(TaxonomicNode taxonNode) {
        SuggestDocument.SuggestDocumentBuilder builder = SuggestDocument.builder();
        builder.id(String.valueOf(taxonNode.id()))
                .dictionary(SuggestDictionary.UNIPARC_TAXONOMY.name())
                .altValues(extractAltValuesFromOrganism(taxonNode));

        if (Utils.notNullNotEmpty(taxonNode.scientificName())) {
            builder.value(taxonNode.scientificName());
        }

        return builder.build();
    }

    private static List<String> extractAltValuesFromOrganism(TaxonomicNode taxonNode) {
        List<String> altValues = new ArrayList<>();
        if (Utils.notNullNotEmpty(taxonNode.commonName())) {
            altValues.add(taxonNode.commonName());
        }
        if (Utils.notNullNotEmpty(taxonNode.synonymName())) {
            altValues.add(taxonNode.synonymName());
        }
        return altValues;
    }
}
