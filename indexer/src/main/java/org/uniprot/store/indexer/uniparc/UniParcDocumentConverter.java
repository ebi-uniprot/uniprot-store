package org.uniprot.store.indexer.uniparc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.uniprot.core.cv.taxonomy.TaxonomicNode;
import org.uniprot.core.cv.taxonomy.TaxonomyRepo;
import org.uniprot.core.json.parser.uniparc.UniParcJsonConfig;
import org.uniprot.core.uniparc.UniParcDBCrossReference;
import org.uniprot.core.uniparc.UniParcDatabaseType;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniprot.taxonomy.Taxonomy;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.core.xml.uniparc.UniParcEntryConverter;
import org.uniprot.store.indexer.converter.DocumentConverter;
import org.uniprot.store.search.document.uniparc.UniParcDocument;
import org.uniprot.store.search.document.uniparc.UniParcDocument.UniParcDocumentBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 *
 * @author jluo
 * @date: 19 Jun 2019
 *
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
		builder.upi(item.getAccession()).seqLength(item.getSequence().getLength())
				.sequenceChecksum(item.getSequence().getChecksum());
		uniparcEntry.getDbXReferences().forEach(val -> processDbReference(val, builder));
		builder.entryStored(ByteBuffer.wrap(getBinaryObject(uniparcEntry)));
		uniparcEntry.getTaxonomies().stream().forEach(taxon -> processTaxonomy(taxon, builder));
		return builder.build();
	}
	private void processDbReference(UniParcDBCrossReference xref, UniParcDocumentBuilder builder) {
		UniParcDatabaseType type = xref.getDatabaseType();
		if (xref.isActive()) {
			builder.active(type.toDisplayName());
		}
		builder.database(type.toDisplayName());
		if ((type == UniParcDatabaseType.SWISSPROT) || (type == UniParcDatabaseType.TREMBL)) {
			builder.uniprotAccession(xref.getId());
			builder.uniprotIsoform(xref.getId());
		}

		if (type == UniParcDatabaseType.SWISSPROT_VARSPLIC) {
			builder.uniprotIsoform(xref.getId());
		}
		xref.getProperties().stream().filter(val -> val.getKey().equals(UniParcDBCrossReference.PROPERTY_PROTEOME_ID))
				.map(val -> val.getValue()).forEach(val -> builder.upid(val));

		xref.getProperties().stream().filter(val -> val.getKey().equals(UniParcDBCrossReference.PROPERTY_PROTEIN_NAME))
				.map(val -> val.getValue()).forEach(val -> builder.proteinName(val));

		xref.getProperties().stream().filter(val -> val.getKey().equals(UniParcDBCrossReference.PROPERTY_GENE_NAME))
				.map(val -> val.getValue()).forEach(val -> builder.geneName(val));

	}

	private void processTaxonomy(Taxonomy taxon, UniParcDocumentBuilder builder) {
		builder.taxLineageId((int) taxon.getTaxonId());
		builder.organismTaxon(taxon.getScientificName());
		if (taxon.hasCommonName()) {
			builder.organismTaxon(taxon.getCommonName());
		}
		if (taxon.hasMnemonic()) {
			builder.organismTaxon(taxon.getMnemonic());
		}
		if (taxon.hasSynonyms()) {
			builder.organismTaxons(taxon.getSynonyms());
		}
		setLineageTaxon((int) taxon.getTaxonId(), builder);
	}

	private void setLineageTaxon(int taxId, UniParcDocumentBuilder builder) {
		if (taxId > 0) {
			Optional<TaxonomicNode> taxonomicNode = taxonomyRepo.retrieveNodeUsingTaxID(taxId);

			while (taxonomicNode.isPresent()) {
				TaxonomicNode node = taxonomicNode.get();
				builder.taxLineageId(node.id());
				builder.organismTaxons(extractTaxonode(node));
				taxonomicNode = getParentTaxon(node.id());
			}
		}
	}

	private Optional<TaxonomicNode> getParentTaxon(int taxId) {
		Optional<TaxonomicNode> optionalNode = taxonomyRepo.retrieveNodeUsingTaxID(taxId);
		return optionalNode.filter(TaxonomicNode::hasParent).map(TaxonomicNode::parent);
	}

	private List<String> extractTaxonode(TaxonomicNode node) {
		List<String> taxonmyItems = new ArrayList<>();
		if (node.scientificName() != null && !node.scientificName().isEmpty()) {
			taxonmyItems.add(node.scientificName());
		}
		if (node.commonName() != null && !node.commonName().isEmpty()) {
			taxonmyItems.add(node.commonName());
		}
		if (node.synonymName() != null && !node.synonymName().isEmpty()) {
			taxonmyItems.add(node.synonymName());
		}
		if (node.mnemonic() != null && !node.mnemonic().isEmpty()) {
			taxonmyItems.add(node.mnemonic());
		}
		return taxonmyItems;
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

