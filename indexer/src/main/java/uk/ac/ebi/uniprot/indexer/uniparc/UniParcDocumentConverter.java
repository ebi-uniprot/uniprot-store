package uk.ac.ebi.uniprot.indexer.uniparc;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomicNode;
import uk.ac.ebi.uniprot.cv.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.domain.uniparc.UniParcDBCrossReference;
import uk.ac.ebi.uniprot.domain.uniparc.UniParcDatabaseType;
import uk.ac.ebi.uniprot.domain.uniparc.UniParcEntry;
import uk.ac.ebi.uniprot.domain.uniprot.taxonomy.Taxonomy;
import uk.ac.ebi.uniprot.indexer.converter.DocumentConverter;
import uk.ac.ebi.uniprot.json.parser.uniparc.UniParcJsonConfig;
import uk.ac.ebi.uniprot.search.document.uniparc.UniParcDocument;
import uk.ac.ebi.uniprot.search.document.uniparc.UniParcDocument.UniParcDocumentBuilder;
import uk.ac.ebi.uniprot.xml.jaxb.uniparc.Entry;
import uk.ac.ebi.uniprot.xml.uniparc.UniParcEntryConverter;

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

