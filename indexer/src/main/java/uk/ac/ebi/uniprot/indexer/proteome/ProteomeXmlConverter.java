package uk.ac.ebi.uniprot.indexer.proteome;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;

import uk.ac.ebi.uniprot.domain.proteome.builder.ProteomeBuilder;
import uk.ac.ebi.uniprot.domain.taxonomy.Taxonomy;
import uk.ac.ebi.uniprot.domain.taxonomy.builder.TaxonomyBuilder;
import uk.ac.ebi.uniprot.indexer.document.AbstractDocumentConverter;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomicNode;
import uk.ac.ebi.uniprot.indexer.uniprot.taxonomy.TaxonomyRepo;
import uk.ac.ebi.uniprot.xml.jaxb.proteome.GeneType;
import uk.ac.ebi.uniprot.xml.jaxb.proteome.Proteome;
import uk.ac.ebi.uniprot.json.parser.proteome.ProteomeJsonConfig;
import uk.ac.ebi.uniprot.search.document.proteome.ProteomeDocument;
import uk.ac.ebi.uniprot.xml.proteome.ProteomeConverter;

public class ProteomeXmlConverter extends AbstractDocumentConverter<Proteome, ProteomeDocument> {
	private static final String GC_SET_ACC = "GCSetAcc";
	// private final AvroProteomeConverter avroConverter = new
	// AvroProteomeConverter();
	private final TaxonomyRepo taxonomyRepo;
	private final ProteomeConverter proteomeConverter;
	private final ObjectMapper objectMapper;
	public ProteomeXmlConverter(TaxonomyRepo taxonomyRepo) {
		this.taxonomyRepo = taxonomyRepo;
		proteomeConverter = new ProteomeConverter();
		this.objectMapper = ProteomeJsonConfig.getInstance().getObjectMapper();
	}

	@Override
	public List<ProteomeDocument> convert(Proteome source) {
		List<ProteomeDocument> documents = new ArrayList<>();
		ProteomeDocument document = new ProteomeDocument();
		document.upid = source.getUpid();
		setOrganism(source.getTaxonomy().intValue(), document);
		setLineageTaxon(source.getTaxonomy().intValue(), document);
		document.isReferenceProteome = source.isIsReferenceProteome() || source.isIsRepresentativeProteome();
		document.isRedundant = (source.getRedundantTo() != null) && (!source.getRedundantTo().isEmpty());
		document.genomeAccession = fetchGenomeAccessions(source);
		document.superkingdom = source.getSuperregnum().name();
		document.genomeAssembly =fetchGenomeAssemblyId(source);
		document.accession = fetchGeneAccessions(source);
		document.gene = fetchGeneNames(source);
		document.content.add(document.upid);
		document.content.add(source.getDescription());
		document.content.addAll(document.organismTaxon);
		document.taxLineageIds.forEach(val -> document.content.add(val.toString()));
	
		document.proteomeStored = getBinaryObject(source);
		documents.add(document);
		return documents;
	}
	
	

	private void setOrganism(int taxonomyId, ProteomeDocument document) {
		document.organismTaxId = taxonomyId;
		Optional<TaxonomicNode> taxonomicNode = taxonomyRepo.retrieveNodeUsingTaxID(taxonomyId);
		if (taxonomicNode.isPresent()) {
			TaxonomicNode node = taxonomicNode.get();
			List<String> extractedTaxoNode = extractTaxonode(node);
			document.organismName.addAll(extractedTaxoNode);
		}

	}
	 private void setLineageTaxon(int taxId, ProteomeDocument document) {
	        if (taxId > 0) {
	            Optional<TaxonomicNode> taxonomicNode = taxonomyRepo.retrieveNodeUsingTaxID(taxId);

	            while (taxonomicNode.isPresent()) {
	                TaxonomicNode node = taxonomicNode.get();
	                document.taxLineageIds.add(node.id());
	                document.organismTaxon.addAll(extractTaxonode(node));
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



	private List<String> fetchGeneAccessions(Proteome source) {
		Set<String> accessions = source.getCanonicalGene().stream().flatMap(geneType -> {
			Set<String> accessionSet = geneType.getRelatedGene().stream().map(GeneType::getAccession)
					.collect(Collectors.toSet());
			accessionSet.add(geneType.getGene().getAccession());
			return accessionSet.stream();
		}).collect(Collectors.toSet());
		return new ArrayList<>(accessions);
	}

	private List<String> fetchGeneNames(Proteome source) {
		Set<String> geneNames = source.getCanonicalGene().stream().flatMap(geneType -> {
			Set<String> accessionSet = geneType.getRelatedGene().stream().map(GeneType::getGeneName)
					.collect(Collectors.toSet());
			accessionSet.add(geneType.getGene().getGeneName());
			return accessionSet.stream();
		}).collect(Collectors.toSet());
		return new ArrayList<>(geneNames);
	}

	private List<String> fetchGenomeAssemblyId(Proteome source) {
		return source.getDbReference().stream()
				.filter(val -> val.getType().equals(GC_SET_ACC))				
				.map(val ->val.getId())
				.collect(Collectors.toList());
	}
	private byte[] getBinaryObject(Proteome source) {
		uk.ac.ebi.uniprot.domain.proteome.Proteome proteome = this.proteomeConverter.fromXml(source);
		
		
		ProteomeBuilder builder = ProteomeBuilder.newInstance().from(proteome);
				Optional<TaxonomicNode> taxonomicNode = taxonomyRepo.retrieveNodeUsingTaxID((int)proteome.getTaxonomy().getTaxonId());
		if(taxonomicNode.isPresent()) {
			builder.taxonomy(getTaxonomy(taxonomicNode.get(), proteome.getTaxonomy().getTaxonId()));
			builder.taxonLineage(getLineage(taxonomicNode.get().id()));
		}
		uk.ac.ebi.uniprot.domain.proteome.Proteome modifiedProteome = builder.build();
		byte[] binaryEntry;
        try {
            binaryEntry = objectMapper.writeValueAsBytes(modifiedProteome);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to parse proteome to binary json: ",e); 
        }
		return binaryEntry;
	}
	private Taxonomy getTaxonomy(TaxonomicNode node, long taxId) {

			TaxonomyBuilder builder = TaxonomyBuilder.newInstance();
			builder.taxonId(taxId).scientificName(node.scientificName());
			if (!Strings.isNullOrEmpty(node.commonName()))
				builder.commonName(node.commonName());
			if (!Strings.isNullOrEmpty(node.mnemonic()))
				builder.mnemonic(node.mnemonic());
			if (!Strings.isNullOrEmpty(node.synonymName())) {
				builder.addSynonyms(node.synonymName());
			}
			return builder.build();
		
	}

	private List<String> getLineage(int taxId){
		List<String> lineage = new ArrayList<>();
		 Optional<TaxonomicNode> taxonomicNode = getParentTaxon(taxId);

         while (taxonomicNode.isPresent()) {
             TaxonomicNode node = taxonomicNode.get();
             lineage.add(node.scientificName());
             taxonomicNode = getParentTaxon(node.id());
         }
         return lineage;
	}
	private List<String> fetchGenomeAccessions(Proteome source) {
		return source.getComponent().stream().map(val -> val.getGenomeAccession()).flatMap(val -> val.stream())
				.collect(Collectors.toList());
	}
}
