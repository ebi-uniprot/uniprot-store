package uk.ac.ebi.uniprot.indexer.uniprotkb.proteome;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.solr.common.SolrInputDocument;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.solr.core.SolrTemplate;

import uk.ac.ebi.uniprot.search.SolrCollection;
import uk.ac.ebi.uniprot.xml.jaxb.proteome.ComponentType;
import uk.ac.ebi.uniprot.xml.jaxb.proteome.ProteinType;
import uk.ac.ebi.uniprot.xml.jaxb.proteome.Proteome;

/**
 *
 * @author jluo
 * @date: 2 May 2019
 *
 */

public class UniProtKBProteomeWriter implements ItemWriter<Proteome> {
	private static final String GC_SET_ACC = "GCSetAcc";
	private final SolrTemplate solrTemplate;
	private final SolrCollection collection;

	public UniProtKBProteomeWriter(SolrTemplate solrTemplate, SolrCollection collection) {
		this.solrTemplate = solrTemplate;
		this.collection = collection;
	}

	@Override
	public void write(List<? extends Proteome> items) throws Exception {
		for (Proteome item : items) {
			write(item);
		}
		this.solrTemplate.softCommit(collection.name());
	}

	private void write(Proteome item) throws Exception {
		Optional<String> genomeAssemblyId = fetchGenomeAssemblyId(item);
		List<ComponentType> components = item.getComponent();
		for (ComponentType component : components) {
			List<String> genomeAccessions = component.getGenomeAccession();
			if (genomeAccessions.isEmpty() && !genomeAssemblyId.isPresent()) {
				continue;
			}
			List<ProteinType> proteins = component.getProtein();
			proteins.forEach(protein -> addToSolr(protein, genomeAssemblyId, genomeAccessions));
		}
		this.solrTemplate.softCommit(collection.name());
	}

	private void addToSolr(ProteinType protein, Optional<String> genomeAssemblyId, List<String> genomeAccessions) {
		SolrInputDocument solrInputDocument = new SolrInputDocument();
		solrInputDocument.addField("accession_id", protein.getAccession());
		if (genomeAssemblyId.isPresent()) {
			Map<String, Object> fieldModifier = new HashMap<>(1);
			fieldModifier.put("set", genomeAssemblyId.get());
			solrInputDocument.addField("genome_assembly", fieldModifier);
		}
		if (!genomeAccessions.isEmpty()) {
			Map<String, Object> fieldModifier = new HashMap<>(1);
			fieldModifier.put("add", genomeAccessions);
			solrInputDocument.addField("genome_accession", fieldModifier);
		}
		this.solrTemplate.saveBean(collection.name(), solrInputDocument);
	}

	private Optional<String> fetchGenomeAssemblyId(Proteome source) {
		return source.getDbReference().stream().filter(val -> val.getType().equals(GC_SET_ACC)).map(val -> val.getId())
				.findFirst();
	}
}
