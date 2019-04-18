package uk.ac.ebi.uniprot.datastore.uniprot;

import java.util.List;

import org.springframework.batch.item.ItemWriter;

import uk.ac.ebi.uniprot.datastore.voldemort.VoldemortClient;
import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;
import uk.ac.ebi.uniprot.domain.uniprot.builder.UniProtEntryBuilder;
import uk.ebi.uniprot.scorer.uniprotkb.UniProtEntryScored;

/**
 *
 * @author jluo
 * @date: 18 Apr 2019
 *
*/

public class UniProtEntryWriter implements ItemWriter<UniProtEntry> {
	private final VoldemortClient<UniProtEntry> client;
	public UniProtEntryWriter(VoldemortClient<UniProtEntry> client) {
		this.client = client;
	}
	@Override
	public void write(List<? extends UniProtEntry> entries) throws Exception {
		entries.stream().map(this::addAnnotationScore).forEach(entry -> client.saveEntry(entry));		
	}
	
	private UniProtEntry addAnnotationScore(UniProtEntry entry) {
		 UniProtEntryScored entryScored = new UniProtEntryScored(entry);
         double score = entryScored.score();
         UniProtEntryBuilder.ActiveEntryBuilder builder = new UniProtEntryBuilder().from(entry);
         builder.annotationScore(score);
         return  builder.build();
	}
}

