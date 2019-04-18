package uk.ac.ebi.uniprot.datastore.uniprot;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import uk.ac.ebi.uniprot.domain.uniprot.UniProtEntry;

/**
 *
 * @author jluo
 * @date: 18 Apr 2019
 *
*/

public class UniProtEntryReader implements ItemReader<UniProtEntry> {

	@Override
	public UniProtEntry read()
			throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		// TODO Auto-generated method stub
		return null;
	}

}

