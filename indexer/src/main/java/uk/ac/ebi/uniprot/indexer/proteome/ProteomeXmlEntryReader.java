package uk.ac.ebi.uniprot.indexer.proteome;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import uk.ac.ebi.uniprot.xml.XmlChainIterator;
import uk.ac.ebi.uniprot.xml.jaxb.proteome.Proteome;
/**
 * 
 * @author jluo
 *
 */
public class ProteomeXmlEntryReader implements ItemReader<Proteome> {
	public static final String PROTEOME_ROOT_ELEMENT = "proteome";
    private XmlChainIterator<Proteome, Proteome> entryIterator;
	public ProteomeXmlEntryReader(String filepath) {
		init(filepath);
	}
	
	private void init(String filepath) {
        File file = new File(filepath);
        List<String> collect = null;

        if (file.isFile()) {
            collect = new ArrayList<>();
            collect.add(filepath);
            
        }else if (file.isDirectory()) {
            String[] list = file.list((dir, name) -> name.endsWith(".xml") ||  name.endsWith(".xml.gz"));

            if (list.length == 0) {
                throw new RuntimeException("No proteome xml exists in the specified directory");
            }

            collect = Arrays.asList(list).stream().map((fi) ->
                    (filepath + "/" + fi)).collect(Collectors.toList());

        }else{
            throw new RuntimeException("Please specify the directory that contains the proteome xml files");
        }
        entryIterator = new XmlChainIterator<>(new XmlChainIterator.FileInputStreamIterator(collect),
                Proteome.class, PROTEOME_ROOT_ELEMENT, Function.identity() );
    }


	@Override
	public Proteome read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		if(entryIterator.hasNext())
			return entryIterator.next();
		else
			return null;
	}

}
