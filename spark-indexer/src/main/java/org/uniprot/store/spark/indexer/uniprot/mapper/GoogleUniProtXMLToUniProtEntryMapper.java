package org.uniprot.store.spark.indexer.uniprot.mapper;

import java.io.StringReader;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.xml.jaxb.uniprot.Uniprot;
import org.uniprot.core.xml.uniprot.GoogleUniProtEntryConverter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GoogleUniProtXMLToUniProtEntryMapper implements Function<String, UniProtKBEntry> {

    @Override
    public UniProtKBEntry call(String entryXml) {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance("org.uniprot.core.xml.jaxb.uniprot");
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();

            String wrappedXml =
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                            + "<uniprot xmlns=\"http://uniprot.org/uniprot\">"
                            + entryXml
                            + "</uniprot>";

            StringReader reader = new StringReader(wrappedXml);
            Uniprot uniprot = (Uniprot) unmarshaller.unmarshal(reader);

            GoogleUniProtEntryConverter converter = new GoogleUniProtEntryConverter();
            List<UniProtKBEntry> entries =
                    uniprot.getEntry().stream().map(converter::fromXml).toList();
            return entries.get(0);
        } catch (JAXBException e) {
            log.error("Error while parsing google protlm uniprot XML:{}", entryXml, e);
            throw new RuntimeException(e);
        }
    }
}
