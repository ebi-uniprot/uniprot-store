package org.uniprot.store.datastore.light.uniref;

import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;

import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.core.xml.jaxb.uniref.Entry;
import org.uniprot.core.xml.uniref.UniRefEntryLightConverter;
import org.uniprot.store.job.common.store.Store;
import org.uniprot.store.job.common.writer.ItemRetryWriter;

/**
 * @author lgonzales
 * @since 07/07/2020
 */
@Slf4j
public class UniRefEntryLightRetryWriter
        extends ItemRetryWriter<UniRefEntryLight, UniRefEntryLight> {
    private static final String HEADER =
            "<UniRef xmlns=\"http://uniprot.org/uniref\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
                    + "xsi:schemaLocation=\"http://uniprot.org/uniref http://www.uniprot.org/docs/uniref.xsd\">";
    private static final String FOOTER = "</UniRef>";
    private final UniRefEntryLightConverter converter;
    private Marshaller marshaller;

    public UniRefEntryLightRetryWriter(
            Store<UniRefEntryLight> store, RetryPolicy<Object> retryPolicy) {
        super(store, retryPolicy);
        this.converter = new UniRefEntryLightConverter();
        initMarshaller("org.uniprot.core.xml.jaxb.uniref");
    }

    private void initMarshaller(String jaxbPackage) {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(jaxbPackage);
            marshaller = jaxbContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        } catch (Exception e) {
            log.error("JAXB initiallation failed", e);
        }
    }

    @Override
    protected String extractItemId(UniRefEntryLight item) {
        return item.getId().getValue();
    }

    @Override
    protected String entryToString(UniRefEntryLight entry) {
        if (this.marshaller == null) return "";
        Entry xmlEntry = converter.toXml(entry);
        StringWriter writer = new StringWriter();
        try {
            this.marshaller.marshal(xmlEntry, writer);
            writer.write("\n");
            return writer.toString();
        } catch (Exception e) {
            log.error("writing xml entry failed");
        }

        return "";
    }

    @Override
    public UniRefEntryLight itemToEntry(UniRefEntryLight item) {
        return item;
    }

    @Override
    protected String getHeader() {
        return HEADER;
    }

    @Override
    protected String getFooter() {
        return FOOTER;
    }
}
