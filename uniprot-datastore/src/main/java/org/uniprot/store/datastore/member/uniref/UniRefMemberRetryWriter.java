package org.uniprot.store.datastore.member.uniref;

import lombok.extern.slf4j.Slf4j;
import net.jodah.failsafe.RetryPolicy;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.xml.jaxb.uniref.MemberType;
import org.uniprot.core.xml.uniref.RepresentativeMemberConverter;
import org.uniprot.store.job.common.store.Store;
import org.uniprot.store.job.common.writer.ItemRetryWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import java.io.StringWriter;

/**
 * @author sahmad
 * @since 23/07/2020
 */
@Slf4j
public class UniRefMemberRetryWriter
        extends ItemRetryWriter<RepresentativeMember, RepresentativeMember> {

    private final RepresentativeMemberConverter converter;
    private Marshaller marshaller;

    public UniRefMemberRetryWriter(
            Store<RepresentativeMember> store, RetryPolicy<Object> retryPolicy) {
        super(store, retryPolicy);
        this.converter = new RepresentativeMemberConverter();
        initMarshaller("org.uniprot.core.xml.jaxb.uniref");
    }

    private void initMarshaller(String jaxbPackage) {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(jaxbPackage);
            marshaller = jaxbContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FRAGMENT, Boolean.TRUE);
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
        } catch (Exception e) {
            log.error("JAXB initialisation failed", e);
        }
    }

    @Override
    protected String extractItemId(RepresentativeMember item) {
        return item.getMemberId();
    }

    @Override
    protected String entryToString(RepresentativeMember entry) {

        MemberType xmlEntry = converter.toXml(entry);
        StringWriter writer = new StringWriter();

        try {
            this.marshaller.marshal(xmlEntry, writer);
            writer.write("\n");
            return writer.toString();
        } catch (Exception e) {
            log.error("Writing xml entry failed {}", e.getMessage());
        }

        return "";
    }

    @Override
    public RepresentativeMember itemToEntry(RepresentativeMember item) {
        return item;
    }
}
