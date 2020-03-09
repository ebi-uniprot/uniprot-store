package org.uniprot.store.config.uniprotkb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.uniprot.store.config.returnfield.model.ResultFieldItemType;
import org.uniprot.store.config.returnfield.model.ReturnField;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created 03/03/2020
 *
 * @author Edd
 */
public class CreateJsonForReturnFields {
    private static List<ReturnField> fields;
    private static ObjectMapper om;

    public static void main(String[] args) throws IOException {
        om = new ObjectMapper();
        fields = new ArrayList<>();
        JsonNode rootNode =
                om.readTree(
                        new File(
                                "/home/eddturner/working/intellij/website/uniprot-store/uniprot-config/src/main/resources/result-fields-config/resultfields.json"));

        AtomicInteger itemPosition = new AtomicInteger();
        rootNode.iterator().forEachRemaining(node -> createFieldForGroup(node, itemPosition));

        printFields();
    }

    private static void createFieldForGroup(JsonNode node, AtomicInteger itemPosition) {
        String groupName = node.get("groupName").asText();
        int position = itemPosition.getAndIncrement();
        ReturnField returnField = new ReturnField();
        returnField.setGroupName(groupName);
        returnField.setItemType(ResultFieldItemType.GROUP);
        returnField.setSeqNumber(position);
        returnField.setIsDatabaseGroup(node.get("isDatabase").asBoolean());
        returnField.setId(groupName.replace(" ", "_").toLowerCase());
        fields.add(returnField);

        AtomicInteger childPosition = new AtomicInteger();
        node.get("fields")
                .iterator()
                .forEachRemaining(child -> createFieldForChild(child, returnField, childPosition));
    }

    private static void createFieldForChild(
            JsonNode child, ReturnField parent, AtomicInteger childPosition) {
        String label = child.get("label").asText();
        ReturnField returnField = new ReturnField();
        returnField.setPath("fill me in!");
        returnField.setFilter("fill me in if needed!");
        returnField.setName(child.get("name").asText());
        returnField.setLabel(label);
        returnField.setParentId(parent.getId());
        returnField.setChildNumber(childPosition.getAndIncrement());
        returnField.setItemType(ResultFieldItemType.SINGLE);
        returnField.setId(parent.getId() + "/" + label.replace(" ", "_").toLowerCase());
        fields.add(returnField);
    }

    private static void printFields() {
        try {
            System.out.println(om.writerWithDefaultPrettyPrinter().writeValueAsString(fields));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
