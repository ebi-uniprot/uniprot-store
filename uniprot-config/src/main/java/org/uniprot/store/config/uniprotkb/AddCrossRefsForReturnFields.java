package org.uniprot.store.config.uniprotkb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Created 03/03/2020
 *
 * @author Edd
 */
public class AddCrossRefsForReturnFields {
    private static List<JsonNode> fields;
    private static ObjectMapper om;

    public static void main(String[] args) throws IOException {
        om = new ObjectMapper();
        fields = new ArrayList<>();
        JsonNode rootNode =
                om.readTree(
                        new File(
                                "/home/eddturner/working/intellij/website/uniprot-store/uniprot-config/src/main/resources/result-fields-config/uniprotkb-result-fields.json"));

        AtomicInteger itemPosition = new AtomicInteger();
        rootNode.iterator().forEachRemaining(node -> convertDRField(node, itemPosition));

        printFields();
    }

    private static void convertDRField(JsonNode node, AtomicInteger itemPosition) {
        if (node.has("name")) {
            String name = node.get("name").asText();
            if (name.startsWith("dr_")) {
                ((ObjectNode) node).put("path", "databaseCrossReferences");
                ((ObjectNode) node)
                        .put("filter", "[?(@.databaseType=='" + node.get("label").asText() + "')]");
            }
        }
        fields.add(node);
    }

    private static void printFields() {
        try {
            System.out.println(om.writerWithDefaultPrettyPrinter().writeValueAsString(fields));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
