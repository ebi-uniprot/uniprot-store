package org.uniprot.store.config.uniprotkb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonList;

/**
 * Created 03/03/2020
 *
 * @author Edd
 */
public class MergeFilterIntoPathForReturnFields {
    private static List<JsonNode> fields;
    private static ObjectMapper om;

    public static void main(String[] args) throws IOException {
        om = new ObjectMapper();
        fields = new ArrayList<>();
        JsonNode rootNode =
                om.readTree(
                        new File(
                                "/home/edd/working/intellij/website/uniprot-store/uniprot-config/src/main/resources/return-fields-config/uniprotkb-return-fields.json"));
//                                "/home/edd/working/intellij/website/uniprot-store/uniprot-config/src/test/resources/test-return-fields.json"));

        AtomicInteger itemPosition = new AtomicInteger();
        rootNode.iterator().forEachRemaining(node -> convertFiltersToPath(node, itemPosition));

        printFields();
    }

    private static void convertFiltersToPath(JsonNode node, AtomicInteger itemPosition) {
        JsonNode oldPathNode = node.get("path");
        if (oldPathNode != null) {
            String oldPath = node.get("path").asText();
            String newPath = oldPath;
            if (node.has("filter")) {
                String filter = node.get("filter").asText();
                newPath = oldPath + filter;
//                ((ArrayNode) ((ObjectNode) node).putArray("paths"))
//                        .addAll((ArrayNode) om.valueToTree(singletonList(newPath)));
                ((ObjectNode) node).remove("filter");
            }


            if (node.has("path")) {
                ((ObjectNode) node).remove("path");
                ((ArrayNode) ((ObjectNode) node).putArray("paths"))
                        .addAll((ArrayNode) om.valueToTree(singletonList(newPath)));
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
