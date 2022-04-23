
import generated.Node;
import lombok.RequiredArgsConstructor;
import entity.NodeEntity;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import service.NodeService;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

@RequiredArgsConstructor
public class OsmProcessor {
    public static final int NODES_TO_PROCESS = 30000;
    private final NodeService nodeService;

    public void processOsm() throws IOException, XMLStreamException, JAXBException {
        try (
            InputStream fileInputStream = Files.newInputStream(Paths.get("RU-NVS.osm.bz2"));
            StaxStreamHandler handler = new StaxStreamHandler(new BZip2CompressorInputStream(new BufferedInputStream(fileInputStream)))
        ) {
            processDataFrom(handler);
        }
    }

    private void processDataFrom(StaxStreamHandler handler) throws XMLStreamException, JAXBException {
        XMLStreamReader reader = handler.getReader();
        JAXBContext jaxbContext = JAXBContext.newInstance(Node.class);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();

        processNodeSection(reader, unmarshaller);
    }

    private void processNodeSection(XMLStreamReader reader, Unmarshaller unmarshaller) throws XMLStreamException, JAXBException {
        saveNodesWithExecuteQuery(reader, unmarshaller, NODES_TO_PROCESS);
        saveNodesWithPreparedStatement(reader, unmarshaller, NODES_TO_PROCESS);
        saveNodesBuffered(reader, unmarshaller, NODES_TO_PROCESS);
    }

    private void saveNodesWithPreparedStatement(XMLStreamReader reader, Unmarshaller unmarshaller, int nodesToProcess) throws XMLStreamException, JAXBException {
        int count = 0;
        Long time = 0L;
        while (reader.hasNext()) {
            int event = reader.next();
            if (isNodeSectionStart(reader, event)) {
                Node node = (Node) unmarshaller.unmarshal(reader);
                NodeEntity entity = NodeEntity.convert(node);

                long cur = System.currentTimeMillis();
                nodeService.putNodeWithPreparedStatement(entity);
                cur = System.currentTimeMillis() - cur;
                count++;

                time += cur;

                if (count % NODES_TO_PROCESS == 0 && count != 0) {
                    System.out.println("Strategy: PreparedStatement");
                    System.out.println("Current input objects: " + count);
                    System.out.println("Speed: " + count / time.doubleValue() * 1000);
                    System.out.println("Current time: " + time);
                    System.out.println("-----------------------------------");
                }

                if (count == nodesToProcess)
                    break;
            }
        }
    }

    private void saveNodesBuffered(XMLStreamReader reader, Unmarshaller unmarshaller, int nodesToProcess) throws XMLStreamException, JAXBException {
        int count = 0;
        Long time = 0L;
        while (reader.hasNext()) {
            int event = reader.next();
            if (isNodeSectionStart(reader, event)) {
                Node node = (Node) unmarshaller.unmarshal(reader);

                NodeEntity entity = NodeEntity.convert(node);

                long cur = System.currentTimeMillis();
                nodeService.putNodeBuffered(entity);
                cur = System.currentTimeMillis() - cur;
                count++;

                time += cur;

                if (count % NODES_TO_PROCESS == 0 && count != 0) {
                    System.out.println("Strategy: Buffered");
                    System.out.println("Current input objects: " + count);
                    System.out.println("Speed: " + count / time.doubleValue() * 1000);
                    System.out.println("Current time: " + time);
                    System.out.println("-----------------------------------");
                }

                if (count == nodesToProcess) {
                    nodeService.flush();
                    break;
                }
            }
        }
    }

    private void saveNodesWithExecuteQuery(XMLStreamReader reader, Unmarshaller unmarshaller, int nodesToProcess) throws XMLStreamException, JAXBException {
        int count = 0;
        Long time = 0L;
        while (reader.hasNext()) {
            int event = reader.next();
            if (isNodeSectionStart(reader, event)) {
                Node node = (Node) unmarshaller.unmarshal(reader);

                NodeEntity entity = NodeEntity.convert(node);

                long cur = System.currentTimeMillis();
                nodeService.putNode(entity);
                cur = System.currentTimeMillis() - cur;
                count++;

                time += cur;

                if (count % NODES_TO_PROCESS == 0 && count != 0) {
                    System.out.println("Strategy: ExecuteQuery");
                    System.out.println("Current input objects: " + count);
                    System.out.println("Speed: " + count / time.doubleValue() * 1000);
                    System.out.println("Current time: " + time);
                    System.out.println("-----------------------------------");
                }

                if (count == nodesToProcess)
                    break;
            }
        }
    }

    private boolean isNodeSectionStart(XMLStreamReader reader, int event) {
        return event == XMLEvent.START_ELEMENT && "node".equals(reader.getLocalName());
    }
}


