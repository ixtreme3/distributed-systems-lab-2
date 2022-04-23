import dao.NodeDao;
import dao.TagDao;
import dao.impl.NodeDaoImpl;
import dao.impl.TagDaoImpl;
import database.DBUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import service.NodeService;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.SQLException;

public class Main {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) throws IOException, XMLStreamException, JAXBException, SQLException {
        logger.info("start");
        DBUtil.init();
        NodeDao nodeDao = new NodeDaoImpl(DBUtil.getConnection());
        TagDao tagDao = new TagDaoImpl();
        NodeService nodeService = new NodeService(nodeDao, tagDao);
        OsmProcessor osmProcessor = new OsmProcessor(nodeService);
        Unpacker.bz2ToOsm();
        osmProcessor.processOsm();
        logger.info("finish");
    }
}
/*
Current input objects: 30000
Speed: 1875.2344043005376
Current time: 15998
-----------------------------------
Strategy: PreparedStatement
Current input objects: 30000public
Speed: 1867.1811788137177
Current time: 16067
-----------------------------------
Strategy: Buffered
Current input objects: 30000
Speed: 28735.632183908045
Current time: 1044
 */