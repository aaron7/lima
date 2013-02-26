package uk.ac.cam.cl.groupproject12.lima.monitor;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Manages the replication of data between HBase and PostgreSQL on completion of
 * a Hadoop M-R job.
 * 
 * @author Team Lima
 * 
 */
public class EventMonitor {

	Configuration hbaseConfig = HBaseConfiguration.create();
	Connection jdbcPGSQL = null;

	public EventMonitor(HBaseConnectionDetails hbaseConf,
			IDataSynchroniser synchroniser) throws PGSQLConfigurationException, SQLException {
		hbaseConfig.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM,
				hbaseConf.getHost());
		hbaseConfig.setInt(Constants.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT,
				hbaseConf.getPort());

		// Set up PGSQL connection
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.err.println(Constants.ERROR_POSTGRESQL_DRIVER_MISSING);
			System.exit(1);
		}

		try {
			PostgreSQLConnectionDetails pgsqlConn = getPostgresConnection();

			this.jdbcPGSQL = DriverManager.getConnection(
					String.format(Constants.PGSQL_CONNECTION_STRING,
							pgsqlConn.getHost(), pgsqlConn.getPort(),
							pgsqlConn.getDbname()), pgsqlConn.getUsername(),
					pgsqlConn.getPassword());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Invoke the synchronisation method of the synchroniser
		synchroniser.synchroniseTables(this);
	}

	Configuration getHBaseConfig() {
		return this.hbaseConfig;
	}

	Connection getPGSQLConnection() {
		return this.jdbcPGSQL;
	}

	// Parses the XML file in a well-defined location to obtain PGSQL connection
	// information. Returns a PostgreSQLConnectionDetails object containing such
	// details.
	private static PostgreSQLConnectionDetails getPostgresConnection()
			throws PGSQLConfigurationException {
		File fXmlFile = new File(String.format(
				Constants.PGSQL_CONNECTION_XML_LOCATION,
				System.getProperty("user.dir")));
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		try {
			dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);

			doc.getDocumentElement().normalize();

			// Get the PGSQL connection details
			NodeList PGSQLConnectionInfo = doc.getElementsByTagName("pgsql");

			// Too many configuration entries! Cannot decide on one particular
			// one deterministically... which PGSQL are we to use? Abort and
			// report this error.
			if (PGSQLConnectionInfo.getLength() != 1) {
				throw new PGSQLConfigurationException(
						Constants.ERROR_POSTGRESQL_CONFIG_NOT_ONE);
			}

			Node node = PGSQLConnectionInfo.item(0);

			if (node.getNodeType() == Node.ELEMENT_NODE) {
				Element nodeElement = (Element) node;

				String hostname = nodeElement.getElementsByTagName("host")
						.item(0).getTextContent();
				int port = Integer.parseInt(nodeElement
						.getElementsByTagName("port").item(0).getTextContent());
				String username = nodeElement.getElementsByTagName("username")
						.item(0).getTextContent();
				String password = nodeElement.getElementsByTagName("password")
						.item(0).getTextContent();
				String dbName = nodeElement.getElementsByTagName("dbName")
						.item(0).getTextContent();

				if (hostname == null || port == 0 || username == null
						|| password == null || dbName == null) {
					throw new PGSQLConfigurationException(
							Constants.ERROR_POSTGRESQL_CONFIG_MALFORMED);
				} else {
					return new PostgreSQLConnectionDetails(hostname, port,
							username, password, dbName);
				}
			}

			throw new PGSQLConfigurationException(
					Constants.ERROR_POSTGRESQL_CONFIG_MALFORMED);
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// If it gets to this point without returning, something's horribly
		// wrong.
		return null;
	}

	public static void main(String[] args) throws PGSQLConfigurationException, SQLException {
		new EventMonitor(new HBaseConnectionDetails("localhost", 2182),
				new ThreatSynchroniser(22));
	}
}
