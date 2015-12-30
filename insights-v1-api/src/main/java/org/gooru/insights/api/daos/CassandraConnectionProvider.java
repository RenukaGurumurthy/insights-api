package org.gooru.insights.api.daos;

import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

@Component
public class CassandraConnectionProvider {

    private static Keyspace logKeyspace;
    private static String hosts;
    private static String clusterName;
    private static String logKeyspaceName;
    private static String logDataCentre;
    
    private static final Logger logger = LoggerFactory.getLogger(CassandraConnectionProvider.class);

    @Resource(name = "cassandra")
	private Properties cassandra;

    @PostConstruct
    private void initConnection(){
    	if(logKeyspace == null){
        logger.info("Loading cassandra properties");
        hosts = this.getCassandraConstant().getProperty("cluster.hosts");
        clusterName = this.getCassandraConstant().getProperty("cluster.name");
        logKeyspaceName = this.getCassandraConstant().getProperty("log.keyspace");
        logDataCentre = this.getCassandraConstant().getProperty("log.datacentre");
        initInsights();
        }
    }
    
	private static void initInsights() {
		try {
			logger.info("Loading cassandra connection properties");
			ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl("MyConnectionPool").setPort(9160).setSeeds(hosts).setSocketTimeout(30000).setMaxTimeoutWhenExhausted(2000)
					.setMaxConnsPerHost(30).setInitConnsPerHost(1);
			if (!hosts.startsWith("127.0")) {
				poolConfig.setLocalDatacenter(logDataCentre);
			}

			AstyanaxContext<Keyspace> logContext = new AstyanaxContext.Builder().forCluster(clusterName).forKeyspace(logKeyspaceName)
					.withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE).setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN))
					.withConnectionPoolConfiguration(poolConfig).withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).buildKeyspace(ThriftFamilyFactory.getInstance());
			logContext.start();
			logKeyspace = (Keyspace) logContext.getClient();
			logger.info("Initialized connection to " + logKeyspaceName + " keyspace");
		} catch (Exception e) {
			logger.error("Error while initializing cassandra : {}", e);
		}
	}
    
	public static Keyspace getLogKeyspace() {

		if (logKeyspace == null) {
			initInsights();
		}
		return logKeyspace;
	}
    
    private Properties getCassandraConstant() {
		return cassandra;
	}
    
}
