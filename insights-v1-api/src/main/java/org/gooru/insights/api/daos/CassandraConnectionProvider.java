package org.gooru.insights.api.daos;

import java.util.Properties;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class CassandraConnectionProvider {

    private static String hosts;
    private static String clusterName;
    private static String logKeyspaceName;
    private static String logDataCeter;
    private static Cluster cluster;
    private static Session session;
    
    private static final Logger logger = LoggerFactory.getLogger(CassandraConnectionProvider.class);

    @Resource(name = "cassandra")
	private Properties cassandra;

    public void initConnection(){
        logger.info("Loading cassandra properties");
        hosts = this.getCassandraConstant().getProperty("analytics.cassandra.seeds");
        clusterName = this.getCassandraConstant().getProperty("analytics.cassandra.cluster");
        logKeyspaceName = this.getCassandraConstant().getProperty("analytics.cassandra.keyspace");
        logDataCeter = this.getCassandraConstant().getProperty("analytics.cassandra.datacenter");
        initCassandraClient();
    }
    
    @PreDestroy
    private void closeConnection(){
    	session.close();
    }
    
	private  static void initCassandraClient() {
		try {
			cluster = Cluster
					.builder()
					.withClusterName(clusterName)
					.addContactPoint(hosts)
					.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
					/*.withReconnectionPolicy(
							new ExponentialReconnectionPolicy(1000, 30000))
					.withLoadBalancingPolicy(
							new TokenAwarePolicy(new DCAwareRoundRobinPolicy(logDataCeter)))*/
					.build();
			session = cluster.connect(logKeyspaceName);

		} catch (Exception e) {
			logger.error("Error while initializing cassandra : {}", e);
		}
	} 
	public ProtocolVersion getProtocolVersion(){
		return cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
	}
	
	public static String getLogKeyspaceName() {
		return logKeyspaceName;
	}
	public Session getCassSession() {
		if(session == null) {
			initConnection();
		}
		return session;
	}    
    private Properties getCassandraConstant() {
    	return cassandra;
		
	}

}
