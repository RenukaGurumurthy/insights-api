package org.gooru.insights.api.daos;

import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

@Component
public class CassandraConnectionProvider {

    private static String hosts;
    private static String clusterName;
    private static String logKeyspaceName;
    private static Cluster cluster;
    private static Session session;
    
    private static final Logger logger = LoggerFactory.getLogger(CassandraConnectionProvider.class);

    @Resource(name = "cassandra")
	private Properties cassandra;

    @PostConstruct
    private void initConnection(){
        logger.info("Loading cassandra properties");
        hosts = this.getCassandraConstant().getProperty("cluster.hosts");
        clusterName = this.getCassandraConstant().getProperty("cluster.name");
        logKeyspaceName = this.getCassandraConstant().getProperty("log.keyspace");
        initCassandraClient();
        
    }
    
    @PreDestroy
    private void closeConnection(){
    	System.out.print("closingg connection.....");
    	session.close();
    }
    
	private static void initCassandraClient() {
		try {
			System.out.print("Init connection.....");
			cluster = Cluster
					.builder()
					.withClusterName(clusterName)
					.addContactPoint(hosts)
					.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
					.withReconnectionPolicy(
							new ExponentialReconnectionPolicy(1000, 30000))
					.withLoadBalancingPolicy(
							new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
					.build();
			session = cluster.connect(logKeyspaceName);

		} catch (Exception e) {
			logger.error("Error while initializing cassandra : {}", e);
		}
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
