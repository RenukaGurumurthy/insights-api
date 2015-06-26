package org.gooru.insights.api.daos;

import java.io.IOException;
import java.io.Reader;

import javax.annotation.Resource;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.ibatis.common.resources.Resources;
import com.ibatis.sqlmap.client.SqlMapClient;
import com.ibatis.sqlmap.client.SqlMapClientBuilder;

@Repository
@Transactional
public class BaseRepositoryImpl implements BaseRepository {
   
	@Resource(name = "sessionFactoryReadOnly")
    private SessionFactory masterDataFactory;
    
	@Resource(name = "sessionFactoryPSQLReadOnly")
    private SessionFactory sessionActivityFactory;
    
	
	@Override
 	public Session getSession() {
 		Session currentSession = null;
		try {
			currentSession = masterDataFactory.getCurrentSession();
		} catch (Exception e) {
			currentSession = masterDataFactory.openSession();
		}
		return currentSession;
	}
	
	@Override
 	public Session getInsightsSession() {
 		Session currentSession = null;
		try {
			currentSession = sessionActivityFactory.getCurrentSession();
		} catch (Exception e) {
			currentSession = sessionActivityFactory.openSession();
		}
		return currentSession;
	}
	
	@Override
	public SqlMapClient getSQLMapClient() {
		Reader rd = null;
		SqlMapClient smc = null;
		try {
			rd = Resources.getResourceAsReader("SqlMapConfig.xml");
			smc = SqlMapClientBuilder.buildSqlMapClient(rd);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return smc;
	}
}
