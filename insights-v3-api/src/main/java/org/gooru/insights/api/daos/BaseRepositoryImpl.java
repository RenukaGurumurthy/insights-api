package org.gooru.insights.api.daos;

import javax.annotation.Resource;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@Transactional
public class BaseRepositoryImpl implements BaseRepository {
   
	@Resource(name = "sessionFactoryReadOnly")
    private SessionFactory masterDataFactory;
    	
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
	
}
