package org.gooru.insights.api.daos;

import java.util.List;

import javax.annotation.Resource;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@Transactional
public class BaseRepositoryImpl implements BaseRepository {
   
	@Resource(name = "sessionFactoryReadOnly")
    private SessionFactory sessionFactoryReadOnly;
    
 	public Session getSession() {
 		Session currentSession = null;
		try {
			currentSession = sessionFactoryReadOnly.getCurrentSession();
		} catch (Exception e) {
			currentSession = sessionFactoryReadOnly.openSession();
		}
		return currentSession;
	}
	
 	public String getTitle(Integer contentId) {
        String sql = "select title from resource  where content_id = " + contentId ;
        List<Object> resultList = getSession().createSQLQuery(sql).list();
        return resultList.size() > 0 ? resultList.get(0).toString() : null;
    }

}
