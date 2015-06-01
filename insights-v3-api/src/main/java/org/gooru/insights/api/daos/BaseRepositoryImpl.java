package org.gooru.insights.api.daos;

import java.util.List;

import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
@Transactional
public class BaseRepositoryImpl implements BaseRepository {

    @Autowired
    private SessionFactory localSessionFactory;
    
    @Autowired
    private SessionFactory gooruSlaveSessionFactory;
    
    @Autowired
    private SessionFactory gooruQASessionFactory;
    

    public String getTitle(Integer contentId) {
        String sql = "select title from resource  where content_id = " + contentId ;
        List<Object> r = getLocalSession().getCurrentSession().createSQLQuery(sql).list();
        return r.size() > 0 ? r.get(0).toString() : null;
    }

	public SessionFactory getGooruSlaveSession() {
		return gooruSlaveSessionFactory;
	}
	
	public SessionFactory getGooruQASession() {
		return gooruQASessionFactory;
	}
	
	public SessionFactory getLocalSession() {
		return localSessionFactory;
	}

}
