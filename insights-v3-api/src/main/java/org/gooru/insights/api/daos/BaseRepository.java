package org.gooru.insights.api.daos;

import org.hibernate.Session;

import com.ibatis.sqlmap.client.SqlMapClient;

public interface BaseRepository {
 	Session getSession();

	Session getInsightsSession();

	SqlMapClient getSQLMapClient();    
}
