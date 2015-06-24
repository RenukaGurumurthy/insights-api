package org.gooru.insights.api.daos;

import org.hibernate.Session;

public interface BaseRepository {
 	Session getSession();

	Session getInsightsSession();    
}
