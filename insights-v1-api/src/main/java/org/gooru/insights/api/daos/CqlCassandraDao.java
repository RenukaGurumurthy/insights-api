package org.gooru.insights.api.daos;

import com.netflix.astyanax.model.CqlResult;

public interface CqlCassandraDao {
	
	//TODO 	Test code to be removed
	void saveSession(String classId, String courseId, String unitId, String topicId, String lessonId, String collectionId, String collectionType, long score, long timespent, long views);
	
	CqlResult<String, String> executeCql(String columnFamilyName, String Query);
}
