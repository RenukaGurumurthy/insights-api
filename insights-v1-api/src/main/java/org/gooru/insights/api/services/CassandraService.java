package org.gooru.insights.api.services;

import java.util.Set;

import com.datastax.driver.core.ResultSet;

public interface CassandraService {	

	ResultSet getSessionActivityType(String sessionId, String gooruOid);

	ResultSet getUserCollectionSessions(String userUid, String collectionUid,
			String collectionType, String classUid, String courseUid,
			String unitUid, String lessonUid);

	ResultSet getUserAssessmentSessions(String userUid, String collectionUid,
			String collectionType, String classUid, String courseUid,
			String unitUid, String lessonUid, String eventType);

	ResultSet getUserSessionActivity(String sessionId);

	ResultSet getUserSessionContentActivity(String sessionId, String gooruOid);

	ResultSet getUserCurrentLocationInClass(String classUid, String userUid);

	ResultSet getAllUserCurrentLocationInClass(String classUid);

	ResultSet getUserClassContentLatestSession(String classUid,
			String courseUid, String unitUid, String lessonUid,
			String collectionUid, String userUid);

	ResultSet getUsersClassContentLatestSession(String classUid,
			String courseUid, String unitUid, String lessonUid,
			String collectionUid);

	ResultSet getUserClassActivityDatacube(String rowKey, String userUid,
			String collectionType);

	ResultSet getClassActivityDatacube(String sessionId, String collectionType);

	ResultSet getUserPeerDetail(String rowKey);

	ResultSet getSubjectActivity(String rowKey, String subjectId);

	ResultSet getCourseActivity(String rowKey, String subjectId, String courseId);

	ResultSet getDomainActivity(String rowKey, String subjectId,
			String courseId, String domainId);

	ResultSet getStandardsActivity(String rowKey, String subjectId,
			String courseId, String domainId, String standardsId);

	ResultSet getStudentQuestionGrade(String teacherUid, String userUid,
			String sessionId);

	ResultSet getClassCollectionCount(String classUid, String collectionUid);

	ResultSet getAuthorizedUsers(String gooruOid);

	ResultSet getStatisticalMetrics(String gooruOids);

	ResultSet getStudentsClassActivity(String classId, String courseId, String unitId, String lessonId,
			String collectionId);	
	ResultSet getTaxonomyItemCount(Set<String> ids);
	
	ResultSet getTaxonomyParents(String taxonomyId);
	
	ResultSet getSessionResourceTaxonomyActivity(String sessionId, String gooruOid);

	ResultSet getEvent(String eventId);

	ResultSet getSesstionIdsByUserId(String userUid);
}
