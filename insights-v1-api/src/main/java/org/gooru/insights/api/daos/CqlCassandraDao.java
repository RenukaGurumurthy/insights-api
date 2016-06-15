package org.gooru.insights.api.daos;

import java.util.Set;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;

public interface CqlCassandraDao {

	ResultSet getUserSessionActivity(String parameters);

	ResultSet getStudentQuestionGrade(String teacherUid, String userUid,
			String sessionId);

	ResultSet getStandardsActivity(String rowKey, String subjectId,
			String courseId, String domainId, String standardsId);

	ResultSet getDomainActivity(String rowKey, String subjectId,
			String courseId, String domainId);

	ResultSet getCourseActivity(String rowKey, String subjectId, String courseId);

	ResultSet getSubjectActivity(String rowKey, String subjectId);

	ResultSet getUserPeerDetail(String rowKey);

	ResultSet getClassActivityDatacube(String sessionId, String collectionType);

	ResultSet getUserClassActivityDatacube(String rowKey, String userUid,
			String collectionType);

	ResultSet getUsersClassContentLatestSession(String classUid,
			String courseUid, String unitUid, String lessonUid,
			String collectionUid);

	ResultSet getUserClassContentLatestSession(String classUid,
			String courseUid, String unitUid, String lessonUid,
			String collectionUid, String userUid);

	ResultSet getAllUserCurrentLocationInClass(String classUid);

	ResultSet getUserCurrentLocationInClass(String classUid, String userUid);

	ResultSet getUserSessionContentActivity(String sessionId, String gooruOid);

	ResultSet getUserAssessmentSessions(String userUid, String collectionUid,
			String collectionType, String classUid, String courseUid,
			String unitUid, String lessonUid, String eventType);

	ResultSet getUserCollectionSessions(String userUid, String collectionUid,
			String collectionType, String classUid, String courseUid,
			String unitUid, String lessonUid);

	ResultSet getSessionActivityType(String sessionId, String gooruOid);

	ResultSet getClassCollectionCount(String classUid, String collectionUid);

	ResultSet getTaxonomyItemCount(Set<String> ids);

	ResultSet getAuthorizedUsers(String gooruOid);

	ResultSet getStatMetrics(String gooruOids);

	ResultSet getTaxonomyParents(String taxonomyIds);

	ResultSet getStudentsClassActivity(String classId, String courseId, String unitId, String lessonId,
			String collectionId);

	ResultSet getSessionResourceTaxonomyActivity(String sessionId, String gooruOid);

	ResultSet getEvent(String eventId);

	ResultSet getSesstionIdsByUserId(String userUid);

	ResultSet getArchievedClassMembers(String classId);

	ResultSet getArchievedClassData(String rowKey);

	ResultSet getArchievedContentTitle(String contentId);

	ProtocolVersion getClusterProtocolVersion();

	ResultSet getArchievedUserDetails(String userId);

	ResultSet getArchievedCollectionItem(String contentId);

	ResultSet getArchievedCollectionRecentSessionId(String rowKey);

	ResultSet getArchievedSessionData(String sessionId);
}
