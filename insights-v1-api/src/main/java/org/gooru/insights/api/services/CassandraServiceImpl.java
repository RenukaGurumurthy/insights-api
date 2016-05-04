package org.gooru.insights.api.services;

import java.util.Set;

import org.gooru.insights.api.daos.CqlCassandraDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.datastax.driver.core.ResultSet;

@Service
public class CassandraServiceImpl implements CassandraService{

	@Autowired
	private CqlCassandraDao cqlDAO;
	
	@Override
	public ResultSet getSessionActivityType(String sessionId, String gooruOid) {
		return cqlDAO.getSessionActivityType(sessionId, gooruOid);
	}
	
	@Override
	public ResultSet getUserCollectionSessions(String userUid,
			String collectionUid, String collectionType, String classUid,
			String courseUid, String unitUid, String lessonUid) {
		return cqlDAO.getUserCollectionSessions(userUid, collectionUid, collectionType, classUid, courseUid, unitUid, lessonUid);
	}
	
	@Override
	public ResultSet getUserAssessmentSessions(String userUid,
			String collectionUid, String collectionType, String classUid,
			String courseUid, String unitUid, String lessonUid, String eventType) {
		return cqlDAO.getUserAssessmentSessions(userUid, collectionUid, collectionType, classUid, courseUid, unitUid, lessonUid, eventType);
	}
	
	@Override
	public ResultSet getUserSessionActivity(String sessionId) {
		return cqlDAO.getUserSessionActivity(sessionId);
	}
	
	@Override
	public ResultSet getUserSessionContentActivity(String sessionId, String gooruOid) {
		return cqlDAO.getUserSessionContentActivity(sessionId, gooruOid);
	}
	
	@Override
	public ResultSet getUserCurrentLocationInClass(String classUid, String userUid) {
		return cqlDAO.getUserCurrentLocationInClass(classUid, userUid);
	}
	
	@Override
	public ResultSet getAllUserCurrentLocationInClass(String classUid) {
		return cqlDAO.getAllUserCurrentLocationInClass(classUid);
	}
	

	@Override
	public ResultSet getUserClassContentLatestSession(String classUid,
			String courseUid, String unitUid, String lessonUid,
			String collectionUid, String userUid) {
		return cqlDAO.getUserClassContentLatestSession(classUid, courseUid, unitUid, lessonUid, collectionUid, userUid);
	}
	
	@Override
	public ResultSet getUsersClassContentLatestSession(String classUid,
			String courseUid, String unitUid, String lessonUid,
			String collectionUid) {
		return cqlDAO.getUsersClassContentLatestSession(classUid, courseUid, unitUid, lessonUid, collectionUid);
	}

	@Override
	public ResultSet getUserClassActivityDatacube(String rowKey, String userUid, String collectionType) {
		return cqlDAO.getUserClassActivityDatacube(rowKey, userUid, collectionType);
	}
	
	@Override
	public ResultSet getClassActivityDatacube(String sessionId, String collectionType) {
		return cqlDAO.getClassActivityDatacube(sessionId, collectionType);
	}
	
	@Override
	public ResultSet getUserPeerDetail(String rowKey) {
		return cqlDAO.getUserPeerDetail(rowKey);
	}
	
	@Override
	public ResultSet getSubjectActivity(String rowKey, String subjectId) {
		return cqlDAO.getSubjectActivity(rowKey, subjectId);
	}
	
	@Override
	public ResultSet getCourseActivity(String rowKey, String subjectId,
			String courseId) {
		return cqlDAO.getCourseActivity(rowKey, subjectId, courseId);
	}
	
	@Override
	public ResultSet getDomainActivity(String rowKey, String subjectId,
			String courseId, String domainId) {
		return cqlDAO.getDomainActivity(rowKey, subjectId, courseId, domainId);
	}
	
	@Override
	public ResultSet getStandardsActivity(String rowKey, String subjectId,
			String courseId, String domainId, String standardsId) {
		return cqlDAO.getStandardsActivity(rowKey, subjectId, courseId, domainId, standardsId);
	}
	
	@Override
	public ResultSet getStudentQuestionGrade(String teacherUid, String userUid,
			String sessionId) {
		return cqlDAO.getStudentQuestionGrade(teacherUid, userUid, sessionId);
	}
	
	@Override
	public ResultSet getClassCollectionCount(String classUid, String collectionUid) {
		return cqlDAO.getClassCollectionCount(classUid, collectionUid);
	}

	@Override
	public ResultSet getAuthorizedUsers(String gooruOid) {
		return cqlDAO.getAuthorizedUsers(gooruOid);
	}
	
	@Override
	public ResultSet getStatisticalMetrics(String gooruOids) {
		return cqlDAO.getStatMetrics(gooruOids);
	}
	@Override
	public ResultSet getStudentsClassActivity(String classId, String courseId, String unitId, String lessonId, String collectionId) {
		return cqlDAO.getStudentsClassActivity(classId, courseId, unitId, lessonId, collectionId);
	}	
	@Override
	public ResultSet getTaxonomyItemCount(Set<String> ids) {
		return cqlDAO.getTaxonomyItemCount(ids);
	}
	
	@Override
	public 	ResultSet getTaxonomyParents(String taxonomyIds) {
		return cqlDAO.getTaxonomyParents(taxonomyIds);
	}

	@Override
	public ResultSet getSessionResourceTaxonomyActivity(String sessionId, String gooruOid) {
		return cqlDAO.getSessionResourceTaxonomyActivity(sessionId, gooruOid);
	}
	@Override
	public ResultSet getEvent(String eventId) {
		return cqlDAO.getEvent(eventId);
	}
	@Override
	public ResultSet getSesstionIdsByUserId(String userUid) {
		return cqlDAO.getSesstionIdsByUserId(userUid);
	}
}
