package org.gooru.insights.api.services;

import org.gooru.insights.api.constants.ApiConstants;
import org.gooru.insights.api.constants.InsightsConstant.ColumnFamilySet;
import org.gooru.insights.api.daos.CqlCassandraDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;

@Service
public class CassandraV2ServiceImpl implements CassandraV2Service{

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
}
