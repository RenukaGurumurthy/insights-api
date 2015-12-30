package org.gooru.insights.api.services;

import java.util.UUID;

import org.gooru.insights.api.daos.CqlCassandraDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CassandraV2ServiceImpl implements CassandraV2Service{

	@Autowired
	private CqlCassandraDao cqlDAO;
	
	//TODO 	Test code to be removed
	@Override
	public void insertData(){
		UUID userId = UUID.randomUUID();
		for (int recordCount = 1; recordCount < 100; recordCount++) {
			UUID classId = UUID.randomUUID();
			UUID courseId = UUID.randomUUID();
			UUID unitId = UUID.randomUUID();
			UUID lessonId = UUID.randomUUID();
			cqlDAO.saveSession(userId.toString(), classId.toString(), courseId.toString(), unitId.toString(), lessonId.toString(), "collectionId", "collection", 1L, 200L, 1L);
		}
	}
}
