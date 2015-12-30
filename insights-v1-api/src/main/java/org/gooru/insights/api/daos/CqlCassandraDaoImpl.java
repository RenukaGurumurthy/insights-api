package org.gooru.insights.api.daos;


import org.springframework.stereotype.Repository;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.StringSerializer;

@Repository
public class CqlCassandraDaoImpl extends CassandraConnectionProvider implements CqlCassandraDao {

	private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;

	public ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {

		ColumnFamily<String, String> aggregateColumnFamily;
		aggregateColumnFamily = new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(), StringSerializer.get());
		return aggregateColumnFamily;
	}
	
	//TODO 	Test code to be removed
	@Override
	public void saveSession(String userUid,String classId,String courseId,String unitId,String lessonId,String collectionId,String collectionType,long views, long timespent,long score){
		final String INSERT_STATEMENT = "INSERT INTO class_activity(class_uid, course_uid,unit_uid,lesson_uid,collection_uid,user_uid,collection_type,views , time_spent,score) VALUES (?, ?, ? , ? , ? , ?,?  ,? , ? , ?);";
		try {
		 getLogKeyspace()
		       .prepareQuery(accessColumnFamily("class_activity")).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
		       .withCql(INSERT_STATEMENT)
		       .asPreparedStatement()
		       .withStringValue(classId)
		       .withStringValue(courseId)
		       .withStringValue(unitId)
		       .withStringValue(lessonId)
		       .withStringValue(collectionId)
		       .withStringValue(userUid)
		       .withStringValue(collectionType)
		       .withLongValue(views)
		       .withLongValue(timespent)
		       .withLongValue(score)
		       .execute();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	}
	
	//TODO 	Test code to be removed
	/*public void readUsingCql(){		  
		List<ClassActivity> classActivityList = new ArrayList<ClassActivity>();
	
		OperationResult<CqlResult<String, String>> result = null;
		try {
			result = getLogKeyspace()
			        .prepareQuery(accessColumnFamily("class_activity"))
			        .withCql("SELECT * FROM class_activity WHERE class_uid='d46eb1b1-ad3e-11e5-8608-28e347c74d33' AND course_uid = 'd5997611-ad3e-11e5-8608-28e347c74d33';")
			        .execute();
		} catch (ConnectionException e) {
			e.printStackTrace();
		}
	System.out.println("running...");
	int i = 0;
		for (Row<String, String> row : result.getResult().getRows()) {
			ColumnList<String> columns = row.getColumns();
			System.out.print("...");
			ClassActivity ca= new ClassActivity(columns.getStringValue("class_uid", null), columns.getStringValue("course_uid", null), columns.getStringValue("unit_uid", null), columns.getStringValue("lesson_uid",
					null), columns.getStringValue("collection_uid", null), columns.getStringValue("user_uid", null), columns.getStringValue("collection_type", null),
					columns.getLongValue("score", 0L), columns.getLongValue("time_spent", 0L), columns.getLongValue("views", 0L));
			classActivityList.add(ca);
			i++;
		} 
		System.out.println("Total no.of.iterations" + i + "\n\n");

        List<ClassActivity> transform = classActivityList.stream()
                .collect(Collectors.groupingBy(classActivity -> classActivity.getUnitUid()))
                .entrySet().stream()
                .map(e -> e.getValue().stream()
                    .reduce((f1,f2) -> new ClassActivity(f1.getClassUid(),f1.getCourseUid(),f1.getUnitUid(),f1.getLessonUid(),f1.getCollectionUid(),f1.getUserUid(),f1.getCollectionType(),f1.getScore() + f2.getScore(),f1.getViews() + f2.getViews(),f1.getTimeSpent() + f2.getTimeSpent())))
                    .map(f -> f.get())
                    .collect(Collectors.toList());
            System.out.println(transform);
        
	}*/
}
