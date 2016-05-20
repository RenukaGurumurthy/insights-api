package org.gooru.insights.api.services;

import java.io.File;

public interface ClassExportService {

	File exportCsv(String classId, String courseId, String unitId, String lessonId, String collectionId, String type, String userId);

}
