package org.gooru.insights.api.services;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;

public interface ClassExportService {

	File exportCsv(String classId, String courseId, String unitId, String lessonId, String type, String userId) throws ParseException, IOException;
}
