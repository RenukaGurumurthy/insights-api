package org.gooru.insights.api.services;

import org.gooru.insights.api.daos.BaseRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ClasspageServiceImpl implements ClasspageService {

	@Autowired
	private BaseRepository baseRepository;
	
	@Override
	public String getTitle(Integer contentId) {
		return baseRepository.getTitle(contentId);
	}

}
