package org.gooru.insights.api.services;


public interface MailerService {
	
	
    void sendMail(String to, String subject, String body, String file);
   
    void sendPreConfiguredMail(String message); 
    
    void checkSendType(String to, String subject, String body, String file);

}
