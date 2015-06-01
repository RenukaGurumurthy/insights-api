package org.gooru.insights.api.security;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface AuthorizeOperations
{
	public String operations();
	
	public String[] partyOperations() default {};
	
	public String partyUId() default "";

}
