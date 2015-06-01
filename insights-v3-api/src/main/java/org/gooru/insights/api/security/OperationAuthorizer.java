package org.gooru.insights.api.security;

import java.util.List;

import org.gooru.insights.api.constants.InsightsOperationConstants;
import org.gooru.insights.api.models.RoleEntityOperation;
import org.gooru.insights.api.models.User;
import org.gooru.insights.api.models.UserRoleAssoc;
import org.springframework.stereotype.Component;

@Component
public class OperationAuthorizer  {

	public boolean hasAuthorization(AuthorizeOperations authorizeOperations) {
		if (hasAuthority(authorizeOperations)) {
			return true;
		}
		if (hasAuthority(authorizeOperations)) {
			return true;
		}
		return false;
	}

	private boolean hasAuthority(AuthorizeOperations authorizeOperations) {

		List<String> authorities = getAuthorizationsFromCache();

		if (authorities != null && authorizeOperations.operations() != null) {
			for (String entityOperation : authorizeOperations.operations().split(",")) {
				if (authorities.contains(entityOperation)) {
					return true;
				}
			}
		}

		return false;
	}

	public boolean hasRole(Short userRoleId, User user) {
		if (user != null && user.getUserRoleSet() != null) {
			for (UserRoleAssoc userRoleAssoc : user.getUserRoleSet()) {
				if (userRoleAssoc.getRole().getRoleId().equals(userRoleId)) {
					return true;
				}
			}
		}
		return false;
	}

	public boolean hasAuthorization(String operation, User user) {
		if (user != null && operation != null && user.getUserRoleSet() != null) {
			for (UserRoleAssoc userRoleAssoc : user.getUserRoleSet()) {
				if (userRoleAssoc.getRole().getRoleOperations() == null) {
					break;
				}
				for (RoleEntityOperation entityOperation : userRoleAssoc.getRole().getRoleOperations()) {
					if ((entityOperation.getEntityOperation().getEntityName() + InsightsOperationConstants.ENTITY_ACTION_SEPARATOR + entityOperation.getEntityOperation().getOperationName()).equals(operation)) {
						return true;
					}
				}
			}
		}
		return false;
	}

	public boolean hasAuthorization(String operation) {
		if (hasAutority(operation)) {
			return true;
		}
		if (hasAutority(operation)) {
			return true;
		}
		return false;
	}

	private boolean hasAutority(String operation) {
		List<String> authorities = getAuthorizationsFromCache();
		if (authorities != null && operation != null && authorities != null && authorities.contains(operation)) {
			return true;
		}
		return false;
	}

	private List<String> getAuthorizationsFromCache() {
		//return SessionContextSupport.getUserCredential().getOperationAuthorities();
		return null;
	}

}
