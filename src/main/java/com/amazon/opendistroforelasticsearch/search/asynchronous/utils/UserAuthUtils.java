package com.amazon.opendistroforelasticsearch.search.asynchronous.utils;

import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import org.elasticsearch.common.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UserAuthUtils {

    @SuppressWarnings("unchecked")
    public static User parseUser(Map<String, Object> userDetails) throws IOException {
        if(userDetails == null) {
            return null;
        }
        String name = "";
        List<String> backendRoles = new ArrayList<>();
        List<String> roles = new ArrayList<>();
        List<String> customAttNames = new ArrayList<>();
        for (Map.Entry<String,Object> userDetail : userDetails.entrySet())
        {
            String fieldName = userDetail.getKey();
            switch (fieldName) {
                case User.NAME_FIELD:
                    name = (String) userDetail.getValue();
                    break;
                case User.BACKEND_ROLES_FIELD:
                    if(userDetail.getValue()!= null)
                        backendRoles = (List<String>) userDetail.getValue();
                    break;
                case User.ROLES_FIELD:
                    if(userDetail.getValue()!= null)
                        roles = (List<String>) userDetail.getValue();
                    break;
                case User.CUSTOM_ATTRIBUTE_NAMES_FIELD:
                    if(userDetail.getValue()!= null)
                        customAttNames = (List<String>) userDetail.getValue();
                    break;
                default:
                    break;
            }
        }
        return new User(name, backendRoles, roles, customAttNames);
    }

    public static boolean isUserValid(@Nullable User currentUser, @Nullable User originalUser) {
        if(originalUser == null || currentUser == null) {
            return true;
        }
        if(currentUser.getBackendRoles() == null) {
            return originalUser.getBackendRoles() == null;
        }
        return currentUser.getBackendRoles().containsAll(originalUser.getBackendRoles());
    }
}
