package com.thinkbiganalytics.install.inspector.inspection;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class JwtTokenSyncInspection extends AbstractInspection<JwtTokenSyncInspection.JwtProperties, JwtTokenSyncInspection.JwtProperties> {

    class JwtProperties {
        @Value("${security.jwt.key}")
        private String jwtKey;
    }

    @Override
    public String getName() {
        return "Jwt Token Synchronisation Check";
    }

    @Override
    public String getDescription() {
        return "Checks whether Kylo UI and Kylo Services have the same JWT tokens";
    }

    @Override
    public InspectionStatus inspect(JwtProperties servicesProperties, JwtProperties uiProperties) {
        boolean valid = servicesProperties.jwtKey.equals(uiProperties.jwtKey);
        InspectionStatus inspectionStatus = new InspectionStatus(valid);
        if (!valid) {
            inspectionStatus.setError("'security.jwt.key' property in kylo-services/conf/application.properties does not match 'security.jwt.key' property in kylo-ui/conf/application.properties");
        }
        return inspectionStatus;
    }

    @Override
    public JwtProperties getServicesProperties() {
        return new JwtProperties();
    }

    @Override
    public JwtProperties getUiProperties() {
        return new JwtProperties();
    }
}