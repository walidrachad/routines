package ma.fm6education.sga.exchange.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@ConfigurationProperties(prefix = "talend")
@Configuration
public class JobsProperties {

    private String url;

    private String username;

    private String password;

    private String port;

    private String name;

}
