package au.edu.rmit.trajectory.similarity.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.PropertySource;

/**
 * @author forrest0402
 * @Description
 * @date 11/11/2017
 */
@Configuration
@ComponentScan(basePackages = {"au.edu.rmit.trajectory.similarity"})
@EnableAspectJAutoProxy(proxyTargetClass = true)
@PropertySource(value = {"classpath:project.properties", "classpath:jdbc.properties"})
public class AppConfig {


}
