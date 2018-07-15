package au.edu.rmit.trajectory.similarity.config;

import org.apache.ibatis.datasource.pooled.PooledDataSource;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.annotation.MapperScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import javax.sql.DataSource;

/**
 * @author forrest0402
 * @Description
 * @date 11/21/2017
 */
@Configuration
@MapperScan("au.edu.rmit.trajectory.similarity.persistence")
public class DataConfig {

    @Bean
    public DataSource dataSource() {
        PooledDataSource dataSource = new PooledDataSource();
        dataSource.setDriver("com.mysql.jdbc.Driver");
        dataSource.setUsername("root");
        dataSource.setUrl("jdbc:mysql://localhost:3306/trajectory_similarity?useUnicode=true&characterEncoding=utf-8");
        //dataSource.setUrl("jdbc:mysql://localhost:3306/t_drive?useUnicode=true&characterEncoding=utf-8");
        dataSource.setPassword("123456");
        dataSource.setPoolMaximumActiveConnections(1024);
        dataSource.setPoolMaximumIdleConnections(200);
        return dataSource;
    }

    @Bean
    public DataSourceTransactionManager transactionManager() {
        return new DataSourceTransactionManager(dataSource());
    }

    @Bean
    public SqlSessionFactoryBean sqlSessionFactory() throws Exception {
        SqlSessionFactoryBean sessionFactory = new SqlSessionFactoryBean();
        sessionFactory.setDataSource(dataSource());
        sessionFactory.setTypeAliasesPackage("au.edu.rmit.trajectory.similarity.model");
        sessionFactory.setMapperLocations(new PathMatchingResourcePatternResolver().getResources("classpath:persistence/*.xml"));
        return sessionFactory;
    }

}
