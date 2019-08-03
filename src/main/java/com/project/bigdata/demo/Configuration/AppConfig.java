package com.project.bigdata.demo.Configuration;

import com.project.bigdata.demo.MaprToSQLApplication;
import com.mapr.db.spark.sql.api.java.MapRDBJavaSession;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Map;

@Component
@PropertySource("classpath:resources/application.properties")
@ConfigurationProperties("")
public class AppConfig implements Serializable {

    // private Environment environment;
    private static final long serialVersionUID = 6734015607512574479L;
    private Map<String, String> props;

    public AppConfig() {    }

    public Map<String, String> getProps () {
        return props;
    }
    public void setProps(Map<String, String> props) {
        this.props = props;
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }
    @Bean
    public MapRDBJavaSession getMaprDBJavaSession() {
        return new MapRDBJavaSession(SparkSession.builder()
                .appName("kpmg")
                .master("local[*]")
                .getOrCreate());
    }
    @Bean
    public SparkContext SparkContextBuilder()
    {
        SparkConf sparkConf = new SparkConf()
                .setMaster(props.get("spark.master"))
               .setAppName(props.get("spark.app.name"))
                .set("spark.driver.allowMultipleContexts", "true");

        SparkContext sparkContext = SparkContext.getOrCreate(sparkConf);
        return  (sparkContext) ;
    }

    @Bean
    public SparkSession SparkSessionBuilder()
    {
        return new SparkSession(SparkContextBuilder());
    }

    @Bean
    public SQLContext SqlContextBuilder()
    {
        return SQLContext.getOrCreate(SparkContextBuilder());
    }


    @Bean
    public Logger logger(){
        Logger logger = LoggerFactory.getLogger(MaprToSQLApplication.class);
        return logger;
    }

}