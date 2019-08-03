package com.project.bigdata.demo;

import com.project.bigdata.demo.Configuration.AppConfig;
import com.project.bigdata.demo.Logic.ReportLogic;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.io.Serializable;

@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
@EnableConfigurationProperties({AppConfig.class})
public class MaprToSQLApplication implements CommandLineRunner, Serializable {

	@Autowired
	ReportLogic reportLogic;
	@Autowired
	Logger logger;
	public static final long serialVersionUID = 6734015607512574479L;

	public static void main(String[] args)
	{
		SpringApplication.run(MaprToSQLApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception
	{
		String date = "";
		try
		{
			date = args[0];
		}
		catch (Exception ex)
		{
			logger.info(ex.getMessage());
		}

		reportLogic.spool();
	}
}
