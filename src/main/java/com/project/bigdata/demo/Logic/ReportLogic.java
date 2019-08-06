package com.project.bigdata.demo.Logic;

import com.project.bigdata.demo.Configuration.AppConfig;
import com.project.bigdata.demo.Helper.DataWriter;
import com.project.bigdata.demo.Helper.ReportExceptions;
import com.project.bigdata.demo.utils.SchemaFactory;
import com.mapr.db.spark.sql.api.java.MapRDBJavaSession;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

@Component
public class ReportLogic implements Serializable {

    @Autowired
    private AppConfig appConfig;
    @Autowired
    private DataWriter dataWriter;
    @Autowired
    private SparkContext sparkContext;
    @Autowired
    private SparkSession sparkSession;
    @Autowired
    private SQLContext sqlContext;
    private DateTime settleDate;
    @Autowired
    private Logger logger;
    @Autowired
    private MapRDBJavaSession mapRDBJavaSession;
    Map<String, String> prop;

    private static final long serialVersionUID = 6734015607512574479L;

    private boolean isParquetPathValid(String path)
    {
        //check each path on the file system if it exists
        try
        {
            Dataset test = sparkSession.read().parquet(path);
            return true;
        }catch (Exception ex)
        {
            ReportExceptions.getInstance().getExceptionList().add("Data not available in : " + path);
            logger.error("Data not available: " + ex.getMessage());
            return false;
        }
    }

    public void spool() throws Exception
    {   prop = appConfig.getProps();


        String startDate = (prop.get("start.date"));

        String endDate = (prop.get("end.date"));

        String month = prop.get("month");

        System.out.println("====================starting date is "+ startDate);
        System.out.println("====================ending date is "+ endDate);

        //preparing the columns for hashing
        Column col2=new Column("col_2");
        Column col3 = new Column("col_3");

        String tableName = String.format("%s_",prop.get("jdbc.tableName"));
        System.out.println(prop.get("spring.datasource.url")+" "+prop.get("jdbc.userName")+" "+prop.get("jdbc.password")+" "+tableName);
        Dataset<Row> dataset = mapRDBJavaSession.loadFromMapRDB(prop.get("table.source"), SchemaFactory.getSchema())
                .withColumn("date_col", date_format(col("date_col").divide(lit(1000)).cast(DataTypes.TimestampType), "yyyy-MM-dd HH:mm:sss"))
                .withColumn("col_2", functions.hash(col2))  //hashes the field
                .withColumn("col_3",functions.hash(col3))    //hashes the field
                .filter(col("date_col").between(startDate, endDate));

        dataset.show(10);

        dataset.write()
                .format("jdbc")
                .option("url",prop.get("spring.datasource.url"))
                .option("dbtable", tableName)
                .option("user", prop.get("jdbc.userName"))
                .option("password", prop.get("jdbc.password"))
                .mode(SaveMode.Overwrite)
                .save();
        System.out.println("===========================done saving to sql============================");
        String reportPath = String.format("%s/%s.csv",prop.get("csv.destination"),"kpmg_report_"+startDate);
        System.out.println(reportPath);
        dataWriter.writeToCsv(dataset,prop.get("csv.destination"));
        System.out.println(String.format("[+] Written %s data between %s and %s to CSV file",
                prop.get("table.source"),
                prop.get("start.date"),
                prop.get("end.date"),
                prop.get("csv.destination")));

    }



}
