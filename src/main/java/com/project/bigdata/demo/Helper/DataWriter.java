package com.project.bigdata.demo.Helper;

import org.apache.spark.sql.Dataset;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component
public class DataWriter implements Serializable
{
    private static final long serialVersionUID = 6734015607512574479L;
    public DataWriter(){}
    public void writeToCsv(Dataset data, String path)
    {
        data.coalesce(1).write().format("csv").option("header", "true").mode("overwrite").save(path);
    }

    public void writeToSeparateFileByKey (Dataset data, String path, String partitionCol)
    {
        data.coalesce(1).write().format("csv").option("header", "true").mode("overwrite").partitionBy(partitionCol).save(path);
    }

    public void writeToExcel(){}
}
