package com.project.bigdata.demo.Helper;

import org.springframework.stereotype.Component;

import javax.inject.Singleton;
import java.io.Serializable;
import java.util.ArrayList;

@Component
@Singleton
public class ReportExceptions implements Serializable
{
    private static final long serialVersionUID = 6734015607512574479L;
    private static ArrayList<String> exceptionList;
    private static ReportExceptions instance;

    private ReportExceptions()
    {
        exceptionList = new ArrayList<String>();
    }

    public static ReportExceptions getInstance()
    {
        if (instance == null)
        {
            instance = new ReportExceptions();
        }
        return instance;
    }


    public ArrayList<String> getExceptionList() {
        return exceptionList;
    }

}
