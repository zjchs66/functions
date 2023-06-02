package com.xjj.flink.functions;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author zhoujuncheng
 * @date 2022/9/27
 */
@FunctionHint(output = @DataTypeHint("ROW<osName STRING, os STRING,broswerName STRING,broswerVersion STRING,broswer STRING,device STRING,type STRING>"))
public class UserAgentFuntion extends TableFunction<Row> {

    UASparser uasParser = null;

    @Override
    public void open(FunctionContext context) throws Exception {
        uasParser = new UASparser(OnlineUpdater.getVendoredInputStream());
    }

    public void eval(String str) throws Exception {
        UserAgentInfo userAgentInfo = uasParser.parse(str);
        collect(Row.of(userAgentInfo.getOsFamily(),
                userAgentInfo.getOsName(),
                userAgentInfo.getUaFamily(),
                userAgentInfo.getBrowserVersionInfo(),
                userAgentInfo.getUaName(),
                userAgentInfo.getDeviceType(),
                userAgentInfo.getType()));
    }
}

