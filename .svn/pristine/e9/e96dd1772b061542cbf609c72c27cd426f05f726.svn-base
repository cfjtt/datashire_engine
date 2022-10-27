package com.eurlanda.datashire.engine.squidFlow;

import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataSource;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.TDatabaseSquid;
import com.eurlanda.datashire.engine.util.ConfigurationUtil;
import com.eurlanda.datashire.enumeration.DataBaseType;

/**
 * 各种 join squid
 * Created by Juntao.Zhang on 2014/4/18.
 */
public class HbaseJoinSquidTest extends AbstractSquidTest {
    public static void main(String[] args) throws Exception {
//        TSquidFlow flow = createSquidFlow(JoinType.FullJoin, "full_join_user_dep");
//        flow.run(new CustomJavaSparkContext(getSparkMasterUrl(), "test", getSparkHomeDir(), getSparkJarLocation()));
    }

    private static TDatabaseSquid createCompanyDatabaseSquid() {
        TDataSource dataSource = new TDataSource(ConfigurationUtil.getInnerHbaseHost(),
                ConfigurationUtil.getInnerHbasePort(),
                "", "", "", "TEST.T_DEP", DataBaseType.HBASE_PHOENIX);

        TDatabaseSquid companySquid = new TDatabaseSquid();
        companySquid.setId("2");
        companySquid.setSquidId(2);
        companySquid.setDataSource(dataSource);
        companySquid.putColumn(new TColumn("ID", 4, TDataType.LONG, true, true));
        companySquid.putColumn(new TColumn("DEP_NAME", 5, TDataType.STRING));
        return companySquid;
    }

    private static TDatabaseSquid createPersonDatabaseSquid() {
        TDataSource dataSource = new TDataSource(ConfigurationUtil.getInnerHbaseHost(),
                ConfigurationUtil.getInnerHbasePort(),
                "", "", "", "TEST.T_USER", DataBaseType.HBASE_PHOENIX);

        TDatabaseSquid personSquid = new TDatabaseSquid();
        personSquid.setId("1");
        personSquid.setSquidId(1);
        personSquid.setDataSource(dataSource);
        personSquid.putColumn(new TColumn("ID", 1, TDataType.LONG, true, true));
        personSquid.putColumn(new TColumn("USER_NAME", 2, TDataType.STRING));
        personSquid.putColumn(new TColumn("DEP_ID", 3, TDataType.LONG));

        return personSquid;
    }


}
