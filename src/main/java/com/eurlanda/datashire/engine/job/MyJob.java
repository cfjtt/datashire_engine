package com.eurlanda.datashire.engine.job;

/**
 * Created by zhudebin on 16/3/11.
 */
public class MyJob {


    public void doWork() {
        System.out.println("=================== myjob.doWork() ====================");
        return;
        /**
        try {
            // 会发生改变的状态
            EnumSet<YarnApplicationState> yarnStateEnums = EnumSet.of(YarnApplicationState.ACCEPTED,
                    YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING,
                    YarnApplicationState.RUNNING, YarnApplicationState.SUBMITTED);

            // 获取数据库中处于 running 中状态的提交记录
            ApplicationStatusDao statusDao = ConstantUtil.getBean(ApplicationStatusDao.class);
            List<Map<String, Object>> states = statusDao.getApplicationStatusByState("'" + JobStatusEnum.RUNNING.name() + "'");

            // 比较数据库中的状态和yarn集群状态,
            // 1. 如果yarn查询结构中存在,但是状态不一致,则修改数据库状态
            // 2. 如果yarn查询结果中不存在,则需要重新查询一次yarn,来获取最新的状态


            YarnClient client = YarnClient.createYarnClient();
            client.init(new Configuration());
            client.start();
            List<ApplicationReport> reports = client.getApplications(yarnStateEnums);
            for(Map<String, Object> map : states) {
                for(ApplicationReport report : reports) {
                   if(report.getYarnApplicationState().compareTo(YarnApplicationState.RUNNING) != 0) {
                       // 找到了,但是状态不一致

                       break;
                   }
                }
                //
            }

        } catch (YarnException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
         */
    }

}
