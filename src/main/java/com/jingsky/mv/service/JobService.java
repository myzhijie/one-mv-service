package com.jingsky.mv.service;

import com.jingsky.mv.config.GlobalHandler;
import com.jingsky.mv.config.TablePrefixConfig;
import com.jingsky.mv.maxwell.Maxwell;
import com.jingsky.mv.maxwell.MaxwellConfig;
import com.jingsky.mv.util.DatabaseService;
import com.jingsky.mv.util.exception.BootstrapException;
import com.jingsky.mv.util.exception.IncrementException;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据同步主Job服务类
 */
@Service
@Slf4j
@NoArgsConstructor
public class JobService extends Thread {
    @Autowired
    private GlobalHandler globalHandler;
    @Autowired
    private ConfigService configService;
    @Autowired
    private DatabaseService fromDatabaseService;
    @Autowired
    private DatabaseService toDatabaseService;
    //maxwell实例
    private Maxwell maxwell;
    //循环周期 秒
    private long heartbeatCycle = 10000L;
    //是否终止状态
    private boolean terminate = false;

    /**
     * 开始传输
     * @return
     */
    public void startTransfer() throws Exception {
        //标记调整为非终止
        setTerminate(false);
        //开始任务
        if (!isAlive()) {
            start();
        }
    }

    @Override
    public void run() {
        //启动JOB处理
        while (true) {
            try {
                //开始全量数据迁移和数据增量同步
                if (!terminate) {
                    //启动maxwell
                    startBootstrapAndConsume();
                }
            } catch (Exception e) {
                if(e instanceof BootstrapException){
                    globalHandler.handleBootstrapException((BootstrapException)e);
                }else if(e instanceof IncrementException){
                    globalHandler.handleIncrementException((IncrementException)e);
                }else{
                    globalHandler.handleOtherException(e);
                }
                log.error("TransJobService-doOnce其他异常：", e);
            }

            try {
                Thread.sleep(heartbeatCycle);
            } catch (InterruptedException e) {
                log.error("TransService-run线程休息异常：", e);
            }
        }
    }

    /**
     * 开始全量数据迁移和数据增量同步
     */
    private void startBootstrapAndConsume() throws Exception {
        //参数列表
        List<String> argsList = new ArrayList<>();
        //maxwell自己的数据库配置
        argsList.add("--user=" + toDatabaseService.getUsername());
        argsList.add("--password=" + toDatabaseService.getPassword());
        argsList.add("--host=" + toDatabaseService.getHost());
        argsList.add("--port=" + toDatabaseService.getPort());
        argsList.add("--schema_database=" + toDatabaseService.getDatabase());
        //maxwell读取binlog的配置
        argsList.add("--filter=exclude: *.*,include: " + fromDatabaseService.getDatabase() + ".*");
        argsList.add("--replication_user=" + fromDatabaseService.getUsername());
        argsList.add("--replication_password=" + fromDatabaseService.getPassword());
        argsList.add("--replication_host=" + fromDatabaseService.getHost());
        argsList.add("--replication_port=" + fromDatabaseService.getPort());
        //设置maxwell作为mysql客户端的唯一值
        argsList.add("--replica_server_id=" + TablePrefixConfig.getClientId());
        argsList.add("--producer=view");
        argsList.add("--output_file=logs/maxwell.log");//无实际作用参数
        argsList.add("--output_primary_keys=true");
        argsList.add("--output_primary_key_columns=true");
        argsList.add("--output_ddl=true");
        argsList.add("--bootstrapper=sync");
        argsList.add("--client_id=" + TablePrefixConfig.getClientId());

        String[] args = argsList.toArray(new String[0]);
        MaxwellConfig config = new MaxwellConfig(args);
        maxwell = new Maxwell(config);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> maxwell.terminate()));
        maxwell.start();
    }


    /**
     * 设置终止状态
     *
     * @param terminate
     */
    public void setTerminate(Boolean terminate) {
        this.terminate = terminate;
        if (terminate && maxwell != null) {
            maxwell.terminate();
        }
    }
}
