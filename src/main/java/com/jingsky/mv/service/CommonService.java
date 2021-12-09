package com.jingsky.mv.service;

import com.jingsky.mv.config.TablePrefixConfig;
import com.jingsky.mv.entity.TableView;
import com.jingsky.mv.entity.WellBootstrap;
import com.jingsky.mv.mv.View;
import com.jingsky.mv.util.AsciiUtil;
import com.jingsky.mv.util.DatabaseService;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据传输常用Service方法
 */
@Service
@Slf4j
public class CommonService {
    @Autowired
    private DatabaseService toDatabaseService;
    @Autowired
    private ConfigDao configDao;
    @Autowired
    private JobService jobService;
    //所有视图列表
    private List<View> viewList;
    //mysql client ID
    private String clientId;

    public CommonService() throws SQLException, URISyntaxException {
    }

    @PostConstruct
    public void init() {
        //计算出clientId
        clientId = AsciiUtil.sumStrAscii(TablePrefixConfig.getTablePrefix()) + "";
    }

    /**
     * 开始传输
     * @return
     */
    public void startTransfer() throws Exception {
        //插入bootstrap表
        insertBootstrapTable();
        //标记调整为非终止
        jobService.setTerminate(false);
        //开始任务
        if (jobService.isAlive() == false) {
            jobService.setClientId(clientId);
            jobService.start();
        }
    }

    /**
     * 将需要进行全量迁移的表插入到bootstrap表中
     */
    private void insertBootstrapTable() throws Exception {
        List<WellBootstrap> bootstraps = configDao.findStartAndUnCompleteBootstrap();
        for (WellBootstrap bootstrap : bootstraps) {
            //删除对应视图中的数据
            TableView tableView=configDao.findById("table_view",bootstrap.getId(),TableView.class);
            toDatabaseService.execute("delete from "+tableView.getMvName());
            //从bootstrap表中删除此数据
            configDao.deleteById("bootstrap",bootstrap.getId());
        }
        //将不存在的表循环插入到bootstrap表中
        List<View> viewsList = getAllView();
        for(View view : viewsList) {
            WellBootstrap bootstrap = new WellBootstrap();
            bootstrap.setTableName(view.getMasterTable());
            bootstrap.setRepeatOrder(view.getId());
            bootstrap.setClientId(clientId);
            bootstrap.setCustomSql(view.toSql());
            bootstrap.setBatchNum(1000);
        }
    }

    /**
     * 终止并重置传输
     *
     * @return void
     */
    public void resetTransfer() {
        terminateTransfer();
        //清除maxwell所有表中数据
        //wellBootstrapDao.deleteByMap(null);
        //删除一些表
//        wellColumnsDao.deleteByMap(null);
//        wellDatabasesDao.deleteByMap(null);
//        wellHeartbeatsDao.deleteByMap(null);
//        wellPositionsDao.deleteByMap(null);
//        wellSchemasDao.deleteByMap(null);
//        wellTablesDao.deleteByMap(null);
        log.info("相关数据表清理完毕");
    }

    /**
     * 恢复传输
     *
     * @return
     */
    public void resumeTransfer() {
        jobService.setTerminate(false);
    }

    /**
     * 终止传输
     *
     * @return void
     */
    public void terminateTransfer() {
        jobService.setTerminate(true);
    }

    /**
     * 获取所有视图
     * @return
     */
    public List<View> getAllView() throws SQLException, URISyntaxException {
        if(this.viewList==null) {
            this.viewList = new ArrayList<>();
            List<TableView> tableViewList = configDao.findAllView();
            for (TableView tableView : tableViewList) {
                this.viewList.add(new View(tableView));
            }
        }
        return this.viewList;
    }
}
