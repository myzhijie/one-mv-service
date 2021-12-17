package com.jingsky.mv.maxwell.producer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jingsky.mv.maxwell.MaxwellContext;
import com.jingsky.mv.maxwell.row.RowMap;
import com.jingsky.mv.vo.View;
import com.jingsky.mv.util.GetBeanUtil;
import com.jingsky.mv.util.exception.BootstrapException;
import com.jingsky.mv.util.exception.IncrementException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import java.io.IOException;
import java.util.*;

/**
 * 物化视图生产者
 */
@Slf4j
public class ViewProducer extends AbstractProducer {
    //辅助类
    private ViewProducerHelper helper = (ViewProducerHelper) GetBeanUtil.getContext().getBean("viewProducerHelper");
    //bootstrap时批量的数据
    ThreadLocal<ArrayList<RowMap>> bootstrapRowMapList = ThreadLocal.withInitial(() -> new ArrayList<>());
    //用来记录bootstrap的总条数
    ThreadLocal<Integer> bootstrapNum = ThreadLocal.withInitial(() -> new Integer(0));

    public ViewProducer(MaxwellContext context) throws IOException{
        super(context);
        //初始化一些数据
        try {
            helper.initViewsData();
        }catch (Exception e){
            throw new IOException("ViewProducer initViewsData exception:"+e.getMessage());
        }
    }

    @Override
    public void push(RowMap r) throws Exception {
        JSONObject jsonObject = JSON.parseObject(r.toJSON(outputConfig));
        //查询变动类型
        String type = jsonObject.getString("type");
        //必须是视图中存在的表才进行处理
        if(!helper.chkTableInView(r.getTable())) {
            context.setPosition(r);
            return;
        }
        //处理bootstrap
        if (type.contains("bootstrap-")) {
            View view = helper.getView4Bootstrap(r.getTable(),r.getRepeatOrder());
            try {
                handleBootstrap(r,type, view);
            }catch (Exception exception){
                throw new BootstrapException(view,r,exception);
            }
            context.setPosition(r);
            return;
        }

        //处理增量
        try {
            if (type.equals("insert")) {
                handleInsert(r);
            }else if (type.equals("update")) {
                handleUpdate(r);
            }else if (type.equals("delete")) {
                handleDelete(r);
            }else if (type.equals("table-create") || type.equals("table-drop") || type.equals("table-alter")){
                //do nothing
                log.info("Table-create|drop|alter:"+jsonObject);
            } else {
                throw new RuntimeException("Unknown RowMap type:" + type + ",rowMap:" + jsonObject);
            }
        }catch (Exception exception){
            throw new IncrementException(r,exception);
        }
        context.setPosition(r);
    }

    /**
     * 处理数据删除
     * @param rowMap
     */
    private void handleDelete(RowMap rowMap) throws Exception {
        //获取这个表关联的视图
        List<View> viewList=helper.getViewsByTable(rowMap.getTable());
        for(View view : viewList){
            //此表作为view的主表时
            if(view.getMasterTable().equals(rowMap.getTable())){
                //判断删除前后对where条件的影响
                int flag= helper.chkWhere4RowMap(rowMap,view.getMasterWhereSql());
                if(flag==-1){
                    //需删除
                    helper.delData4View(rowMap,view);
                }else{
                    //其他情况不处理
                }
            }else{
                //非view主表删除时，因没有where条件则直接清空View中的对应列。
                String whereCol=helper.getViewUpdateIdByTable(rowMap.getTable(),view.getId());
                helper.updateData4View(view.getId(),view.getMvName(),whereCol,rowMap);
            }
        }
    }

    /**
     * 处理数据更新
     * @param rowMap
     */
    private void handleUpdate(RowMap rowMap) throws Exception {
        //获取这个表关联的视图
        List<View> viewList=helper.getViewsByTable(rowMap.getTable());
        for(View view : viewList){
            //此表作为view的主表时
            if(view.getMasterTable().equals(rowMap.getTable())){
                int flag= helper.chkWhere4RowMap(rowMap,view.getMasterWhereSql());
                //判断修改前后对where条件的影响
                if(flag==1){
                    //需新增
                    helper.insertData4View(rowMap,view);
                }else if(flag==-1){
                    //需删除
                    helper.delData4View(rowMap,view);
                }else if(flag==2){
                    //需更新
                    helper.updateData4View(view.getId(),view.getMvName(),view.getMasterTablePk(),rowMap);
                }else{
                    //修改前后都不在where范围内不处理
                }
            }else{
                //非view主表时，因没有where条件直接尝试更新数据
                String whereCol=helper.getViewUpdateIdByTable(rowMap.getTable(),view.getId());
                helper.updateData4View(view.getId(),view.getMvName(),whereCol,rowMap);
            }
        }
    }

    /**
     * 对insert进行处理
     * @param rowMap
     * @throws Exception
     */
    private void handleInsert(RowMap rowMap) throws Exception {
        //获取这个表关联的视图
        List<View> viewList=helper.getViewsByTable(rowMap.getTable());
        for(View view : viewList){
            //新增的表不是视图的主表对视图没有影响
            if(!view.getMasterTable().equals(rowMap.getTable())){
                continue;
            }
            //主表有where条件且新建的数据不符合where条件直接跳过
            if(StringUtils.isNotEmpty(view.getMasterWhereSql()) && helper.chkWhere4RowMap(rowMap,view.getMasterWhereSql())!=1){
                continue;
            }
            //根据SQL从库中查询数据进行新增
            helper.insertData4View(rowMap,view);
        }
    }

    /**
     * 对Bootstrap进行处理
     * @param rowMap
     * @param type
     * @param view
     * @throws Exception
     */
    private void handleBootstrap(RowMap rowMap,String type,View view) throws Exception {
        if (type.equals("bootstrap-insert")) {
            bootstrapRowMapList.get().add(rowMap);
            if (bootstrapRowMapList.get().size() >= rowMap.getBatchNum()) {
                doBootstrapInsert(view);
            }
        } else if (type.equals("bootstrap-complete")) {
            if (bootstrapRowMapList.get().size() > 0) {
                doBootstrapInsert(view);
            }
        } else if (type.equals("bootstrap-start")) {
            //log.info("handleBootstrap:"+view.getMvName()+" started");
        }
    }

    /**
     * 将bootstrap的数据插入到数据库
     * @param view
     * @throws Exception
     */
    private void doBootstrapInsert(View view) throws Exception {
        helper.handleBootstrapInsert(view.getMasterTable(),bootstrapRowMapList.get());
        //记录bootstrap的条数
        bootstrapNum.set(bootstrapNum.get()+bootstrapRowMapList.get().size());
        log.info(view.getMvName()+" had bootstrapped nums: "+bootstrapNum.get());
        bootstrapRowMapList.set(new ArrayList());
    }

}
