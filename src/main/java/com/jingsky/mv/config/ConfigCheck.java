package com.jingsky.mv.config;

import com.jingsky.mv.vo.View;
import com.jingsky.mv.vo.ViewCol;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class ConfigCheck implements InitializingBean {
    @Autowired
    private ViewsConfig viewsConfig;

    @Override
    public void afterPropertiesSet() throws Exception {
        //所有支持的聚合函数列表
        String aggregateFunctions= "sum,count,avg,min,max";
        List<View> viewList= viewsConfig.getViews();
        for(View view : viewList){
            for(ViewCol col : view.getViewColList()){
                if(col.getAggregateFunction()!=null){
                    String aggregateFunction=col.getAggregateFunction().toLowerCase();
                    if(!aggregateFunctions.contains(aggregateFunction)){
                        log.error("AggregateFunction must one of '"+aggregateFunctions+"'.");
                        System.exit(0);
                    }
                    if(view.getMasterGroupBy()==null){
                        log.error("MasterGroupBy must be set when AggregateFunction had set.");
                        System.exit(0);
                    }
                    if(!col.getSourceTable().equals(view.getMasterTable())) {
                        log.error("SourceTable must master table in ViewCol when AggregateFunction set.");
                        System.exit(0);
                    }
                }
            }
        }
    }
}
