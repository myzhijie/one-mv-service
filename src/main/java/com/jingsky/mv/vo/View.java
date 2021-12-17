package com.jingsky.mv.vo;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 视图类,非数据表
 */
@Data
@NoArgsConstructor
public class View {
    private Integer id;
    //视图表名
    private String mvName;
    //目标列名
    private List<ViewCol> viewColList;
    //主表名
    private String masterTable;
    //主表主键ID
    private String masterTablePk;
    //主表where条件
    private String masterWhereSql;
    //left join的所有表
    private List<ViewLeftJoin> viewLeftJoinList;
    //查询源数据的SQL
    private String sourceSql;
    //创建视图的SQL
    private String createViewSql;
    private long current=System.currentTimeMillis();

    public View(TableView tableView){
        this.id=tableView.getId();
        this.mvName=tableView.getMvName();
        this.masterTable=tableView.getMasterTable();
        this.masterTablePk=tableView.getMasterTablePk();
        this.masterWhereSql=tableView.getWhereSql();

        this.viewColList =JSON.parseArray(tableView.getColsJson(), ViewCol.class);
        this.viewLeftJoinList=JSON.parseArray(tableView.getLeftJoinJson(),ViewLeftJoin.class);

        makeCreateViewSql();
        makeSourceSql();
    }

    /**
     * 生成创建视图的DDL语句,public只是为了test代码好执行
     * @return String
     */
    public String makeCreateViewSql(){
        if(createViewSql!=null){
            return createViewSql;
        }
        //列中的字段收集，源表+源字段：视图中字段名
        Map<String,String> colMap=new HashMap<>();
        StringBuffer sb=new StringBuffer("CREATE TABLE `"+mvName+"` ( \n");
        //拼接列
        for(ViewCol col : this.viewColList){
            sb.append("    `"+col.getCol()+"` varchar(32) NOT NULL,\n");
            colMap.put(col.getSourceTable()+col.getSourceCol(),col.getCol());
        }
        //拼接left join
        for(ViewLeftJoin leftJoin : this.viewLeftJoinList){
            String tableColName=leftJoin.getTable()+"_"+leftJoin.getJoinCol();
            //已经存在的列不需要增加
            if(colMap.keySet().contains(tableColName)) {
                sb.append("    KEY `key_"+colMap.get(tableColName)+"` (`"+colMap.get(tableColName)+"`)\n");
            }else{
                sb.append("    `" +tableColName+ "` varchar(32) NOT NULL,\n");
                sb.append("    KEY `key_"+tableColName+"` (`"+tableColName+"`),\n");
            }
        }
        sb.append("    PRIMARY KEY (`id`)\n");
        sb.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");
        createViewSql=sb.toString();
        return createViewSql;
    }

    /**
     * 生成数据源SQL，public只是为了test代码好执行
     * @return String
     */
    public String makeSourceSql(){
        if(sourceSql!=null){
            return sourceSql;
        }
        StringBuffer sb=new StringBuffer("select \n");
        //拼接列
        for(ViewCol col : this.viewColList){
            sb.append("    "+col.getSourceTable()+"."+col.getSourceCol()+" as "+col.getCol()+",\n");
        }
        //去掉最后一个多余的,
        sb=new StringBuffer(sb.substring(0,sb.length()-2)+"\n");
        //拼接主表和where
        sb.append("from "+this.masterTable);
        sb.append("\n");
        //拼接left join
        for(ViewLeftJoin leftJoin : this.viewLeftJoinList){
            sb.append("left join "+leftJoin.getTable());
            sb.append(" on "+masterTable+"."+leftJoin.getJoinLeftCol()+"="+leftJoin.getTable()+"."+leftJoin.getJoinCol());
            sb.append("\n");
        }
        sb.append("where "+current+"="+current);
        if(StringUtils.isNotBlank(masterWhereSql)){
            sb.append(" and (" +masterWhereSql+")");
        }
        sb.append("\n");
        sb.append("order by "+this.masterTable+"."+this.masterTablePk);
        sourceSql=sb.toString();
        return sourceSql;
    }

    /**
     * 追加ID条件至主表的where条件，但不改变view
     * @param id
     * @return String
     */
    public String tmpAddIdToMasterWhere(String id){
        return sourceSql.replaceAll(current+"="+current,"id='"+id+"'");
    }
}
