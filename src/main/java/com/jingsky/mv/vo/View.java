package com.jingsky.mv.vo;

import com.google.gson.Gson;
import com.jingsky.mv.service.ConfigService;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

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
    private String sourceSql;
    private long current=System.currentTimeMillis();

    public View(TableView tableView){
        this.id=tableView.getId();
        this.mvName=tableView.getMvName();
        this.masterTable=tableView.getMasterTable();
        this.masterTablePk=tableView.getMasterTablePk();
        this.masterWhereSql=tableView.getWhereSql();

        Gson gson=new Gson();
        this.viewColList =gson.fromJson(tableView.getColsJson(), List.class);
        this.viewLeftJoinList =gson.fromJson(tableView.getLeftJoinJson(), List.class);
        makeSourceSql();
    }

    /**
     * 生成数据源SQL，public只是为了test代码好执行
     * @return String
     */
    public String makeSourceSql(){
        if(sourceSql!=null){
            return sourceSql;
        }
        //列中的字段收集，源表+源字段
        List<String> colList=new ArrayList<>();
        StringBuffer sb=new StringBuffer("select \n");
        //首先拼接上主键
        sb.append("    "+masterTable+"."+masterTablePk+" as "+ ConfigService.VIEW_PK +",\n");
        //拼接列
        for(ViewCol col : this.viewColList){
            //主键不再重复添加
            if(col.getSourceTable().equals(masterTable) && col.getSourceCol().equals(masterTablePk)){
                continue;
            }
            sb.append("    "+col.getSourceTable()+"."+col.getSourceCol()+" as "+col.getCol()+",\n");
            colList.add(col.getSourceTable()+"_"+col.getSourceCol());
        }
        //拼接上join关联的字段
        for(ViewLeftJoin leftJoin : this.viewLeftJoinList){
            if(!colList.contains(leftJoin.getTable()+"_"+leftJoin.getJoinCol())) {
                sb.append("    " + leftJoin.getTable() + "." + leftJoin.getJoinCol() + " as " + leftJoin.getTable() + "_" + leftJoin.getJoinCol() + ",\n");
            }
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
        return sourceSql.replaceAll(current+"="+current,masterTable+"."+masterTablePk+"='"+id+"'");
    }
}
