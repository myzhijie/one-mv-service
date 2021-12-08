package com.jingsky.mv.entity;

import lombok.Data;
import java.io.Serializable;
import java.util.Date;

/**
 * maxwell的bootstrap表
 */
@Data
public class WellBootstrap implements Serializable {
    private static final long serialVersionUID = 1169511611471101159L;

    private Integer id;
    //database_name
    private String databaseName;
    //table_name
    private String tableName;
    //表需要多次bootstrap时的顺序
    private Integer repeatOrder;
    //自定义bootstrap SQL
    private String customSql;
    //bootstrap时多少条回调一次
    private Integer batchNum;

    private Boolean isComplete;
    private Long insertedRows;
    private Long totalRows;
    private Date createdAt;
    private Date startedAt;
    private Date completedAt;
    private String binlogFile;
    private Integer binlogPosition;
    private String clientId;
    private String comment;

}
