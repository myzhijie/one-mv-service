package com.jingsky.mv.maxwell;

import com.jingsky.mv.maxwell.bootstrap.BootstrapController;
import com.jingsky.mv.maxwell.producer.AbstractProducer;
import com.jingsky.mv.maxwell.recovery.Recovery;
import com.jingsky.mv.maxwell.recovery.RecoveryInfo;
import com.jingsky.mv.maxwell.replication.BinlogConnectorReplicator;
import com.jingsky.mv.maxwell.replication.Position;
import com.jingsky.mv.maxwell.replication.Replicator;
import com.jingsky.mv.maxwell.row.HeartbeatRowMap;
import com.jingsky.mv.maxwell.schema.*;
import com.jingsky.mv.maxwell.schema.columndef.ColumnDefCastException;
import com.jingsky.mv.maxwell.schema.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class Maxwell implements Runnable {
    protected MaxwellConfig config;
    protected MaxwellContext context;
    protected Replicator replicator;

    public Maxwell() {
    }

    //static final Logger log = LoggerFactory.getLogger(Maxwell.class);

    public Maxwell(MaxwellConfig config) throws SQLException, URISyntaxException {
        this(new MaxwellContext(config));
    }

    protected Maxwell(MaxwellContext context) throws SQLException, URISyntaxException {
        this.config = context.getConfig();
        this.context = context;
    }

    public void run() {
        try {
            start();
        } catch (Exception e) {
            log.error("maxwell encountered an exception", e);
        }
    }

    public void terminate() {
        Thread terminationThread = this.context.terminate();
        if (terminationThread != null) {
            try {
                terminationThread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private Position attemptMasterRecovery() throws Exception {
        HeartbeatRowMap recoveredHeartbeat = null;
        MysqlPositionStore positionStore = this.context.getPositionStore();
        RecoveryInfo recoveryInfo = positionStore.getRecoveryInfo(config);

        if (recoveryInfo != null) {
            Recovery masterRecovery = new Recovery(
                    config.replicationMysql,
                    config.databaseName,
                    this.context.getReplicationConnectionPool(),
                    this.context.getCaseSensitivity(),
                    recoveryInfo
            );

            recoveredHeartbeat = masterRecovery.recover();

            if (recoveredHeartbeat != null) {
                // load up the schema from the recovery position and chain it into the
                // new server_id
                MysqlSchemaStore oldServerSchemaStore = new MysqlSchemaStore(
                        context.getMaxwellConnectionPool(),
                        context.getReplicationConnectionPool(),
                        context.getSchemaConnectionPool(),
                        recoveryInfo.serverID,
                        recoveryInfo.position,
                        context.getCaseSensitivity(),
                        config.filter,
                        false
                );

                // Note we associate this schema to the start position of the heartbeat event, so that
                // we pick it up when resuming at the event after the heartbeat.
                oldServerSchemaStore.clone(context.getServerID(), recoveredHeartbeat.getPosition());
                return recoveredHeartbeat.getNextPosition();
            }
        }
        return null;
    }

    private void logColumnCastError(ColumnDefCastException e) throws SQLException, SchemaStoreException {
        try (Connection conn = context.getSchemaConnectionPool().getConnection()) {
            log.error("checking for schema inconsistencies in " + e.database + "." + e.table);
            SchemaCapturer capturer = new SchemaCapturer(conn, context.getCaseSensitivity(), e.database, e.table);
            Schema recaptured = capturer.capture();
            Table t = this.replicator.getSchema().findDatabase(e.database).findTable(e.table);
            List<String> diffs = new ArrayList<>();

            t.diff(diffs, recaptured.findDatabase(e.database).findTable(e.table), "old", "new");
            if (diffs.size() == 0) {
                log.error("no differences found");
            } else {
                for (String diff : diffs)
                    log.error(diff);
            }
        }
    }

    protected Position getInitialPosition() throws Exception {
        /* first method:  do we have a stored position for this server? */
        Position initial = this.context.getInitialPosition();

        if (initial == null) {

            /* second method: are we recovering from a master swap? */
            if (config.masterRecovery)
                initial = attemptMasterRecovery();

			/* third method: is there a previous client_id?
			   if so we have to start at that position or else
			   we could miss schema changes, see https://github.com/zendesk/maxwell/issues/782 */

//			if ( initial == null ) {
//				initial = this.context.getOtherClientPosition();
//				if ( initial != null ) {
//					log.info("Found previous client position: " + initial);
//				}
//			}

            /* fourth method: capture the current master position. */
            if (initial == null) {
                try (Connection c = context.getReplicationConnection()) {
                    initial = Position.capture(c, config.gtidMode);
                }
            }

            /* if the initial position didn't come from the store, store it */
            context.getPositionStore().set(initial);
        }

        if (config.masterRecovery) {
            this.context.getPositionStore().cleanupOldRecoveryInfos();
        }

        return initial;
    }

    public String getMaxwellVersion() {
        String packageVersion = getClass().getPackage().getImplementationVersion();
        if (packageVersion == null)
            return "??";
        else
            return packageVersion;
    }

    static String bootString = "Maxwell v%s is booting (%s), starting at %s";

    private void logBanner(AbstractProducer producer, Position initialPosition) {
        String producerName = producer.getClass().getSimpleName();
        log.info(String.format(bootString, getMaxwellVersion(), producerName, initialPosition.toString()));
    }

    protected void onReplicatorStart() {
    }

    protected void onReplicatorEnd() {
    }

    public void start() throws Exception {
        try {
            startInner();
        } catch (Exception e) {
            this.context.terminate(e);
        } finally {
            onReplicatorEnd();
            this.terminate();
        }

        Exception error = this.context.getError();
        if (error != null) {
            throw error;
        }
    }

    private void startInner() throws Exception {
        try (Connection connection = this.context.getReplicationConnection();
             Connection rawConnection = this.context.getRawMaxwellConnection()) {
            MaxwellMysqlStatus.ensureReplicationMysqlState(connection);
            MaxwellMysqlStatus.ensureMaxwellMysqlState(rawConnection);
            if (config.gtidMode) {
                MaxwellMysqlStatus.ensureGtidMysqlState(connection);
            }

            SchemaStoreSchema.ensureMaxwellSchema(rawConnection, this.config.databaseName);

            try (Connection schemaConnection = this.context.getMaxwellConnection()) {
                SchemaStoreSchema.upgradeSchemaStoreSchema(schemaConnection);
            }
        }

        AbstractProducer producer = this.context.getProducer();

        Position initPosition = getInitialPosition();
        logBanner(producer, initPosition);
        this.context.setPosition(initPosition);

        MysqlSchemaStore mysqlSchemaStore = new MysqlSchemaStore(this.context, initPosition);
        BootstrapController bootstrapController = this.context.getBootstrapController(mysqlSchemaStore.getSchemaID());

        if (config.recaptureSchema) {
            mysqlSchemaStore.captureAndSaveSchema();
        }

        mysqlSchemaStore.getSchema(); // trigger schema to load / capture before we start the replicator.

        this.replicator = new BinlogConnectorReplicator(
                mysqlSchemaStore,
                producer,
                bootstrapController,
                config.replicationMysql,
                config.replicaServerID,
                config.databaseName,
                context.getMetrics(),
                initPosition,
                false,
                config.clientID,
                context.getHeartbeatNotifier(),
                config.scripting,
                context.getFilter(),
                config.outputConfig,
                config.bufferMemoryUsage
        );

        context.setReplicator(replicator);
        this.context.start();

        replicator.startReplicator();
        this.onReplicatorStart();

        try {
            replicator.runLoop();
        } catch (ColumnDefCastException e) {
            logColumnCastError(e);
        }
    }
}
