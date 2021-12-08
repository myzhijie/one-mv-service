package com.jingsky.mv.maxwell.bootstrap;

import com.jingsky.mv.config.TablePrefixConfig;
import com.jingsky.mv.maxwell.util.ConnectionPool;
import com.jingsky.mv.maxwell.util.RunLoopProcess;
import com.jingsky.mv.maxwell.producer.AbstractProducer;
import com.jingsky.mv.maxwell.row.RowMap;
import com.jingsky.mv.maxwell.row.RowMapBuffer;
import com.jingsky.mv.util.exception.BootstrapException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

@Slf4j
public class BootstrapController extends RunLoopProcess {
	static final Logger LOGGER = LoggerFactory.getLogger(BootstrapController.class);
	private final long MAX_TX_ELEMENTS = 10000;
	private final ConnectionPool maxwellConnectionPool;
	private final SynchronousBootstrapper bootstrapper;
	private final AbstractProducer producer;
	private final String clientID;
	private final boolean syncMode;
	private Long currentSchemaID;

	public BootstrapController(
		ConnectionPool maxwellConnectionPool,
		AbstractProducer producer,
		SynchronousBootstrapper bootstrapper,
		String clientID,
		boolean syncMode,
		Long currentSchemaID
	) {
		this.maxwellConnectionPool = maxwellConnectionPool;
		this.producer = producer;
		this.bootstrapper = bootstrapper;
		this.clientID = clientID;
		this.syncMode = syncMode;
		this.currentSchemaID = currentSchemaID;
	}

	// this mutex is used to block rows from being produced while a "synchronous"
	// bootstrap is run
	private Object bootstrapMutex = new Object();

	// this one is used to protect against races in an async producer.
	private Object completionMutex = new Object();
	private BootstrapTask activeTask;
	private RowMapBuffer skippedRows = new RowMapBuffer(MAX_TX_ELEMENTS);

	@Override
	protected void work() throws Exception {
		try {
			doWork();
		} catch ( InterruptedException e ) {
		} catch ( SQLException e ) {
			LOGGER.error("got SQLException trying to bootstrap", e);
		}
	}

	private void doWork() throws Exception {
		List<BootstrapTask> tasks = getIncompleteTasks();
		synchronized(bootstrapMutex) {
			if(CollectionUtils.isNotEmpty(tasks)){
				startBootstrap(tasks);
			}
		}
		Thread.sleep(10000);
	}

	private void startBootstrap(List<BootstrapTask> tasks) throws Exception{
		ThreadPoolExecutor pool = new ThreadPoolExecutor(8, 8, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		List<Future> futureList = new ArrayList<>();
		for (BootstrapTask task : tasks) {
			Future future = pool.submit(
				new Callable() {
					@Override
					public Integer call() throws Exception {
						synchronized(completionMutex) {
							activeTask = task;
						}
						bootstrapper.startBootstrap(task, producer, getCurrentSchemaID());
						synchronized(completionMutex) {
							pushSkippedRows();
							activeTask = null;
						}
						return null;
					}});
			futureList.add(future);
		}

		for (Future future : futureList) {
			try {
				future.get();
			} catch (ExecutionException e) {
				if(e.getCause() instanceof BootstrapException){
					throw (BootstrapException) e.getCause();
				}
				throw (Exception) e.getCause();
			}
		}
		pool.shutdown();

		while (!pool.isTerminated()) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				log.error("线程sleep异常,异常信息:{}", e.getMessage(), e);
			}
		}
	}

	private synchronized Long getCurrentSchemaID() {
		return this.currentSchemaID;
	}

	public synchronized void setCurrentSchemaID(long schemaID) {
		this.currentSchemaID = schemaID;
	}

	private List<BootstrapTask> getIncompleteTasks() throws SQLException {
		ArrayList<BootstrapTask> list = new ArrayList<>();
		try ( Connection cx = maxwellConnectionPool.getConnection() ) {
			PreparedStatement s = cx.prepareStatement("select * from "+ TablePrefixConfig.getTablePrefix() +"well_bootstrap where is_complete = 0 and client_id = ? and (started_at is null or started_at <= now()) order by isnull(started_at), started_at asc, id asc");
			s.setString(1, this.clientID);

			ResultSet rs = s.executeQuery();
			//本次bootstrap任务ID
			String jobId= UUID.randomUUID().toString();
			while (rs.next()) {
				BootstrapTask bootstrapTask=BootstrapTask.valueOf(rs);
				bootstrapTask.jobId=jobId;
				list.add(bootstrapTask);
			}
		}
		return list;
	}

	public boolean shouldSkip(RowMap row) throws IOException {
		// The main replication thread skips rows of the currently bootstrapped
		// table and the tables that are queued for bootstrap. The bootstrap thread replays them at
		// the end of the bootstrap.

		if ( syncMode )
			synchronized(bootstrapMutex) { return false; }
		else {
			synchronized (completionMutex) {
				if (activeTask == null)
					return false;

				// async mode with an active task
				if (activeTask.matches(row)) {
					skippedRows.add(row);
					return true;
				} else
					return false;
			}
		}
	}

	private void pushSkippedRows() throws Exception {
		skippedRows.flushToDisk();
		while ( skippedRows.size() > 0 ) {
			RowMap row = skippedRows.removeFirst();
			producer.push(row);
		}
	}

}
