package com.datastax.refdata;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.refdata.model.Dividend;
import com.datastax.refdata.model.ExchangeSymbol;
import com.datastax.refdata.model.HistoricData;

public class ReferenceDao {
	
	private static Logger logger = LoggerFactory.getLogger(ReferenceDao.class);

	private AtomicLong TOTAL_POINTS = new AtomicLong(0);
	private Session session;
	private static String keyspaceName = "datastax_referencedata_demo";
	private static String tableNameHistoric = keyspaceName + ".historic_data";
	private static String tableNameDividends = keyspaceName + ".dividends";
	private static String tableNameMetaData = keyspaceName + ".exchange_metadata";

	private static final String INSERT_INTO_HISTORIC = "Insert into " + tableNameHistoric
			+ " (exchange,symbol,date,open,high,low,close,volume,adj_close) values (?,?,?,?,?,?,?,?,?);";
	private static final String INSERT_INTO_DIVIDENDS = "Insert into " + tableNameDividends
			+ " (exchange,symbol,date,dividend) values (?,?,?,?);";
	private static final String INSERT_INTO_METADATA = "Insert into " + tableNameMetaData
			+ " (exchange,symbol,last_updated_date) values (?,?,?);";
	
	
	private static final String SELECT_ALL = "select * from " + tableNameHistoric;
	private static final String SELECT_ALL_BY_KEY = "select * from " + tableNameHistoric + " where exchange=? and symbol=?";

	private PreparedStatement insertStmtHistoric;
	private PreparedStatement insertStmtDividend;
	private PreparedStatement insertStmtMetaData;
	private PreparedStatement selectStmtByKey;

	public ReferenceDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder().addContactPoints(contactPoints).build();
		this.session = cluster.connect();

		this.insertStmtHistoric = session.prepare(INSERT_INTO_HISTORIC);
		this.insertStmtDividend = session.prepare(INSERT_INTO_DIVIDENDS);
		this.insertStmtMetaData = session.prepare(INSERT_INTO_METADATA);
		this.selectStmtByKey = session.prepare(SELECT_ALL_BY_KEY);
		
		this.insertStmtHistoric.setConsistencyLevel(ConsistencyLevel.ONE);
		this.insertStmtDividend.setConsistencyLevel(ConsistencyLevel.ONE);
		this.insertStmtMetaData.setConsistencyLevel(ConsistencyLevel.ONE);
	}

	public void insertHistoricData(List<HistoricData> list) throws Exception{
		BoundStatement boundStmt = new BoundStatement(this.insertStmtHistoric);
		List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();

		Date mostRecentDate = new Date(0);		
		HistoricData mostRecent = null;
		
		for (HistoricData historicData : list) {

			boundStmt.setString("exchange", historicData.getExchange());
			boundStmt.setString("symbol", historicData.getSymbol());
			boundStmt.setDate("date", historicData.getDate());
			boundStmt.setDouble("open", historicData.getOpen());
			boundStmt.setDouble("low", historicData.getLow());
			boundStmt.setDouble("high", historicData.getHigh());
			boundStmt.setDouble("close", historicData.getClose());
			boundStmt.setInt("volume", historicData.getVolume());
			boundStmt.setDouble("adj_close", historicData.getAdjClose());

			results.add(session.executeAsync(boundStmt));
			
			if (historicData.getDate().after(mostRecentDate)){				
				mostRecentDate = historicData.getDate();
				mostRecent = historicData;
			}
						
			TOTAL_POINTS.incrementAndGet();			
		}

		//Insert most recent date.
		if (mostRecent != null){
			BoundStatement boundMetaDataStmt = new BoundStatement(this.insertStmtMetaData);
			boundMetaDataStmt.setString("exchange", mostRecent.getExchange());
			boundMetaDataStmt.setString("symbol", mostRecent.getSymbol());
			boundMetaDataStmt.setDate("last_updated_date", mostRecent.getDate());
			results.add(session.executeAsync(boundMetaDataStmt));
		}
		
		//Wait till we have everything back.
		boolean wait = true;
		while (wait) {
			// start with getting out, if any results are not done, wait is
			// true.
			wait = false;
			for (ResultSetFuture result : results) {
				if (!result.isDone()) {
					wait = true;
					break;
				}
			}
		}
		return;
	}
	
	public void insertDividend(List<Dividend> list) {
		BoundStatement boundStmt = new BoundStatement(this.insertStmtDividend);
		List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();

		for (Dividend dividend: list) {

			boundStmt.setString("exchange", dividend.getExchange());
			boundStmt.setString("symbol", dividend.getSymbol());
			boundStmt.setDate("date", dividend.getDate());
			boundStmt.setDouble("dividend", dividend.getDividend());

			results.add(session.executeAsync(boundStmt));
		}

		//Wait till we have everything back.
		boolean wait = true;
		while (wait) {
			wait = false;
			for (ResultSetFuture result : results) {
				if (!result.isDone()) {
					wait = true;
					break;
				}
			}
		}
		return;
	}
	
	public void selectAllHistoricData(int fetchSize){
		Statement stmt = new SimpleStatement(SELECT_ALL);
		stmt.setFetchSize(fetchSize);
		ResultSet rs = session.execute(stmt);
		
		Iterator<Row> iterator = rs.iterator();
		
		while (iterator.hasNext()){
			iterator.next().getDouble("close");
		}		
	}
	
	public long getTotalPoints(){
		return TOTAL_POINTS.get();
	}

	public void selectAllHistoricData(ExchangeSymbol exchangeSymbol) {
		BoundStatement bound = new BoundStatement(selectStmtByKey);
		
		ResultSetFuture results = session.executeAsync(bound.bind(exchangeSymbol.getExchange(), exchangeSymbol.getSymbol())); 
		
		for (Row row : results.getUninterruptibly()) {			
			row.getString("symbol");			
		}
	}

}
