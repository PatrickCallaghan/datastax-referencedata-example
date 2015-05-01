package com.datastax.refdata;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.refdata.model.ExchangeSymbol;

public class RunQueries {
	
	private static Logger logger = LoggerFactory.getLogger(RunQueries.class);
	int noOfThreads = 10;
	BlockingQueue<ExchangeSymbol> queue = new ArrayBlockingQueue<ExchangeSymbol>(noOfThreads);
	ExecutorService executor = Executors.newFixedThreadPool(noOfThreads);

	private AtomicLong TOTAL_POINTS = new AtomicLong(0);
	
	public RunQueries() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		
		ReferenceDao dao = new ReferenceDao(contactPointsStr.split(","));
		
		List<ExchangeSymbol> exchangeSymbols = new DataLoader(null, null).getExchangeSymbols();
		logger.info("Symbols to fetch : " + exchangeSymbols.size());
		int fetchSize = 50000;	
		
		Timer timer = new Timer();
		dao.selectAllHistoricData(fetchSize);
		timer.end();
		
		logger.info("Select * took : " + timer.getTimeTakenSeconds() + "secs for fetchsize : " + fetchSize);
				
		for (int i = 0; i < noOfThreads; i++) {
			executor.execute(new ReaderThread(dao, queue));
		}
		
		timer = new Timer();		
		
		for (ExchangeSymbol exchangeSymbol : exchangeSymbols){
			
			try {
				queue.put(exchangeSymbol);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
				
		executor.shutdown();
		try {
			executor.awaitTermination(1, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		timer.end();
		
		logger.info("Select * by key took : " + timer.getTimeTakenSeconds() + "secs for " + dao.getRequestCount() + " requests	");
		System.exit(0);
	}

	
	class ReaderThread implements Runnable {

		private ReferenceDao dao;
		private BlockingQueue<ExchangeSymbol> queue;

		public ReaderThread(ReferenceDao dao, BlockingQueue<ExchangeSymbol> queue) {
			this.dao = dao;
			this.queue = queue;
		}

		@Override
		public void run() {			
			while(true){				
				ExchangeSymbol exchangeSymbol = queue.poll();
				
				if (exchangeSymbol!=null){
					dao.selectAllHistoricData(exchangeSymbol);
				}			
				
				if (queue.isEmpty()) break;
			}				
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new RunQueries();
	}
}
