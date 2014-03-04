/*******************************************************************************
 * CassandraDAOImpl.java
 * insights-read-api
 * Created by Gooru on 2014
 * Copyright (c) 2014 Gooru. All rights reserved.
 * http://www.goorulearning.org/
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
package org.gooru.insights.api.daos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Resource;

import org.gooru.insights.api.services.BaseService;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.SmaLatencyScoreStrategyImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

@Repository
@Transactional
public class CassandraDAOImpl implements CassandraDAO {

	@Autowired
	private BaseService baseService;

	@Autowired
	private SessionFactory gooruSlaveSessionFactory;

	static final Logger logger = LoggerFactory
			.getLogger(CassandraDAOImpl.class);

	@Resource(name = "cassandra")
	private Properties cassandra;
	String[] reaction = { "i-need-help", "i-donot-understand", "meh",
			"i-can-understand", "i-can-explain" };
	String gooruSearchKeyspaceCli = "goorusearch.keyspace";
	String logKeyspaceCli = "log.keyspace";
	Map<String, Keyspace> connection = new HashMap<String, Keyspace>();

	public Keyspace getKeyspace(String inletKeyspace, String cassandraQuery) {

		if (connection.containsKey(inletKeyspace)) {
			return connection.get(inletKeyspace);
		} else {
			Keyspace cassandraKeyspace;
			String clusterName = null;
			String hosts = null;
			String keyspace = null;
			ConnectionPoolConfigurationImpl poolConfig = new ConnectionPoolConfigurationImpl(
					"MyConnectionPool");
			clusterName = this.getCassandraConstant().getProperty(
					"cluster.name");
			hosts = this.getCassandraConstant().getProperty("cluster.hosts");
			keyspace = this.getCassandraConstant().getProperty(inletKeyspace);
			poolConfig = new ConnectionPoolConfigurationImpl("MyConnectionPool")
					.setPort(9160).setMaxConnsPerHost(3).setSeeds(hosts);
			poolConfig
					.setLatencyScoreStrategy(new SmaLatencyScoreStrategyImpl());
			// Omit this to use round robin with a token range
			// poolConfig.setLatencyScoreStrategy(new
			// SmaLatencyScoreStrategyImpl(10000, 10000, 100, 2)); // Enabled
			// SMA.
			// Omit this to use round robin with a token range

			if (cassandraQuery.equalsIgnoreCase("cassandraClient")) {
				AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
						.forCluster(clusterName)
						.forKeyspace(keyspace)
						.withAstyanaxConfiguration(
								new AstyanaxConfigurationImpl()
										.setDiscoveryType(
												NodeDiscoveryType.RING_DESCRIBE)
										.setConnectionPoolType(
												ConnectionPoolType.TOKEN_AWARE))
						.withConnectionPoolConfiguration(poolConfig)
						.withConnectionPoolMonitor(
								new CountingConnectionPoolMonitor())
						.buildKeyspace(ThriftFamilyFactory.getInstance());

				context.start();

				cassandraKeyspace = context.getClient();
			} else if (cassandraQuery.equalsIgnoreCase("cassandraCql")) {
				AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
						.withAstyanaxConfiguration(
								new AstyanaxConfigurationImpl().setCqlVersion(
										"3.0.0").setTargetCassandraVersion(
										"1.2.6")).forCluster(clusterName)
						.forKeyspace(keyspace)
						.buildKeyspace(ThriftFamilyFactory.getInstance());

				context.start();
				cassandraKeyspace = context.getClient();
			} else {
				AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
						.forCluster(clusterName)
						.forKeyspace(keyspace)
						.withAstyanaxConfiguration(
								new AstyanaxConfigurationImpl()
										.setDiscoveryType(
												NodeDiscoveryType.RING_DESCRIBE)
										.setConnectionPoolType(
												ConnectionPoolType.TOKEN_AWARE))
						.withConnectionPoolConfiguration(poolConfig)
						.withConnectionPoolMonitor(
								new CountingConnectionPoolMonitor())
						.buildKeyspace(ThriftFamilyFactory.getInstance());

				context.start();

				cassandraKeyspace = context.getClient();
			}
			connection.put(inletKeyspace, cassandraKeyspace);

			return cassandraKeyspace;
		}
	}

	public ColumnFamily<String, String> accessColumnFamily(
			String columnFamilyName) {

		ColumnFamily<String, String> aggregateColumnFamily;

		aggregateColumnFamily = new ColumnFamily<String, String>(
				columnFamilyName, StringSerializer.get(),
				StringSerializer.get());

		return aggregateColumnFamily;
	}

	public Map<String, Object> getRowKey(String columnFamilyName,
			Map<String, String> filterSet, Integer limit, String offset) {
		ColumnFamilyQuery<String, String> query = getKeyspace(logKeyspaceCli,
				"").prepareQuery(this.accessColumnFamily(columnFamilyName))
				.setConsistencyLevel(ConsistencyLevel.CL_ONE);
		IndexQuery<String, String> indexedQuery = null;
		OperationResult<Rows<String, String>> resultRows = null;
		Map<String, Object> dataSet = new HashMap<String, Object>();
		if (filterSet != null) {
			indexedQuery = query.searchWithIndex();
		}
		if (getBaseService().checkNull(limit)) {
			indexedQuery.setRowLimit(limit);
		}
		for (Map.Entry<String, String> data : filterSet.entrySet()) {
			indexedQuery.addExpression().whereColumn(data.getKey()).equals()
					.value(data.getValue());
		}
		try {
			resultRows = indexedQuery.execute();
			Rows<String, String> listOfRows = resultRows.getResult();

			for (Row<String, String> entry : listOfRows) {

				dataSet.put(entry.getKey(), entry.getRawKey());
			}
		} catch (ConnectionException e) {
			logger.error("error while getting keys ");
		}

		return dataSet;
	}

	public List<Map<String, Object>> getColumnListUsingKey(
			String columnFamilyName, String key, Map<String, String> filterSet,
			Integer limit, String prefix, String suffix,
			Collection<String> columnList, Map<String, String> whereCondition,
			Collection<String> rawGooruOId) {
		List<Map<String, Object>> resultSet = new ArrayList<Map<String, Object>>();
		if (getBaseService().checkNull(prefix)) {
			key = prefix + "" + key;
		}
		if (getBaseService().checkNull(suffix)) {
			key = key + "" + suffix;
		}

		if (getBaseService().checkNull(key)) {
			boolean hasgrade = false;
			OperationResult<ColumnList<String>> singleRecord = null;
			RowQuery<String, String> query = getKeyspace(logKeyspaceCli,
					"cassandraClient").prepareQuery(
					this.accessColumnFamily(columnFamilyName)).getKey(key);

			if (columnList != null && !columnList.isEmpty()) {
				query.withColumnSlice(columnList);
			}
			try {
				singleRecord = query.execute();
			} catch (ConnectionException e) {
				logger.error("Error while reading cassandra: " + e);
			}

			if (columnList.contains("averageGrade")) {
				hasgrade = true;
			}
			Integer questionCount = 0;
			Integer answerCount = 0;
			Integer view = 0;
			Map<String, Object> columnMap = new HashMap<String, Object>();
			Map<String, Integer> totalReaction = new HashMap<String, Integer>();
			totalReaction.put("data", 0);
			StringBuffer json = new StringBuffer();

			for (Column<String> column : singleRecord.getResult()) {
				String columnName = column.getName();
				this.checkDataType(column, columnMap, json, totalReaction,
						rawGooruOId);

				if (columnName.equalsIgnoreCase("VC~all")) {
					view = Integer.parseInt(columnMap.get("VC~all").toString());
				}
				if (columnName.equalsIgnoreCase("total_qn_count")) {
					questionCount = Integer.parseInt(columnMap
							.get("total_qn_count").toString().trim());
				}
				if (columnName.equalsIgnoreCase("total_correct_ans")) {
					answerCount = Integer.parseInt(columnMap.get(
							"total_correct_ans").toString());
				}
			}
			if (hasgrade) {
				questionCount = questionCount * view;
				questionCount = (questionCount != 0) ? questionCount : 1;
				questionCount = (answerCount * 100 / questionCount);
				columnMap.put("averageGrade", questionCount);
			}
			if (getBaseService().checkNull(json.toString())) {
				checkReaction(json);
				columnMap.put("reaction", "[{" + json + ",\"totalReaction\":"
						+ totalReaction.get("data") + "}]");
			}
			resultSet.add(columnMap);
			return resultSet;
		} else {
			if (whereCondition != null && (!whereCondition.isEmpty())) {
				OperationResult<Rows<String, String>> multipleRecord = null;
				IndexQuery<String, String> IndexQuery = null;
				ColumnFamilyQuery<String, String> query = getKeyspace(
						logKeyspaceCli, "cassandraClient").prepareQuery(
						this.accessColumnFamily(columnFamilyName));

				for (Map.Entry<String, String> entry : whereCondition
						.entrySet()) {
					IndexQuery = query.searchWithIndex().addExpression()
							.whereColumn(entry.getKey()).equals()
							.value(entry.getValue());
				}
				if (columnList != null && !columnList.isEmpty()) {
					IndexQuery.withColumnSlice(columnList);
				}
				try {
					multipleRecord = IndexQuery.execute();
				} catch (ConnectionException e) {
					logger.error("Error while reading cassandra:" + e);
				}
				for (Row<String, String> rows : multipleRecord.getResult()) {
					StringBuffer json = new StringBuffer();

					Map<String, Integer> totalReaction = new HashMap<String, Integer>();
					totalReaction.put("data", 0);
					Map<String, Object> columnMap = new HashMap<String, Object>();
					for (Column<String> column : rows.getColumns()) {
						this.checkDataType(column, columnMap, json,
								totalReaction, rawGooruOId);
					}

					if (getBaseService().checkNull(json.toString())) {
						checkReaction(json);
						columnMap.put("reaction",
								"[{" + json + ",\"totalReaction\":"
										+ totalReaction.get("data") + "}]");
					}
					resultSet.add(columnMap);
				}
				return resultSet;
			} else {
				return null;
			}
		}

	}

	void checkDataType(Column<String> column, Map<String, Object> columnMap,
			StringBuffer json, Map<String, Integer> totalReaction,
			Collection<String> rawGooruOId) {
		String columnName = column.getName();
		if (columnName != null) {
			if (columnName.contains("R~RA~")) {
				json = buildJson(this.getKey(columnName),
						column.getLongValue(), json);
				Integer calculatedValue = (int) (totalReaction.get("data") + (column
						.getLongValue()));
				totalReaction.put("data", calculatedValue);
			} else if (columnName.contains("TS") || columnName.contains("VC")
					|| columnName.contains("total_correct_ans")
					|| columnName.contains("total_qn_count")
					|| columnName.contains("CP~Q~all~")
					|| columnName.contains("deleted")
					|| columnName.contains("item_sequence")) {
				columnMap.put(columnName, column.getLongValue());
			} else {
				if (columnName.equalsIgnoreCase("gooru_oid")
						|| columnName.equalsIgnoreCase("resource_gooru_oid")) {
					if (getBaseService().checkNull(rawGooruOId.toString())) {

						rawGooruOId.add(column.getStringValue());
					} else {

						rawGooruOId.add(column.getStringValue());
					}
				}
				columnMap.put(columnName, column.getStringValue());
			}

		}
	}

	public List<Map<String, Object>> getRowsUsingKey(String columnFamilyName,
			Collection<String> keys, Map<String, String> filterSet,
			Integer limit, String prefix, String suffix,
			Collection<String> columnList, Map<String, String> whereCondition,
			Collection<String> rawGooruOId) {
		List<Map<String, Object>> resultSet = new ArrayList<Map<String, Object>>();
		Collection<String> appendedKey = new ArrayList<String>();
		if (getBaseService().checkNull(prefix)) {
			for (String key : keys) {

				key = prefix + "" + key;
				appendedKey.add(key);
			}
		} else {
			appendedKey.addAll(keys);
		}
		if (getBaseService().checkNull(suffix)) {
			for (String key : keys) {
				key = key + "" + suffix;
				appendedKey.add(key);
			}
		} else {
			appendedKey.addAll(keys);
		}

		OperationResult<Rows<String, String>> multipleRecord = null;
		RowSliceQuery<String, String> query = getKeyspace(logKeyspaceCli,
				"cassandraClient").prepareQuery(
				this.accessColumnFamily(columnFamilyName)).getKeySlice(
				appendedKey);
		if (columnList != null && !columnList.isEmpty()) {
			query.withColumnSlice(columnList);
		}
		try {
			multipleRecord = query.execute();
		} catch (ConnectionException e) {
			logger.error("Error while reading cassandra:" + e);
		}
		for (Row<String, String> rows : multipleRecord.getResult()) {
			StringBuffer json = new StringBuffer();
			Map<String, Integer> totalReaction = new HashMap<String, Integer>();
			totalReaction.put("data", 0);
			boolean hasRecord = false;
			Map<String, Object> columnMap = new HashMap<String, Object>();
			for (Column<String> column : rows.getColumns()) {
				hasRecord = true;
				this.checkDataType(column, columnMap, json, totalReaction,
						rawGooruOId);
			}
			if (getBaseService().checkNull(json.toString())) {
				checkReaction(json);
				columnMap.put("reaction", "[{" + json + ",\"totalReaction\":"
						+ totalReaction.get("data") + "}]");
			}
			if (hasRecord) {
				resultSet.add(columnMap);
			}
		}
		return resultSet;
	}

	public void checkReaction(StringBuffer json) {
		String data = json.toString();
		for (String value : reaction) {
			if (data.contains(value)) {
			} else {
				this.buildJson(value, 0, json);
			}
		}
	}

	public String getKey(String columName) {
		String properKey = "randomReaction";
		try {
			Integer key = new Integer(columName.replaceAll("R~RA~", ""));
			if (key < 6) {
				properKey = reaction[key - 1];
			}
			return properKey;
		} catch (Exception e) {

			return properKey;
		}
	}

	public StringBuffer buildJson(String key, long value, StringBuffer json) {
		if (getBaseService().checkNull(json.toString())) {
			json.append(",\"" + key + "\":" + value + "");
		} else {
			json.append("\"" + key + "\":" + value + "");
		}
		return json;
	}

	void checkResourceDataType(Row<String, String> column,
			Map<String, Object> columnMap) {
		columnMap.put("CP-TS~all",
				column.getColumns().getLongValue("CP-TS~all", null));
		columnMap.put("CP-VC~all",
				column.getColumns().getLongValue("CP-VC~all", null));
	}

	/**
	 * Read record passing key - query for specific row
	 * 
	 * @param columnFamilyName
	 * @param key
	 */

	public OperationResult<ColumnList<String>> read(String keyspace,
			String columnFamilyName, String key) {

		OperationResult<ColumnList<String>> query = null;
		try {
			query = getKeyspace(keyspace, "cassandraClient")
					.prepareQuery(this.accessColumnFamily(columnFamilyName))
					.getKey(key).execute();

		} catch (ConnectionException e) {

			e.printStackTrace();
			logger.error("Query execution exeption" + e);
		}

		return query;
	}

	/**
	 * Read Record given row key and Querying for Specific Columns in a row
	 */

	public OperationResult<ColumnList<String>> read(String keyspace,
			String columnFamilyName, String key, Collection<String> columnList) {

		OperationResult<ColumnList<String>> query = null;
		try {
			query = getKeyspace(keyspace, "cassandraClient")
					.prepareQuery(this.accessColumnFamily(columnFamilyName))
					.getKey(key).withColumnSlice(columnList).execute();
		} catch (ConnectionException e) {

			e.printStackTrace();
			logger.error("Query execution exeption" + e);
		}

		return query;
	}

	/**
	 * Read record querying for
	 * 
	 * @param columnFamilyName
	 * @param value
	 *            = where condition value
	 * @return key
	 */

	public OperationResult<Rows<String, String>> read(String keyspace,
			String columnFamilyName, String column, String value) {

		OperationResult<Rows<String, String>> Column = null;

		try {
			Column = getKeyspace(keyspace, "cassandraClient")
					.prepareQuery(this.accessColumnFamily(columnFamilyName))
					.searchWithIndex().addExpression().whereColumn(column)
					.equals().value(value).execute();

		} catch (ConnectionException e) {

			e.printStackTrace();
			logger.error("Query execution exeption" + e);
		}
		return Column;

	}

	/*
	 * Read All rows given columName alone withColumnSlice(String... columns)
	 */
	public OperationResult<Rows<String, String>> readAll(String keyspace,
			String columnFamilyName, String column) {

		OperationResult<Rows<String, String>> queryResult = null;
		try {

			queryResult = getKeyspace(keyspace, "cassandraClient")
					.prepareQuery(this.accessColumnFamily(columnFamilyName))
					.getAllRows().withColumnSlice(column).execute();
		} catch (ConnectionException e) {

			e.printStackTrace();
			logger.error("Query execution exeption" + e);
		}
		return queryResult;

	}

	@Async
	public void markDeleteStatus() {

		StringBuffer collectionItemIds = new StringBuffer();
		Set<String> collectionId = new HashSet<String>();
		String startTime = null;
		String lastModified = null;
		OperationResult<CqlResult<String, String>> data = null;
		try {
			logger.info("get status \n");
			data = getKeyspace(logKeyspaceCli, "cassandraClient")
					.prepareQuery(this.accessColumnFamily("collection_item"))
					.withCql("SELECT endtime FROM jobtracker where key =5")
					.execute();

		} catch (ConnectionException e) {
			try {
				logger.error("getting  status failed\n");
				getKeyspace(logKeyspaceCli, "cassandraClient")
						.prepareQuery(this.accessColumnFamily("jobtracker"))
						.withCql(
								" UPDATE jobtracker SET status = 1 WHERE key ='5' ")
						.execute();
				return;
			} catch (Exception ee) {
			}
			e.printStackTrace();
			logger.error("can't get status \n");

		}
		for (Row<String, String> row : data.getResult().getRows()) {
			for (Column<String> column : row.getColumns()) {

				if (column.getName().equalsIgnoreCase("endtime")) {
					startTime = column.getStringValue();
				}
			}
		}
		try {
			if (startTime != null && !startTime.isEmpty()) {
				Query query = gooruSlaveSessionFactory
						.getCurrentSession()
						.createSQLQuery(
								"SELECT collection_item_id,c.gooru_oid,c.last_modified FROM collection_item AS ci INNER JOIN content AS c ON (ci.collection_content_id = c.content_id) WHERE c.last_modified "
										+ " BETWEEN '"
										+ startTime
										+ "' AND NOW() ORDER BY c.last_modified ASC");
				List<Object[]> collectionItemId = query.list();
				for (Object[] value : collectionItemId) {
					if (collectionItemIds != null
							&& !collectionItemIds.toString().isEmpty()) {
						collectionItemIds.append(",");
						collectionId.add(",");
					}
					collectionItemIds.append("'" + value[0].toString() + "'");
					collectionId.add(value[1].toString());
					lastModified = value[2].toString();

				}
			} else {
				return;
			}
		} catch (Exception e) {
			logger.error("Query execution exeption" + e);
			return;
		}
		StringBuffer cassandraCollectionItemIds = new StringBuffer();
		try {

			for (String id : collectionId) {

				OperationResult<Rows<String, String>> cassandraCollectionItemId = getKeyspace(
						logKeyspaceCli, "cassandraClient")
						.prepareQuery(
								this.accessColumnFamily("collection_item"))
						.searchWithIndex().addExpression()
						.whereColumn("collection_gooru_oid").equals().value(id)
						.execute();
				for (Row<String, String> row : cassandraCollectionItemId
						.getResult()) {
					if (cassandraCollectionItemIds != null
							&& !cassandraCollectionItemIds.toString().isEmpty()) {
						cassandraCollectionItemIds.append(",");
					}
					cassandraCollectionItemIds.append("'" + row.getKey() + "'");
				}
			}
			for (String id : cassandraCollectionItemIds.toString().split(",")) {
				if (!collectionItemIds.toString().contains(id)) {
					try {

						getKeyspace(logKeyspaceCli, "cassandraClient")
								.prepareQuery(
										this.accessColumnFamily("collection_item"))
								.withCql(
										"UPDATE collection_item SET deleted = 1 WHERE key="
												+ id + "").execute();

					} catch (ConnectionException e) {
						e.printStackTrace();
						logger.error("Query execution exeption" + e);
					}
				}
			}
			try {
				getKeyspace(logKeyspaceCli, "cassandraClient")
						.prepareQuery(this.accessColumnFamily("jobtracker"))
						.withCql(
								" UPDATE jobtracker SET status = 0,endtime='"
										+ lastModified + "' WHERE key ='5' ")
						.execute();
			} catch (ConnectionException e) {
				try {
					getKeyspace(logKeyspaceCli, "cassandraClient")
							.prepareQuery(this.accessColumnFamily("jobtracker"))
							.withCql(
									" UPDATE jobtracker SET status = 1 WHERE key ='5' ")
							.execute();

				} catch (Exception ee) {
				}
				e.printStackTrace();
				logger.error("Query execution exeption" + e);
			}

		} catch (ConnectionException e) {
			logger.error("transformation execution failed \n" + e);
		}
		logger.error("transformation execution completed \n");
	}

	@Async
	public void markDeleteStatus(String startTime, String endTime) {

		StringBuffer collectionItemIds = new StringBuffer();
		Set<String> collectionId = new HashSet<String>();
		String lastModified = null;
		OperationResult<CqlResult<String, String>> data = null;
		try {
			if (startTime != null && !startTime.isEmpty()) {
				Query query = gooruSlaveSessionFactory
						.getCurrentSession()
						.createSQLQuery(
								"SELECT collection_item_id,c.gooru_oid,c.last_modified FROM collection_item AS ci INNER JOIN content AS c ON (ci.collection_content_id = c.content_id) WHERE c.last_modified "
										+ " BETWEEN '"
										+ startTime
										+ "' AND  '"
										+ endTime + "' ");
				List<Object[]> collectionItemId = query.list();
				for (Object[] value : collectionItemId) {
					if (collectionItemIds != null
							&& !collectionItemIds.toString().isEmpty()) {
						collectionItemIds.append(",");
						collectionId.add(",");
					}
					collectionItemIds.append("'" + value[0].toString() + "'");
					collectionId.add(value[1].toString());
					lastModified = value[2].toString();

				}
			} else {
				return;
			}
		} catch (Exception e) {
			logger.error("Query execution failed \n" + e);
			return;
		}
		StringBuffer cassandraCollectionItemIds = new StringBuffer();
		try {

			for (String id : collectionId) {

				OperationResult<Rows<String, String>> cassandraCollectionItemId = getKeyspace(
						logKeyspaceCli, "cassandraClient")
						.prepareQuery(
								this.accessColumnFamily("collection_item"))
						.searchWithIndex().addExpression()
						.whereColumn("collection_gooru_oid").equals().value(id)
						.execute();
				for (Row<String, String> row : cassandraCollectionItemId
						.getResult()) {
					if (cassandraCollectionItemIds != null
							&& !cassandraCollectionItemIds.toString().isEmpty()) {
						cassandraCollectionItemIds.append(",");
					}
					cassandraCollectionItemIds.append("'" + row.getKey() + "'");
				}
			}
			for (String id : cassandraCollectionItemIds.toString().split(",")) {
				if (!collectionItemIds.toString().contains(id)) {
					try {

						getKeyspace(logKeyspaceCli, "cassandraClient")
								.prepareQuery(
										this.accessColumnFamily("collection_item"))
								.withCql(
										"UPDATE collection_item SET deleted = 1 WHERE key="
												+ id + "").execute();

					} catch (ConnectionException e) {
						e.printStackTrace();
						logger.error("Query execution failed \n" + e);
					}
				}
			}

		} catch (ConnectionException e) {
			logger.error("transformation not begin \n" + e);
		}
		logger.error("Transformation Execution completed \n");
	}

	public BaseService getBaseService() {
		return baseService;
	}

	public void setBaseService(BaseService baseService) {
		this.baseService = baseService;
	}

	public Properties getCassandraConstant() {
		return cassandra;
	}

	public void setCassandra(Properties cassandra) {
		this.cassandra = cassandra;
	}

}
