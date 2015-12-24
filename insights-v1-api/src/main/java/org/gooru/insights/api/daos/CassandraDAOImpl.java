package org.gooru.insights.api.daos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.gooru.insights.api.utils.InsightsLogger;
import org.springframework.stereotype.Repository;

import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.retry.ConstantBackoff;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;

@Repository
public class CassandraDAOImpl extends CassandraConnectionProvider implements CassandraDAO {

	private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.CL_QUORUM;

	public ColumnFamily<String, String> accessColumnFamily(String columnFamilyName) {

		ColumnFamily<String, String> aggregateColumnFamily;
		aggregateColumnFamily = new ColumnFamily<String, String>(columnFamilyName, StringSerializer.get(), StringSerializer.get());
		return aggregateColumnFamily;
	}

	/**
	 * Read record passing key - query for specific row
	 * 
	 * @param columnFamilyName
	 * @param key
	 */
	public OperationResult<ColumnList<String>> read(String columnFamilyName, String key) {

		OperationResult<ColumnList<String>> query = null;
		try {
			query = getLogKeyspace().prepareQuery(this.accessColumnFamily(columnFamilyName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key).execute();

		} catch (Exception e) {
			InsightsLogger.error(e);
		}

		return query;
	}

	/**
	 * Read record passing multiple keys
	 * 
	 * @param columnFamilyName
	 * @param key
	 */
	public OperationResult<Rows<String, String>> read(String columnFamilyName, Collection<String> keys) {

		OperationResult<Rows<String, String>> query = null;
		try {
			query = getLogKeyspace().prepareQuery(this.accessColumnFamily(columnFamilyName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKeySlice(keys).execute();

		} catch (Exception e) {
			InsightsLogger.error(e);
		}
		return query;
	}
	
	public HashMap<String,String> getMonitorEventProperty() {
		
		Rows<String, String> operators = null;
		HashMap<String,String> operatorsObject = new HashMap<String,String>();
		try {
			operators = getLogKeyspace().prepareQuery(this.accessColumnFamily("monitor_events"))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
					.getAllRows()
					.withColumnRange(new RangeBuilder().setMaxSize(10).build())
			        .setExceptionCallback(new ExceptionCallback() {
			             @Override
			             public boolean onException(ConnectionException e) {
			                 try {
			                     Thread.sleep(1000);
			                 } catch (InterruptedException e1) {
			                 }
			                 return true;
			             }})
			        .execute().getResult();
		} catch (Exception e) {
			InsightsLogger.error(e);
		}
		for (Row<String, String> row : operators) {
				operatorsObject.put(row.getKey(), row.getColumns().getStringValue("property", null));
			}
		return operatorsObject;
		
	}
	
	public ColumnList<String> getDashBoardKeys(String key) {

		ColumnList<String> jobConstants = null;
		try {
			jobConstants = getLogKeyspace().prepareQuery(this.accessColumnFamily("job_config_settings")).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
			.getKey(key).execute().getResult();
		} catch (Exception e) {
			InsightsLogger.error(e);
		}
		
		return jobConstants;
		
	}
	
	public List<Map<String, Object>> getRangeRowCount(String columnFamilyName, String startTime, String endTime, String eventName) {

		int query = 0;
		try {
			query = getLogKeyspace().prepareQuery(this.accessColumnFamily(columnFamilyName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex().addExpression().whereColumn("start_time")
					.lessThanEquals().value(endTime).addExpression().whereColumn("end_time").greaterThanEquals().value(startTime).execute().getResult().size();

		} catch (Exception e) {
			InsightsLogger.error(e);
		}
		Map<String, Object> map = new HashMap<String, Object>();
		map.put(eventName, query);
		List<Map<String, Object>> listMap = new ArrayList<Map<String, Object>>();
		listMap.add(map);
		return listMap;
	}

	/**
	 * Read Record given row key and Querying for Specific Columns in a row
	 */

	public OperationResult<ColumnList<String>> read(String columnFamilyName, String key, Collection<String> columnList) {
		RowQuery<String, String> query = null;
		OperationResult<ColumnList<String>> queryResult = null;

		try {
			query = getLogKeyspace().prepareQuery(this.accessColumnFamily(columnFamilyName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key);
			if (CollectionUtils.isNotEmpty(columnList)) {
				query.withColumnSlice(columnList);
			}
			queryResult = query.execute();
		} catch (Exception e) {
			InsightsLogger.error(e);
		}

		return queryResult;
	}

	/**
	 * Read record querying for
	 * 
	 * @param columnFamilyName
	 * @param value
	 *            = where condition value
	 * @return key
	 */

	public OperationResult<Rows<String, String>> read(String columnFamilyName, String column, String value, Collection<String> columnList) {

		IndexQuery<String, String> Columns = null;

		OperationResult<Rows<String, String>> query = null;

		try {
			Columns = getLogKeyspace().prepareQuery(this.accessColumnFamily(columnFamilyName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex().addExpression().whereColumn(column).equals()
					.value(value);
			if (CollectionUtils.isNotEmpty(columnList)) {
				Columns.withColumnSlice(columnList);
			}
			query = Columns.execute();

		} catch (Exception e) {
			InsightsLogger.error(e);
		}
		return query;

	}

	public int getColumnCount(String columnFamilyName, String key) {

		OperationResult<Integer> query = null;
		try {
			query = getLogKeyspace().prepareQuery(this.accessColumnFamily(columnFamilyName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKey(key).getCount().execute();
			return query.getResult().intValue();
		} catch (Exception e) {
			InsightsLogger.error(e);
		}
		return 0;
	}

	public int getRowCount(String columnFamilyName, Map<String, Object> whereCondition, Collection<String> columnList,int retryCount) {

		IndexQuery<String, String> Columns = null;

		try {
			Columns = getLogKeyspace().prepareQuery(this.accessColumnFamily(columnFamilyName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex();
			if (!whereCondition.isEmpty()) {
				for (Map.Entry<String, Object> map : whereCondition.entrySet()) {
					if (map.getValue() instanceof String) {
						Columns.addExpression().whereColumn(map.getKey()).equals().value(String.valueOf(map.getValue()));
					} else if (map.getValue() instanceof Integer) {
						Columns.addExpression().whereColumn(map.getKey()).equals().value(Integer.valueOf(map.getValue().toString()));
					} else if (map.getValue() instanceof Long) {
						Columns.addExpression().whereColumn(map.getKey()).equals().value(Long.valueOf(map.getValue().toString()));
					}
				}
			}
			if (CollectionUtils.isNotEmpty(columnList)) {
				Columns.withColumnSlice(columnList);
			}
			return Columns.execute().getResult().size();
		} catch (Exception e) {
			if(retryCount < 5){
				InsightsLogger.error(e);
                try {
                        Thread.sleep(3);
                        retryCount++;
                } catch (InterruptedException e1) {
        			InsightsLogger.error(e);
                }
                return getRowCount(columnFamilyName, whereCondition, columnList ,retryCount);
	        }

		}
		return 0;

	}

	public OperationResult<Rows<String, String>> read(String columnFamilyName, String column, String value, int retryCount) {

		OperationResult<Rows<String, String>> Columns = null;
		try {
			Columns = getLogKeyspace()
					.prepareQuery(this.accessColumnFamily(columnFamilyName))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.withRetryPolicy(new ConstantBackoff(2000, 5))
					.searchWithIndex().addExpression().whereColumn(column)
					.equals().value(value).execute();

		} catch (Exception e) {
			if (retryCount < 6) {
				InsightsLogger.error(e);
				try {
					Thread.sleep(3);
					retryCount++;
				} catch (InterruptedException e1) {

					InsightsLogger.error(e);
				}
				return read(columnFamilyName, column, value,
						retryCount);
			}
		}
		return Columns;
	}

	public OperationResult<Rows<String, String>> read(String columnFamilyName, String column, int value, int retryCount) {

		OperationResult<Rows<String, String>> Columns = null;
		try {
			Columns = getLogKeyspace()
					.prepareQuery(this.accessColumnFamily(columnFamilyName))
					.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL)
					.withRetryPolicy(new ConstantBackoff(2000, 5))
					.searchWithIndex().addExpression().whereColumn(column)
					.equals().value(value).execute();

		} catch (Exception e) {
			if (retryCount < 6) {
				InsightsLogger.error(e);
				try {
					Thread.sleep(3);
					retryCount++;
				} catch (InterruptedException e1) {

					InsightsLogger.error(e);
				}
				return read(columnFamilyName, column, value,retryCount);
			}
		}
		return Columns;
	}
	
	/*
	 * Read All rows given columName alone withColumnSlice(String... columns)
	 */
	public OperationResult<Rows<String, String>> readAll(String columnFamilyName, String column,int retryCount) {

		OperationResult<Rows<String, String>> queryResult = null;
		try {

			queryResult = getLogKeyspace().prepareQuery(this.accessColumnFamily(columnFamilyName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getAllRows().withColumnSlice(column).execute();
		} catch (Exception e) {
			 if(retryCount < 6){
					InsightsLogger.error(e);
					
                 try {
                         Thread.sleep(3);
                         retryCount++;
                 } catch (InterruptedException e1) {
         			InsightsLogger.error(e);
                 }
                 return readAll(columnFamilyName,column,retryCount);
	         }
		}
		return queryResult;
	}

	/*
	 * Read All rows given columName alone withRowSlice(String... Rows)
	 */
	public OperationResult<Rows<String, String>> readAll(String columnFamilyName, Collection<String> keys, Collection<String> columns,int retryCount) {

		OperationResult<Rows<String, String>> queryResult = null;

		try {
			RowSliceQuery<String, String> Query = getLogKeyspace().prepareQuery(this.accessColumnFamily(columnFamilyName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).getKeySlice(keys);
			if (CollectionUtils.isNotEmpty(columns)) {
				Query.withColumnSlice(columns);
			}
			queryResult = Query.execute();

		} catch (Exception e) {
			if (retryCount < 6) {
				InsightsLogger.error(e);
				try {
					Thread.sleep(3);
					retryCount++;
				} catch (InterruptedException e1) {
					InsightsLogger.error(e);
				}
				return readAll(columnFamilyName, keys, columns, retryCount);
			}

		}
		return queryResult;

	}

	public OperationResult<Rows<String, String>> readAll(String columnFamily, String whereColumn, String columnValue, Collection<String> columns,int retryCount) {
		OperationResult<Rows<String, String>> queryResult = null;
		IndexQuery<String, String> indexQuery = null;
		try {

			ColumnFamilyQuery<String, String> Query = getLogKeyspace().prepareQuery(this.accessColumnFamily(columnFamily)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

			if (!whereColumn.isEmpty()) {

				indexQuery = Query.searchWithIndex().addExpression().whereColumn(whereColumn).equals().value(columnValue);
			}
			if (CollectionUtils.isNotEmpty(columns)) {
				indexQuery.withColumnSlice(columns);
			}
			queryResult = indexQuery.execute();

		} catch (Exception e) {
			 if(retryCount < 6){
					InsightsLogger.error(e);
                 try {
                         Thread.sleep(3);
                         retryCount++;
                 } catch (InterruptedException e1) {
         			InsightsLogger.error(e);
                 }
                 return readAll(columnFamily, whereColumn, columnValue, columns,retryCount);
         }

		}
		return queryResult;
	}

	public OperationResult<Rows<String, String>> readAll(String columnFamilyName, Map<String, Object> whereColumn, Collection<String> columnSclice,int retryCount) {

		OperationResult<Rows<String, String>> queryResult = null;
		IndexQuery<String, String> indexQuery = null;
		try {
			indexQuery = getLogKeyspace().prepareQuery(this.accessColumnFamily(columnFamilyName)).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5)).searchWithIndex();
			for (Map.Entry<String, Object> map : whereColumn.entrySet()) {
				if (map.getValue() instanceof String) {
					indexQuery.addExpression().whereColumn(map.getKey()).equals().value(String.valueOf(map.getValue()));
				} else if (map.getValue() instanceof Integer) {
					indexQuery.addExpression().whereColumn(map.getKey()).equals().value(Integer.valueOf(map.getValue().toString()));

				} else if (map.getValue() instanceof Long) {
					indexQuery.addExpression().whereColumn(map.getKey()).equals().value(Long.valueOf(map.getValue().toString()));

				}
			}
			if (CollectionUtils.isNotEmpty(columnSclice)) {
				indexQuery.withColumnSlice(columnSclice);
			}
			queryResult = indexQuery.execute();
		} catch (Exception e) {
			 if(retryCount < 6){
					InsightsLogger.error(e);
                 try {
                         Thread.sleep(3);
                         retryCount++;
                 } catch (InterruptedException e1) {
         			InsightsLogger.error(e);
                 }
                 return readAll(columnFamilyName, whereColumn, columnSclice,retryCount);
	         }
		}
		return queryResult;
	}

	public void addRowKeyValues(String cfName,String keyName,Map<String,Object> data){
		MutationBatch m = getLogKeyspace().prepareMutationBatch()
				.setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));

		for (String column : data.keySet()) {
			m.withRow(this.accessColumnFamily(cfName), keyName)
					.putColumnIfNotNull(column,
							String.valueOf(data.get(column)));
		}
		try {
			m.execute();
		} catch (Exception e) {
			InsightsLogger.error(e);
		}
	}
	
	public void incrementCounterValues(String cfName,String keyName,Map<String,Object> data) {
		MutationBatch m = getLogKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
		for (String column : data.keySet()) {
		m.withRow(this.accessColumnFamily(cfName), keyName)
				.incrementCounterColumn(column, Long.valueOf(String.valueOf(data.get(column))));
		}
		try {
			m.execute();
		} catch (Exception e) {
			InsightsLogger.error(e);
		}
	}
	
	public void saveProfileSettings(String cfName,String keyName,String columnName,String data) {
		MutationBatch m = getLogKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
		m.withRow(this.accessColumnFamily(cfName), keyName)
				.putColumnIfNotNull(columnName, data);
		try {
			m.execute();
		} catch (Exception e) {
			InsightsLogger.error(e);
		}
	}
	
	public void saveDefaultProfileSettings(String cfName, String keyName,String column, String value) {
		MutationBatch m = getLogKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
		m.withRow(this.accessColumnFamily(cfName), keyName)
				.putColumnIfNotNull(column, value);
		try {
			m.execute();
		} catch (Exception e) {
			InsightsLogger.error(e);
		}
	}
	
	public void deleteRowKey(String cfName,String keyName) {
		MutationBatch m = getLogKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
		for (String key : keyName.split(",")) {
			m.withRow(this.accessColumnFamily(cfName), key).delete();
		}
		try {
			m.execute();
		} catch (Exception e) {
			InsightsLogger.error(e);
		}
	}
	
	public void deleteColumnInRow(String cfName,String keyName,String columnName) {
		MutationBatch m = getLogKeyspace().prepareMutationBatch().setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5));
		for(String column : columnName.split(",")) {
			m.withRow(this.accessColumnFamily(cfName), keyName).deleteColumn(column);
		}
		try {
			m.execute();
		} catch (Exception e) {
			InsightsLogger.error(e);
		}
	}
	
	public boolean putStringValue(String columnFamily,String key,Map<String,String> columns){
		
		try{
			MutationBatch mutationBatch = getLogKeyspace().prepareMutationBatch();
			for(Map.Entry<String, String> entry : columns.entrySet()){
				mutationBatch.withRow(this.accessColumnFamily(columnFamily), key).putColumn(entry.getKey(), entry.getValue());
				mutationBatch.execute();
			}
			return true;
		}catch(Exception e){
			InsightsLogger.error(e);
		}
		return false;
	}
	
	public ColumnList<String> getConfigKeys(String key) {

		ColumnList<String> jobConstants = null;
		try {
			jobConstants = getLogKeyspace().prepareQuery(this.accessColumnFamily("job_config_settings")).setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL).withRetryPolicy(new ConstantBackoff(2000, 5))
			.getKey(key).execute().getResult();
		} catch (ConnectionException e) {
			InsightsLogger.error(e);
		}
		
		return jobConstants;
		
	}
}
