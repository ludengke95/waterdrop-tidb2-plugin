package io.github.interestinglab.waterdrop.input.batch;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.interestinglab.waterdrop.apis.BaseStaticInput;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author ludengke
 * @date 2020/1/15
 **/
public class JdbcPartitionInput extends BaseStaticInput {

	Config config = ConfigFactory.empty();

	public JdbcPartitionInput() {
	}

	@Override
	public Dataset<Row> getDataset(SparkSession spark) {
		String ids = config.getString("partition.id");
		String[] idArray = ids.split(",");
		List<Map> list = new Gson().fromJson(config.getString("partition.id.param"), new TypeToken<List<Map>>(){}.getType());
		ArrayList<String> partitionQuery = new ArrayList<>();
		String[] s = new String[]{};
		if(list !=null && list.size()>0){
			for (Map paramMap : list) {
				StringBuffer sb = new StringBuffer();
				sb.append(" 1 = 1 ");
				for (String id : idArray) {
					sb.append(" and "+id+" = '"+paramMap.get(id)+"' ");
				}
				partitionQuery.add(sb.toString());
			}
		}
		else {
			String url = config.getString("url");
			Connection connection = null;
			try {
				connection = DriverManager.getConnection(url, config.getString("user"), config.getString("password"));
				QueryRunner qr = new QueryRunner();
				List<Map<String, Object>> distincts = qr.query(connection, String.join(" ", "select distinct", ids, "from", config.getString("table")), new MapListHandler());
				for (Map<String, Object> distinct: distincts){
					StringBuffer sb = new StringBuffer();
					sb.append(" 1 = 1 ");
					for (String id : idArray) {
						if(distinct.get(id) == null){
							sb.append(" and "+id+" is null");
						}else {
							sb.append(" and "+id+" = '"+distinct.get(id)+"' ");
						}
					}
					partitionQuery.add(sb.toString());
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			finally {
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}

		}
		return queryDataSetWithDB(spark, partitionQuery.toArray(s));
	}

	@Override
	public void setConfig(Config config) {
		this.config = config;
	}

	@Override
	public Config getConfig() {
		return config;
	}

	@Override
	public Tuple2<Object, String> checkConfig() {
		boolean idFlag = config.hasPath("partition.id");
		boolean paramFlag = config.hasPath("partition.id.param");
		boolean url = config.hasPath("url");
		boolean table = config.hasPath("table");
		boolean resultTableName = config.hasPath("result_table_name");
		boolean user = config.hasPath("user");
		boolean password = config.hasPath("password");
		boolean driver = config.hasPath("driver");
		if(url && table && resultTableName && user && password && driver){
			if(!idFlag){
				return new Tuple2<>(false,"partition.id is necessary");
			}else {
				if (paramFlag){
					String ids = config.getString("partition.id");
					String[] idArray = ids.split(",");
					List<Map> list = new Gson().fromJson(config.getString("partition.id.param"), new TypeToken<List<Map>>(){}.getType());
					boolean flag = true;
					if(list !=null && list.size()>0){
						for (Map paramMap : list) {
							for (String id : idArray) {
								if (!paramMap.containsKey(id)){
									flag = false;
									break;
								}
							}
							if (!flag){
								break;
							}
						}
					}
					if(flag){
						return new Tuple2<>(true,"");
					}else {
						return new Tuple2<>(false,"partition.id.param字符串中有子元素不包含partition.id");
					}
				}else {
					return new Tuple2<>(true,"");
				}
			}
		}
		else {
			return new Tuple2<>(false,"url,table,result_table_name,user,password is necessary");
		}
	}

	/**
	 * 获取数据库中的表数据
	 *
	 * @return Dataset<Row> 列名会从下划线转为驼峰
	 */
	public Dataset<Row> queryDataSetWithDB (SparkSession session,String[] partitions) {
		Properties properties = new Properties();
		properties.setProperty("user", config.getString("user"));
		properties.setProperty("password", config.getString("password"));
		properties.setProperty("driver", config.getString("driver"));
		Dataset<Row> dataset;
		if(partitions==null){
			dataset = session.read()
					.jdbc(config.getString("url"), config.getString("table"), properties);
		}
		else {
			dataset = session.read()
					.jdbc(config.getString("url"), config.getString("table"), partitions,properties);
		}
		return dataset;
	}
}
