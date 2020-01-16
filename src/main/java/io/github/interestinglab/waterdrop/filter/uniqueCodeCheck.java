package io.github.interestinglab.waterdrop.filter;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.interestinglab.waterdrop.apis.BaseFilter;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * @author ludengke
 * @date 2020/1/15
 **/
public class uniqueCodeCheck extends BaseFilter {

	public uniqueCodeCheck() {
	}

	private Config config = ConfigFactory.empty();

	@Override
	public Config getConfig() {
		return config;
	}

	@Override
	public void setConfig(Config config) {
		this.config = config;
	}

	@Override
	public Tuple2<Object, String> checkConfig() {
		if (!config.hasPath("field")) {
			return new Tuple2<>(false, "please specify [field]");
		}
		return new Tuple2<>(true, "");
	}

	@Override
	public Dataset<Row> process(SparkSession spark, Dataset<Row> df) {
		return df.filter((FilterFunction<Row>) value -> {
			String code = value.getString(value.fieldIndex(config.getString("field")));
			if (code != null && !"".equals(code.trim()) && !code.matches("^[0-9]*$")){
				return false;
			}
			Integer bit1 = Integer.valueOf(String.valueOf(code.charAt(0)));
			Integer bit2 = Integer.valueOf(String.valueOf(code.charAt(1)));
			Integer bit3 = Integer.valueOf(String.valueOf(code.charAt(2)));
			Integer bit4 = Integer.valueOf(String.valueOf(code.charAt(3)));
			Integer bit5 = Integer.valueOf(String.valueOf(code.charAt(4)));
			Integer bit6 = Integer.valueOf(String.valueOf(code.charAt(5)));
			Integer bit7 = Integer.valueOf(String.valueOf(code.charAt(6)));
			Integer bit8 = Integer.valueOf(String.valueOf(code.charAt(7)));
			Integer bit9 = Integer.valueOf(String.valueOf(code.charAt(8)));
			Integer bit10 = Integer.valueOf(String.valueOf(code.charAt(9)));
			Integer bit11 = Integer.valueOf(String.valueOf(code.charAt(10)));
			Integer bit12 = Integer.valueOf(String.valueOf(code.charAt(11)));
			Integer bit13 = Integer.valueOf(String.valueOf(code.charAt(12)));
			int t1 = (bit2 + bit4 + bit6 + bit8 + bit10 + bit12) * 3;
			int t2 = (bit1 + bit3 + bit5 + bit7 + bit9 + bit11);
			int t3 = (10 - (t1+t2)%10)%10;
			if(bit13 == t3){
				return true;
			}else {
				return false;
			}
		});
	}
}
