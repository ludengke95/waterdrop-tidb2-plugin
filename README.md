# waterdrop-tidb2-plugin
可能是由于 waterdrop 的作者比较忙吧，waterdrop 的 input 中的 tidb 插件用到的 tispark 依赖包还是 1.1 版本的。导致读取 spark2.3 读取 tidb3.0 数据失败(tidb2.0 没测，估计应该可以吧，如果哪位测了话，可以告诉我一下，原版的1.4.2读tidb2.0的行不。)

### TIDB
  3.0 版本的要想用 spark 就得用 spark2.3+ ，并且 tispark2.0+。
### waterdrop
  1.4.2 的版本用的是 spark2.3，要想读取 tidb 数据，得用tispark2.0+，并且 tidb3.0+
  
### 升级
  所以三个都要升级到对应版本，要求 waterdrop-1.4.2，tispark2.0+，tidb3.0+
  
### 使用
  1. 将Waterdrop-1.4.2-tispark2.2.jar包下载下来，替换原版的1.4.2版本lib下的jar包 https://github.com/ludengke95/waterdrop-tidb2-plugin/releases
  2. 将waterdrop-tidb2-plugin.jar按照原来作者插件的形式放到plugins里面。
  3. 在配置文件的input中，使用插件
  ```
  intput{
    warterdrop.input.batch.Tidb{
      database = "hive_data_warehouse"
      pre_sql = "select * from test"
      result_table_name = "test"
    }
  }
  ```
