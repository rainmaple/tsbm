## Secondary development  document

***  clone or download this project **

####  How to test the  new database ?

1. First, you need to install the db server in os (linux such as centos 7)

2. create the java class in the package ```cn.edu.ruc.adapter``` and implement this interface (```cn.edu.ruc.adapter.DBAdapter```)
   and implement related methods

3. about the related methods, here for example:

```java
 /**
   * @method initDataSource
   * you need to config the database, user,password,ip,port,etc.
   */
	public void initDataSource(TsDataSource ds, TsParamConfig tspc);
	/**
	 * @method preWrite
	 * create sql batch for points formated by cn.edu.ruc.base.TsPackage
	 */
	public Object preWrite(TsWrite tsWrite);
	/**
	 * @method excWrite
	 * excute the sql batch created by preWrite
	 */
	public Status execWrite(Object write);
	/**
	 * @method initDataSource
	 * shift the query to sql 
	 */
	public Object preQuery(TsQuery tsQuery);
	/**
	 * @method execQuery
	 * excute the sql created by preQuery
	 */
	public Status execQuery(Object query);
	/**
	 * @method closeAdapter
	 * shutdown the adpter
	 */
	public void closeAdapter();
```

 

#### Test the new Database

read [this document](https://github.com/dbiir/ts-benchmark/blob/master/README.md)  run this test tool.