package org.apache.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.google.protobuf.ServiceException;

/**
 * Hello world!
 *
 */
public class App 
{
    private static final String TABLE_NAME = "mytable";
    private static final String CF_DEFAULT = "cf";
    public static final byte[] QUALIFIER = "col1".getBytes();
    private static final byte[] ROWKEY = "rowkey1".getBytes();


	public static void main(String[] args) throws IOException, ServiceException {
		Logger.getRootLogger().setLevel(Level.DEBUG);
		Configuration configuration = HBaseConfiguration.create();
		
		// Zookeeper quorum
		configuration.set("hbase.zookeeper.quorum", "slave1.test,slave3.test,slave2.test");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hadoop.security.authentication", "kerberos");
		configuration.set("hbase.security.authentication", "kerberos");
		configuration.set("hbase.cluster.distributed", "true");
		
		// check this setting on HBase side
		configuration.set("hbase.rpc.protection", "authentication"); 

		//what principal the master/region. servers use.
		configuration.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@EXAMPLE.COM"); 
		configuration.set("hbase.regionserver.keytab.file", "src/hbase.service.keytab"); 
		
		// this is needed even if you connect over rpc/zookeeper
		configuration.set("hbase.master.kerberos.principal", "hbase/_HOST@EXAMPLE.COM"); 
		configuration.set("hbase.master.keytab.file", "src/hbase.service.keytab");
		
		System.setProperty("java.security.krb5.conf","src/krb5.conf");
		// Enable/disable krb5 debugging 
		System.setProperty("sun.security.krb5.debug", "false");

		String principal = System.getProperty("kerberosPrincipal","hbase/slave1.test@EXAMPLE.COM");
		String keytabLocation = System.getProperty("kerberosKeytab","src/hbase.service.keytab");

		// kinit with principal and keytab
		UserGroupInformation.setConfiguration(configuration);
		UserGroupInformation.loginUserFromKeytab(principal, keytabLocation);
		
		Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create(configuration));
		System.out.println(connection.getAdmin().isTableAvailable(TableName.valueOf("defaults:mytable")));
		
		//dba to me create table
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        tableDescriptor.addFamily(new HColumnDescriptor(CF_DEFAULT));
        System.out.print("Creating table. ");
        Admin admin = connection.getAdmin();
        admin.createTable(tableDescriptor);
        System.out.println(" Done.");
        
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Put put = new Put(ROWKEY);
        put.addColumn(CF_DEFAULT.getBytes(), QUALIFIER, "this is value".getBytes());
        table.put(put);
        Get get = new Get(ROWKEY);
        Result r = table.get(get);
        byte[] b = r.getValue(CF_DEFAULT.getBytes(), QUALIFIER);  // returns current version of value
        System.out.println(new String(b));
        System.out.println(connection.getAdmin().isTableAvailable(TableName.valueOf("mytable")));
	}
}
