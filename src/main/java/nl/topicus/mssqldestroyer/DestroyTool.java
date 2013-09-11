package nl.topicus.mssqldestroyer;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class DestroyTool {
	static Logger log = Logger.getLogger(DestroyTool.class);
	
	protected Options options;
	
	protected CommandLine cmd;
	
	protected File credFile;
	
	protected String user;
	
	protected String password;
	
	protected String database;
	
	protected String server;
	
	protected String instance;

	protected Connection conn;
		
	public DestroyTool () {
		options = new Options();
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Specify the credentials properties file");
		OptionBuilder.withLongOpt("credentials");
		options.addOption(OptionBuilder.create("c"));
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Specify the username of the database user");
		OptionBuilder.withLongOpt("user");
		options.addOption(OptionBuilder.create("u"));
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(false);
		OptionBuilder.withDescription("Specify the password of the database user");
		OptionBuilder.withLongOpt("password");
		options.addOption(OptionBuilder.create("p"));
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(true);
		OptionBuilder.withLongOpt("database");
		OptionBuilder.withDescription("Specify the name of the database to destroy");
		options.addOption(OptionBuilder.create("d"));
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(false);
		OptionBuilder.withLongOpt("instance");
		OptionBuilder.withDescription("Specify the name of the instance to use on the SQL server");
		options.addOption(OptionBuilder.create("i"));
		
		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired(true);
		OptionBuilder.withLongOpt("server");
		OptionBuilder.withDescription("Specify the hostname or IP address of the SQL server");
		options.addOption(OptionBuilder.create("s"));
	}
	
	public String quoteIdentifier (String name) {
		name = name.replaceAll("\"",  "\\\\\"");
		name = "\"" + name + "\"";
		return name;
	}
	
	public String quoteValue (String value) {
		value = value.replaceAll("'", "\\\\'");
		value = "'" + value + "'";
		return value;
	}
	
	public Options getOptions () {
		return this.options;
	}
	
	public void run (String[] args) throws Exception {
		parseArgs(args);		
		
		openConnection();
		
		// find databases to DROP (name may include a wildcard)
		ArrayList<String> dbList = findDatabases();
		
		if (dbList.size() == 0) {
			log.info("Found no databases to drop");
			log.info("Finished");
			return;
		} else if (dbList.size() == 1) {
			log.info("Found 1 database to drop: " + dbList.get(0));
		} else {
			log.info("Found " + dbList.size() + " databases to drop: ");
			for(String db : dbList) {
				log.info(db);
			}
		}
		
		// DROP each database
		for(String db : dbList) {
			dropDatabase(db);
		}
				
		closeConnection();
		
		log.info("Finished!");
	}
	
	protected void dropDatabase (String db)  {
		log.info("Closing ALL connections to database " + quoteIdentifier(db) + "...");
		
		Statement q;
		try {
			q = conn.createStatement();
		} catch (SQLException e) {
			log.error("Unable to create Statement object");
			return;
		}
		
		try {
			q.execute("ALTER DATABASE " + quoteIdentifier(db) + " SET SINGLE_USER WITH ROLLBACK IMMEDIATE");
			log.info("All connections closed");
		} catch (SQLException e) {
			log.error("Unable to close all connections", e);
		}
		
		log.info("Dropping database " + quoteIdentifier(db) + "...");
		
		try {
			q.execute("DROP DATABASE " + quoteIdentifier(db));
			log.info("Database " + quoteIdentifier(db) + " dropped");
		} catch (SQLException e) {
			log.error("Unable to drop database", e);
		}	
		
		try {
			q.close();
		} catch (SQLException e) {
			// don't care
		}
	}
	
	protected ArrayList<String> findDatabases () throws SQLException {
		log.info("Checking which databases to drop...");
		
		ArrayList<String> dbList = new ArrayList<String>();
		
		PreparedStatement q = conn.prepareStatement("SELECT * FROM sysdatabases WHERE name LIKE ?");
		q.setString(1, this.database);
		
		ResultSet res = q.executeQuery();
		
		while(res.next()) {
			dbList.add(res.getString("name"));
		}
		
		log.info("Done checking");
		return dbList;
	}
	
		
	protected boolean databaseExists (String name) throws SQLException {
		PreparedStatement q = conn.prepareStatement("SELECT * FROM sysdatabases WHERE name = ?");
		q.setString(1,  name);
		
		ResultSet res = q.executeQuery();
		
		boolean ret = res.next();
		
		res.close();
		q.close();
		
		return ret;
	}
	
	
	
	protected void closeConnection () throws SQLException {
		if (conn != null && conn.isClosed() == false) {
			conn.close();
			log.info("Closed connection to SQL server");
		}		
	}
	
	protected void openConnection () throws SQLException, ClassNotFoundException {
		Class.forName("net.sourceforge.jtds.jdbc.Driver");
		
		Properties connProps = new Properties();
		
		if (user != null && password != null) {
			connProps.setProperty("user",  user);
			connProps.setProperty("password", password);
		}
		
		if(instance != null){
			connProps.setProperty("instance", instance);
		}
		
		String url = "jdbc:jtds:sqlserver://" + server + "/master";
		log.debug("Using connection URL for MS SQL Server: " + url);
		

		conn = DriverManager.getConnection(url, connProps);
		log.info("Opened connection to MS SQL Server");	
	}
	
	protected void parseArgs (String[] args) throws Exception {
		CommandLineParser parser = new BasicParser();
		cmd = parser.parse( options, args);
		
		// check for credentials
		if (StringUtils.isEmpty(cmd.getOptionValue("credentials")) == false) {
			File credFile = new File(cmd.getOptionValue("credentials"));
			
			if (credFile.exists() == false || credFile.canRead() == false) {
				throw new Exception("Unable to read credentials file '" + credFile.getPath() + "'");
			}
			
			Properties config = new Properties();
			config.load(new FileInputStream(credFile));
			
			if (config.containsKey("user")) {
				user = config.getProperty("user");
			}
			
			if (config.containsKey("password")) {
				password = config.getProperty("password");
			}
		}
		
		if (StringUtils.isEmpty(cmd.getOptionValue("user")) == false) {
			user = cmd.getOptionValue("user");
		}
		
		if (StringUtils.isEmpty(cmd.getOptionValue("password")) == false) {
			password = cmd.getOptionValue("password");
		}
		
		if (StringUtils.isEmpty(user) || StringUtils.isEmpty(password)) {
			throw new MissingCredentialsException("Missing credentials (user/password)");
		}
		
		server = cmd.getOptionValue("server");
		log.info("SQL server: " + server);
		
		log.info("User: " + user);
		
		instance = cmd.getOptionValue("instance");
		if (StringUtils.isEmpty(instance) == false) {
			log.info("Instance: " + instance);
		}
		
		database = cmd.getOptionValue("database");
		log.info("Database to destroy: " + database);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {		
		BasicConfigurator.configure();
		log.info("Started MSSQL-Destroyer tool");
		
		DestroyTool tool = new DestroyTool();
		
		try {
			tool.run(args);
		} catch (ParseException e) {
			System.err.println("ERROR: " + e.getMessage());
			System.out.println("");
			
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(" ", tool.getOptions());
			System.exit(1);
		} catch (Exception e) {
			log.fatal(e.getMessage(), e);
			System.exit(1);
		}		
		
		// clean exit
		System.exit(0);		
	}
	
	
	public class MissingCredentialsException extends Exception {
		private static final long serialVersionUID = 1L;

		public MissingCredentialsException(String string) {
			super(string);
		}
	}

}
