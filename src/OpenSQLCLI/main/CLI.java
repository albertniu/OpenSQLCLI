package OpenSQLCLI.main;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.util.Calendar;

import OpenSQLCLI.DatabaseConnector.MySQLConnector;
import OpenSQLCLI.DatabaseConnector.OracleConnector;

public class CLI
{
	static public String m_dbMySQL;
	static public String m_dbOracle;
	static public String m_DBMS;

	private static void doSelect(String sql)
	{
		ResultSet rs = null;

		try
		{
			if ( m_DBMS.equals ( "oracle" ) )
			{
				long begin = Calendar.getInstance ().getTimeInMillis ();
				rs = OracleConnector.select ( sql );
				long end = Calendar.getInstance ().getTimeInMillis ();
				OracleConnector.printResult ( rs, sql.split ( " " )[3], (end - begin) / 1000.0 );
			}
			else if ( m_DBMS.equals ( "mysql" ) )
			{
				long begin = Calendar.getInstance ().getTimeInMillis ();
				rs = MySQLConnector.select ( sql );
				long end = Calendar.getInstance ().getTimeInMillis ();
				MySQLConnector.printResult ( rs, sql.split ( " " )[3], (end - begin) / 1000.0 );
			}
		}
		catch ( Exception ex )
		{
			ex.printStackTrace ();
		}
		finally
		{
			if ( m_DBMS.equals ( "oracle" ) )
			{
				OracleConnector.release ( rs );
			}
			else if ( m_DBMS.equals ( "mysql" ) )
			{
				MySQLConnector.release ( rs );
			}
		}
	}

	private static void execSQL(String sql)
	{
		if ( m_DBMS.equals ( "oracle" ) )
		{
			if ( sql.startsWith ( "insert" ) )
				OracleConnector.insert ( sql );
			else if ( sql.startsWith ( "update" ) )
				OracleConnector.update ( sql );
			else if ( sql.startsWith ( "delete" ) )
				OracleConnector.delete ( sql );
			else
				System.out.println ( "You have an error in your SQL syntax" );
		}
		else if ( m_DBMS.equals ( "mysql" ) )
		{
			if ( sql.startsWith ( "insert" ) )
				MySQLConnector.insert ( sql );
			else if ( sql.startsWith ( "update" ) )
				MySQLConnector.update ( sql );
			else if ( sql.startsWith ( "delete" ) )
				MySQLConnector.delete ( sql );
			else
				System.out.println ( "You have an error in your SQL syntax" );
		}
	}

	public static void main(String[] args)
	{
		try
		{
			if ( args.length < 1 )
			{
				String usage = "";
				usage += "Usage: java -jar opensqlcli.jar url\n";
				usage += "For instance:\n";
				usage += "java -Dfile.encoding=UTF-8 -jar opensqlcli.jar \"mysql://127.0.0.1:3306/test?user=test&password=test1234\"\n";
				usage += "java -Dfile.encoding=UTF-8 -jar opensqlcli.jar \"oracle://127.0.0.1:1521/orcl?user=test&password=test1234\"";
				System.out.println ( usage );
				return;
			}

			String url = args[0];

			if ( url.startsWith ( "oracle://" ) )
			{
				Class.forName ( "oracle.jdbc.driver.OracleDriver" );
				m_DBMS = "oracle";
				m_dbOracle = url;
				OracleConnector.getConnection ();
			}
			else if ( url.startsWith ( "mysql://" ) )
			{
				Class.forName ( "com.mysql.jdbc.Driver" );
				m_DBMS = "mysql";
				m_dbMySQL = url;
				MySQLConnector.getConnection ();
			}
			else
			{
				System.out.println ( "This DBMS is not supported." );
				return;
			}

			BufferedReader br = new BufferedReader ( new InputStreamReader ( System.in, "UTF-8" ) );
			String line;

			while ( true )
			{
				System.out.print ( m_DBMS + ">" );
				line = br.readLine ();

				if ( line.length () > 0 && line.endsWith ( ";" ) )
					line = line.substring ( 0, line.length () - 1 );

				if ( line.toLowerCase ().equals ( "exit" ) || line.toLowerCase ().equals ( "quit" ) )
					break;

				if ( line.equals ( "" ) )
					continue;

				if ( line.toLowerCase ().startsWith ( "select" ) )
					doSelect ( line );
				else
					execSQL ( line );
			}
		}
		catch ( Exception ex )
		{
			ex.printStackTrace ();
		}
	}
}
