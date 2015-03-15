package OpenSQLCLI.DatabaseConnector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;

import OpenSQLCLI.main.CLI;

public class OracleConnector
{
	static private String m_ConnString = CLI.m_dbOracle;
	static private Connection m_conn = null;

	static public boolean getConnection()
	{
		try
		{
			if ( !isValid () )
			{
				int begin = m_ConnString.indexOf ( "//" ) + 2;
				int end = m_ConnString.indexOf ( "/", begin );
				String host = m_ConnString.substring ( begin, end );

				begin = end + 1;
				end = m_ConnString.indexOf ( "?", begin );
				String dbname = m_ConnString.substring ( begin, end );

				begin = m_ConnString.indexOf ( "user=" ) + 5;
				end = m_ConnString.indexOf ( "&", begin );
				String username = m_ConnString.substring ( begin, end );

				begin = m_ConnString.indexOf ( "password=" ) + 9;
				String password = m_ConnString.substring ( begin );

				String oracle = "jdbc:oracle:thin:@" + host + ":" + dbname;
				m_conn = null;
				m_conn = DriverManager.getConnection ( oracle, username, password );
			}

			return (m_conn != null && !m_conn.isClosed ());
		}
		catch ( Exception ex )
		{
			System.out.println ( ex.toString () );
			return false;
		}
	}

	static private boolean isValid()
	{
		try
		{
			if ( m_conn == null || m_conn.isClosed () )
				return false;

			return true;
		}
		catch ( Exception ex )
		{
			System.out.println ( ex.toString () );
		}

		return false;
	}

	static public void release(ResultSet rs)
	{
		try
		{
			if ( rs != null )
			{
				rs.getStatement ().close ();
				rs.close ();
			}
		}
		catch ( Exception ex )
		{
			System.out.println ( ex.toString () );
		}
	}

	static public void printResult(ResultSet rs, String tableName, double queryTime)
	{
		if ( rs == null )
			return;

		try
		{
			ResultSetMetaData meta = rs.getMetaData ();
			int cols = meta.getColumnCount ();
			int[] colSize = new int[cols];
			String[] colName = new String[cols];

			for ( int i = 1; i <= cols; ++i )
			{
				colName[i - 1] = meta.getColumnName ( i );
				colSize[i - 1] = rs.getMetaData ().getColumnDisplaySize ( i );
			}

			for ( int i = 1; i <= cols; ++i )
			{
				rs.beforeFirst ();
				int cn = (colName[i - 1].getBytes ().length - colName[i - 1].length ()) / 2;
				int en = colName[i - 1].length () - cn;
				int len = (int) (cn * 1.5 + en);
				int maxlen = len;

				while ( rs.next () )
				{
					if ( meta.getColumnTypeName ( i ).contains ( "INT" ) )
					{
						int colValue = rs.getInt ( i );
						String strValue = String.valueOf ( colValue );
						len = strValue.length ();

						if ( len > maxlen )
							maxlen = len;
					}
					else
					{
						String colValue = rs.getString ( i );
						colValue = colValue == null ? " " : colValue;

						cn = (colValue.getBytes ().length - colValue.length ()) / 2;
						en = colValue.length () - cn;
						len = (int) (cn * 1.5 + en);

						if ( len > maxlen )
							maxlen = len;
					}
				}

				colSize[i - 1] = maxlen;
			}

			System.out.print ( "+" );
			{
				for ( int i = 1; i <= cols; ++i )
				{
					for ( int j = 0; j < colSize[i - 1] + 6; ++j )
						System.out.print ( "-" );
				}
			}

			System.out.print ( "+\n" );

			for ( int i = 1; i <= cols; ++i )
			{
				System.out.print ( "|" );
				System.out.print ( " " );
				System.out.print ( colName[i - 1] );

				int cn = (colName[i - 1].getBytes ().length - colName[i - 1].length ()) / 2;
				int en = colName[i - 1].length () - cn;
				int len = (int) (cn * 1.5 + en);

				for ( int j = len; j < colSize[i - 1]; ++j )
					System.out.print ( " " );

				System.out.print ( "\t" );
			}

			System.out.print ( "|\n" );

			System.out.print ( "+" );
			{
				for ( int i = 1; i <= cols; ++i )
				{
					for ( int j = 0; j < colSize[i - 1] + 6; ++j )
						System.out.print ( "-" );
				}
			}

			System.out.print ( "+\n" );
			int count = 0;
			rs.beforeFirst ();

			while ( rs.next () )
			{
				count += 1;

				for ( int i = 1; i <= cols; ++i )
				{
					if ( meta.getColumnTypeName ( i ).contains ( "INT" ) )
					{
						System.out.print ( "|" );
						int colValue = rs.getInt ( i );
						String strValue = String.valueOf ( colValue );

						System.out.print ( " " );
						System.out.print ( strValue );

						for ( int j = strValue.length (); j < colSize[i - 1]; ++j )
							System.out.print ( " " );

						System.out.print ( "\t" );
					}
					else
					{
						System.out.print ( "|" );
						String colValue = rs.getString ( i );
						colValue = colValue == null ? " " : colValue;

						System.out.print ( " " );
						System.out.print ( colValue );

						int cn = (colValue.getBytes ().length - colValue.length ()) / 2;
						int en = colValue.length () - cn;
						int len = (int) (cn * 1.5 + en);

						for ( int j = len; j < colSize[i - 1]; ++j )
							System.out.print ( " " );

						System.out.print ( "\t" );
					}
				}

				System.out.print ( "|\n" );
			}

			System.out.print ( "+" );
			{
				for ( int i = 1; i <= cols; ++i )
				{
					for ( int j = 0; j < colSize[i - 1] + 6; ++j )
						System.out.print ( "-" );
				}
			}

			System.out.print ( "+\n" );

			if ( count > 0 )
				System.out.print ( String.format ( "%d rows in set (%.2fsec)\n", count, queryTime ) );
			else
				System.out.print ( String.format ( "Empty set (%.2fsec)\n", queryTime ) );
		}
		catch ( Exception ex )
		{
			System.out.println ( ex.getMessage () );
		}
	}

	static public ResultSet select(String sql)
	{
		if ( !getConnection () )
			return null;

		Statement stmt = null;
		ResultSet rs = null;

		try
		{
			stmt = m_conn.createStatement ( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE );
			rs = stmt.executeQuery ( sql );
			return rs;
		}
		catch ( SQLException ex )
		{
			try
			{
				System.out.println ( "ERROR " + ex.getErrorCode () + ": " + ex.getMessage () );

				if ( stmt != null )
					stmt.close ();
			}
			catch ( Exception ex2 )
			{
				System.out.println ( ex2.getMessage () );
			}

			return null;
		}
	}

	static public boolean insert(String sql)
	{
		long begin = Calendar.getInstance ().getTimeInMillis ();

		if ( !getConnection () )
			return false;

		Statement stmt = null;

		try
		{
			stmt = m_conn.createStatement ();
			stmt.executeUpdate ( sql );
			long end = Calendar.getInstance ().getTimeInMillis ();
			System.out.print ( String.format ( "Query OK, %d rows affected (%.2fsec)\n", stmt.getUpdateCount (), (end - begin) / 1000.0 ) );
			return true;
		}
		catch ( SQLException ex )
		{
			System.out.println ( "ERROR " + ex.getErrorCode () + ": " + ex.getMessage () );
			return false;
		}
		finally
		{
			closeStatement ( stmt );
		}
	}

	static public boolean delete(String sql)
	{
		long begin = Calendar.getInstance ().getTimeInMillis ();

		if ( !getConnection () )
			return false;

		Statement stmt = null;

		try
		{
			stmt = m_conn.createStatement ();
			stmt.executeUpdate ( sql );
			long end = Calendar.getInstance ().getTimeInMillis ();
			System.out.print ( String.format ( "Query OK, %d rows affected (%.2fsec)\n", stmt.getUpdateCount (), (end - begin) / 1000.0 ) );
			return true;
		}
		catch ( SQLException ex )
		{
			System.out.println ( "ERROR " + ex.getErrorCode () + ": " + ex.getMessage () );
			return false;
		}
		finally
		{
			closeStatement ( stmt );
		}
	}

	static public boolean update(String sql)
	{
		long begin = Calendar.getInstance ().getTimeInMillis ();

		if ( !getConnection () )
			return false;

		Statement stmt = null;

		try
		{
			stmt = m_conn.createStatement ();
			stmt.executeUpdate ( sql );
			long end = Calendar.getInstance ().getTimeInMillis ();
			System.out.print ( String.format ( "Query OK, %d rows affected (%.2fsec)\n", stmt.getUpdateCount (), (end - begin) / 1000.0 ) );
			return true;
		}
		catch ( SQLException ex )
		{
			System.out.println ( "ERROR " + ex.getErrorCode () + ": " + ex.getMessage () );
			return false;
		}
		finally
		{
			closeStatement ( stmt );
		}
	}

	static private void closeStatement(Statement stmt)
	{
		try
		{
			if ( stmt != null )
				stmt.close ();
		}
		catch ( Exception ex )
		{
			System.out.println ( ex.toString () );
			closeConnection ();
		}
	}

	static private void closeConnection()
	{
		try
		{
			if ( m_conn != null )
			{
				m_conn.close ();
			}
		}
		catch ( Exception ex )
		{
			System.out.println ( ex.toString () );
		}
		finally
		{
			m_conn = null;
		}
	}
}
