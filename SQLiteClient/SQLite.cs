using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using TCore.SqlCore;

namespace TCore.SQLiteClient;

public class SQLite : ISql
{
    private SQLiteTransaction? m_transaction;
    private readonly SQLiteConnection? m_sqlc;
    public bool InTransaction  => m_transaction != null;
    public ISqlTransaction? Transaction => m_transaction;

    public static string Iso8601DateFromPackedSqliteDate(string packedDate)
    {
        if (packedDate.Contains("-"))
            return packedDate;

        if (packedDate[8] != 'T' || packedDate.Length < 14)
            throw new ArgumentException($"{packedDate} is not in format YYYYMMDDTHHMMSS[.ssssssss]");

        return $"{packedDate.Substring(0, 4)}-{packedDate.Substring(4, 2)}-{packedDate.Substring(6,2)}T{packedDate.Substring(9,2)}:{packedDate.Substring(11,2)}:{packedDate.Substring(13)}";
    }

    public SQLite()
    {
        m_sqlc = null;
        m_transaction = null;
    }

    public SQLite(SQLiteConnection sqlc, SQLiteTransaction? sqlt)
    {
        m_sqlc = sqlc;
        m_transaction = sqlt;
    }

    #region Connection Management

    public SQLiteConnection Connection
    {
        get
        {
            if (m_sqlc == null)
                throw new Exception("no connection");

            return m_sqlc;
        }
    }

    public static SQLite OpenConnection(string sResourceConnString)
    {
        SQLiteConnection sqlc = new SQLiteConnection(sResourceConnString);

        sqlc.Open();

        return new SQLite(sqlc, null);
    }

    void ISql.Close()
    {
        if (InTransaction)
            ((ISql)this).Rollback();

        ((ISqlTransaction?)m_transaction)?.Dispose();
        m_sqlc?.Close();
        m_sqlc?.Dispose();
    }
    #endregion

    #region Commands

    ISqlCommand ISql.CreateCommand()
    {
        return new SQLiteCommand(Connection.CreateCommand());
    }

    public SQLiteCommand CreateCommandInternal()
    {
        return new SQLiteCommand(Connection.CreateCommand());
    }
    #endregion

    #region Non Queries

    void ISql.ExecuteNonQuery(
        string commandText,
        CustomizeCommandDelegate? customizeParams,
        TableAliases? aliases)
    {
        ISqlCommand sqlcmd = ((ISql)this).CreateCommand();

        try
        {
            sqlcmd.CommandText = aliases?.ExpandAliases(commandText) ?? commandText;
            if (customizeParams != null)
                customizeParams(sqlcmd);

            if (Transaction != null)
                sqlcmd.Transaction = Transaction;

            sqlcmd.ExecuteNonQuery();
        }
        finally
        {
            sqlcmd.Close();
        }
    }

    void ISql.ExecuteNonQuery(
        SqlCommandTextInit cmdText,
        CustomizeCommandDelegate? customizeParams)
    {
        ((ISql)this).ExecuteNonQuery(cmdText.CommandText, customizeParams, cmdText.Aliases);
    }
    #endregion

    #region Scalar Queries

    private int NExecuteScalar(string sQuery, TableAliases? aliases = null)
    {
        ISqlCommand sqlcmd = ((ISql)this).CreateCommand();

        try
        {
            sqlcmd.CommandText = aliases?.ExpandAliases(sQuery) ?? sQuery;
            if (Transaction != null)
                sqlcmd.Transaction = this.Transaction;

            Int64 n = (Int64)sqlcmd.ExecuteScalar();

            return (int)n;
        }
        finally
        {
            sqlcmd.Close();
        }
    }

    string ISql.SExecuteScalar(SqlCommandTextInit cmdText)
    {
        ISqlCommand sqlcmd = ((ISql)this).CreateCommand();

        try
        {
            sqlcmd.CommandText = cmdText.Aliases?.ExpandAliases(cmdText.CommandText) ?? cmdText.CommandText;
            if (Transaction != null)
                sqlcmd.Transaction = this.Transaction;

            return (string)sqlcmd.ExecuteScalar();
        }
        finally
        {
            sqlcmd.Close();
        }
    }

    int ISql.NExecuteScalar(SqlCommandTextInit cmdText)
    {
        return NExecuteScalar(cmdText.CommandText, cmdText.Aliases);
    }

    DateTime ISql.DttmExecuteScalar(SqlCommandTextInit cmdText)
    {
        string s = ((ISql)this).SExecuteScalar(cmdText);

        return DateTime.Parse(SQLite.Iso8601DateFromPackedSqliteDate(s));
    }

#endregion

    #region Readers

    ISqlReader ISql.ExecuteQuery(
        Guid crids,
        string query,
        TableAliases? aliases,
        CustomizeCommandDelegate? customizeDelegate)
    {
        SqlSelect selectTags = new SqlSelect();

        selectTags.AddBase(query);
        if (aliases != null)
            selectTags.AddAliases(aliases);

        string sQuery = selectTags.ToString();

        SQLiteReader? sqlr = null;

        try
        {
            string sCmd = sQuery;

            sqlr = new(this);
            sqlr.ExecuteQuery(sQuery, null, customizeDelegate);

            return sqlr;
        }
        catch
        {
            ((ISqlReader?)sqlr)?.Close();
            throw;
        }
    }

    T ISql.ExecuteDelegatedQuery<T>(
        Guid crids,
        string query,
        ISqlReader.DelegateReader<T> delegateReader,
        TableAliases? aliases,
        CustomizeCommandDelegate? customizeDelegate)
    {
        if (delegateReader == null)
            throw new Exception("must provide delegate reader");

        ISqlReader? sqlr = ((ISql)this).ExecuteQuery(crids, query, aliases, customizeDelegate);

        try
        {
            T t = new();
            bool fOnce = false;

            while (sqlr.Read())
            {
                delegateReader(sqlr, crids, ref t);
                fOnce = true;
            }

            if (!fOnce)
                throw new SqlExceptionNoResults();

            return t;
        }
        finally
        {
            sqlr.Close();
        }
    }

    T ISql.ExecuteMultiSetDelegatedQuery<T>(
        Guid crids, string sQuery, ISqlReader.DelegateMultiSetReader<T> delegateReader, TableAliases? aliases, CustomizeCommandDelegate? customizeDelegate) =>
        throw new SqlExceptionNotImplementedInThisClient();

    ISqlReader ISql.CreateReader()
    {
        return new SQLiteReader(this);
    }

#endregion

    #region Transactions

    void ISql.BeginTransaction()
    {
        if (InTransaction)
            throw new SqlExceptionInTransaction();

        SQLiteReader.ExecuteWithDatabaseLockRetry(
            () => m_transaction = new SQLiteTransaction(Connection.BeginTransaction()),
            250,
            5000);
    }

    void ISql.BeginExclusiveTransaction()
    {
        if (InTransaction)
            throw new SqlExceptionInTransaction();

        SQLiteReader.ExecuteWithDatabaseLockRetry(
#pragma warning disable CS0618
            () => m_transaction = new SQLiteTransaction(Connection.BeginTransaction(IsolationLevel.Serializable, false)),
#pragma warning restore CS0618
            250,
            5000);
    }

    void ISql.Rollback()
    {
        if (!InTransaction)
            throw new SqlExceptionNotInTransaction();

        try
        {
            ((ISqlTransaction?)m_transaction)!.Rollback();
        }
        finally
        {
            ((ISqlTransaction?)m_transaction)?.Dispose();
            m_transaction = null;
        }   
    }

    void ISql.Commit()
    {
        if (!InTransaction)
            throw new SqlExceptionNotInTransaction();

        try
        {
            ((ISqlTransaction?)m_transaction)!.Commit();
        }
        finally
        {
            ((ISqlTransaction?)m_transaction)?.Dispose();
            m_transaction = null;
        }
    }
    #endregion

}
