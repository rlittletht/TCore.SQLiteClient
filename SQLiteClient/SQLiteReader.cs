using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Diagnostics;
using System.Threading;
using TCore.SqlCore;

namespace TCore.SQLiteClient;

public class SQLiteReader: ISqlReader
{
    private SQLite? m_sql;
    private bool m_fAttached = false;
    private SQLiteDataReader? m_sqlr = null;
    private Guid m_crids;

    private SQLiteDataReader _Reader
    {
        get
        {
            if (m_sqlr == null)
                throw new Exception("no reader");
            return m_sqlr;
        }
    }
    public SQLiteReader()
    {
        m_fAttached = false;
        m_crids = Guid.Empty;
    }

    public SQLiteReader(SQLiteDataReader reader)
    {
        m_sqlr = reader;
        m_fAttached = true;
    }

    public SQLiteReader(Guid crids)
    {
        m_fAttached = false;
        m_crids = crids;
    }

    public SQLiteReader(SQLite sql)
    {
        Attach(sql);
        m_crids = Guid.Empty;
    }

    public SQLiteReader(SQLite sql, Guid crids)
    {
        Attach(sql);
        m_crids = crids;
    }

    /*----------------------------------------------------------------------------
        %%Function: Attach
        %%Qualified: TCore.SqlReader.Attach
    ----------------------------------------------------------------------------*/
    public void Attach(SQLite sql)
    {
        m_sql = sql;
        if (m_sql != null)
            m_fAttached = true;
    }

    public void ExecuteQuery(
        SqlCommandTextInit cmdText,
        string sResourceConnString,
        CustomizeCommandDelegate? customizeDelegate = null)
    {
        ExecuteQuery(cmdText.CommandText, sResourceConnString, customizeDelegate, cmdText.Aliases);
    }

    public delegate void RetriableDelegate();

    /*----------------------------------------------------------------------------
        %%Function: ExecuteWithDatabaseLockRetry
        %%Qualified: Thetacat.TCore.TcSqlLite.SQLiteReader.ExecuteWithDatabaseLockRetry

        This will retry for a max time of timeout ms, sleeping for retryInterval
        between attempts.

        this will ONLY retry on database locked errors

        cannot have a timeout > 5 minutes
    ----------------------------------------------------------------------------*/
    public static void ExecuteWithDatabaseLockRetry(RetriableDelegate retriable, int retryInterval = 250, int timeout = 5000)
    {
        if (timeout <= 0 || timeout > 5 * 60 * 1000)
            throw new ArgumentException($"{timeout} must be between 0 and 5 minutes");

        if (retryInterval <= 0 || retryInterval > 60 * 1000)
            throw new ArgumentException($"{retryInterval} must be between 0 and 60 seconds");

        Stopwatch watch = Stopwatch.StartNew();

        while (watch.Elapsed.Milliseconds < timeout)
        {
            try
            {
                retriable();
                return;
            }
            catch (SQLiteException e)
            {
                if (!e.Message.Contains("locked"))
                    throw;

                Thread.Sleep(retryInterval);
            }
        }

        // if we get here, we have timed out
        throw new SqlExceptionLockTimeout();
    }

    public void ExecuteQuery(
        string sQuery,
        string? sResourceConnString,
        CustomizeCommandDelegate? customizeDelegate = null,
        TableAliases? aliases = null)
    {
        if (m_sql == null)
        {
            if (sResourceConnString == null)
                throw new ArgumentNullException(nameof(sResourceConnString));

            m_sql = SQLite.OpenConnection(sResourceConnString);
            m_fAttached = false;
        }

        if (m_sql == null)
            throw new SqlException("could not open sql connection");

        SQLiteCommand sqlcmd = m_sql.CreateCommandInternal();
        sqlcmd.CommandText = sQuery;
        if (m_sql.Transaction != null)
            sqlcmd.Transaction = m_sql.Transaction;

        if (customizeDelegate != null)
            customizeDelegate(sqlcmd);

        if (m_sqlr != null)
        {
            m_sqlr.Close();
            m_sqlr.Dispose();
        }

        try
        {
            ExecuteWithDatabaseLockRetry(
                () => m_sqlr = sqlcmd.ExecuteReaderInternal()._Reader);

        }
        catch (Exception exc)
        {
            throw new SqlException(m_crids, exc, "caught exception executing reader");
        }
    }

    public static T DoGenericMultiSetQueryDelegateRead<T>(
        SQLite sql,
        Guid crids,
        string sQuery,
        ISqlReader.DelegateMultiSetReader<T> delegateReader,
        CustomizeCommandDelegate? customizeDelegate = null) where T : new()
    {
        SQLiteReader? sqlr = null;

        if (delegateReader == null)
            throw new Exception("must provide delegate reader");

        try
        {
            string sCmd = sQuery;

            sqlr = new(sql);
            sqlr.ExecuteQuery(sQuery, null, customizeDelegate);

            int recordSet = 0;

            T t = new();
            do
            {
                bool fOnce = false;

                while (sqlr.Read())
                {
                    delegateReader(sqlr, crids, recordSet, ref t);
                    fOnce = true;
                }

                if (!fOnce)
                    throw new SqlExceptionNoResults();

                recordSet++;
            } while (sqlr.NextResult());

            return t;
        }
        finally
        {
            sqlr?.Close();
        }
    }

    public Int16 GetInt16(int index) => _Reader.GetInt16(index);
    public Int32 GetInt32(int index) => _Reader.GetInt32(index);
    public string GetString(int index) => _Reader.GetString(index);
    public Guid GetGuid(int index) => Guid.Parse(_Reader.GetString(index));
    public double GetDouble(int index) => _Reader.GetDouble(index);
    public Int64 GetInt64(int index) => _Reader.GetInt64(index);
    public DateTime GetDateTime(int index) => _Reader.GetDateTime(index);

    public Int16? GetNullableInt16(int index) => _Reader.IsDBNull(index) ? null : _Reader.GetInt16(index);
    public Int32? GetNullableInt32(int index) => _Reader.IsDBNull(index) ? null : _Reader.GetInt32(index);
    public string? GetNullableString(int index) => _Reader.IsDBNull(index) ? null : _Reader.GetString(index);
    public Guid? GetNullableGuid(int index) => _Reader.IsDBNull(index) ? null : Guid.Parse(_Reader.GetString(index));
    public double? GetNullableDouble(int index) => _Reader.IsDBNull(index) ? null : _Reader.GetDouble(index);
    public Int64? GetNullableInt64(int index) => _Reader.IsDBNull(index) ? null : _Reader.GetInt64(index);
    public DateTime? GetNullableDateTime(int index) => _Reader.IsDBNull(index) ? null : _Reader.GetDateTime(index);

    public bool IsDBNull(int index) => _Reader.IsDBNull(index);

    public Type GetFieldAffinity(int index)
    {
        TypeAffinity affinity = _Reader.GetFieldAffinity(index);

        switch (affinity)
        {
            case TypeAffinity.None:
                return typeof(void);
            case TypeAffinity.Blob:
                return typeof(void);
            case TypeAffinity.DateTime:
                return typeof(DateTime);
            case TypeAffinity.Int64:
                return typeof(Int64);
            case TypeAffinity.Double:
                return typeof(double);
            case TypeAffinity.Text:
                return typeof(string);
            case TypeAffinity.Null:
                return typeof(void);
            case TypeAffinity.Uninitialized:
                return typeof(void);
        }

        throw new SqlException($"affinity {affinity} not recognized for field {index}");
    }

    public string GetFieldName(int index) => _Reader.GetName(index);
    public Type GetFieldType(int index) => _Reader.GetFieldType(index) ?? typeof(void);
    public int GetFieldCount() => _Reader.FieldCount;
    public object GetNativeValue(int index) => _Reader.GetValue(index);

    public bool NextResult() => m_sqlr?.NextResult() ?? false;
    public bool Read() => m_sqlr?.Read() ?? false;

    public void Close()
    {
        m_sqlr?.Close();
        m_sqlr?.Dispose();

        if (!m_fAttached)
            m_sql?.Close();
    }
}
