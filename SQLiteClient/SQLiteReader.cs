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
    private readonly Guid m_crids;

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
        ((ISqlCommand)sqlcmd).CommandText = sQuery;
        if (m_sql.Transaction != null)
            ((ISqlCommand)sqlcmd).Transaction = m_sql.Transaction;

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

    Int16 ISqlReader.GetInt16(int index) => _Reader.GetInt16(index);
    Int32 ISqlReader.GetInt32(int index) => _Reader.GetInt32(index);
    string ISqlReader.GetString(int index) => _Reader.GetString(index);
    Guid ISqlReader.GetGuid(int index) => _Reader.GetFieldAffinity(index) == TypeAffinity.Blob ? _Reader.GetGuid(index) : Guid.Parse(_Reader.GetString(index));
    double ISqlReader.GetDouble(int index) => _Reader.GetDouble(index);
    Int64 ISqlReader.GetInt64(int index) => _Reader.GetInt64(index);
    DateTime ISqlReader.GetDateTime(int index) => _Reader.GetDateTime(index);
    bool ISqlReader.GetBoolean(int index) => _Reader.GetBoolean(index);

    Int16? ISqlReader.GetNullableInt16(int index) => _Reader.IsDBNull(index) ? null : _Reader.GetInt16(index);
    Int32? ISqlReader.GetNullableInt32(int index) => _Reader.IsDBNull(index) ? null : _Reader.GetInt32(index);
    string? ISqlReader.GetNullableString(int index) => _Reader.IsDBNull(index) ? null : _Reader.GetString(index);
    Guid? ISqlReader.GetNullableGuid(int index) => _Reader.IsDBNull(index) ? null :
        _Reader.GetFieldAffinity(index) == TypeAffinity.Blob ? _Reader.GetGuid(index) : Guid.Parse(_Reader.GetString(index));
    double? ISqlReader.GetNullableDouble(int index) => _Reader.IsDBNull(index) ? null : _Reader.GetDouble(index);
    Int64? ISqlReader.GetNullableInt64(int index) => _Reader.IsDBNull(index) ? null : _Reader.GetInt64(index);
    DateTime? ISqlReader.GetNullableDateTime(int index) => _Reader.IsDBNull(index) ? null : _Reader.GetDateTime(index);
    bool? ISqlReader.GetNullableBoolean(int index) => _Reader.IsDBNull(index) ? null : _Reader.GetBoolean(index);

    bool ISqlReader.IsDBNull(int index) => _Reader.IsDBNull(index);

    Type ISqlReader.GetFieldAffinity(int index)
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

    string ISqlReader.GetFieldName(int index) => _Reader.GetName(index);
    Type ISqlReader.GetFieldType(int index) => _Reader.GetFieldType(index) ?? typeof(void);
    int ISqlReader.GetFieldCount() => _Reader.FieldCount;
    object ISqlReader.GetNativeValue(int index) => _Reader.GetValue(index);

    bool ISqlReader.NextResult() => m_sqlr?.NextResult() ?? false;
    bool ISqlReader.Read() => m_sqlr?.Read() ?? false;

    void ISqlReader.Close()
    {
        m_sqlr?.Close();
        m_sqlr?.Dispose();

        if (!m_fAttached)
            ((ISql?)m_sql)?.Close();
    }
}
