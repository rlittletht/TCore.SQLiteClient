using TCore.SqlCore;

namespace TCore.SQLiteClient;

public class SQLiteCommand : ISqlCommand
{
    private readonly System.Data.SQLite.SQLiteCommand m_command;
    private SQLiteTransaction? m_transaction;

    string ISqlCommand.CommandText
    {
        get => m_command.CommandText;
        set => m_command.CommandText = value;
    }

    int ISqlCommand.CommandTimeout
    {
        get => m_command.CommandTimeout;
        set => m_command.CommandTimeout = value;
    }

    ISqlTransaction? ISqlCommand.Transaction
    {
        get => m_transaction;
        set
        {
            m_transaction = value as SQLiteTransaction;
            m_command.Transaction = m_transaction?._Transaction;
        } 
    }

    ISqlReader ISqlCommand.ExecuteReader() => new SQLiteReader(m_command.ExecuteReader());

    public SQLiteReader ExecuteReaderInternal() => new SQLiteReader(m_command.ExecuteReader());

    public SQLiteCommand(System.Data.SQLite.SQLiteCommand command)
    {
        m_command = command;
    }

    int ISqlCommand.ExecuteNonQuery() => m_command.ExecuteNonQuery();

    object ISqlCommand.ExecuteScalar() => m_command.ExecuteScalar();

    void ISqlCommand.AddParameterWithValue(string parameterName, object? value) => m_command.Parameters.AddWithValue(parameterName, value);

    void ISqlCommand.Close()
    {
        m_command.Dispose();
    }
}
