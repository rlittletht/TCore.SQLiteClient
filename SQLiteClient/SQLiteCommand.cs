using TCore.SqlCore;

namespace TCore.SQLiteClient;

public class SQLiteCommand : ISqlCommand
{
    private readonly System.Data.SQLite.SQLiteCommand m_command;
    private ISqlTransaction? m_transaction;

    public string CommandText
    {
        get => m_command.CommandText;
        set => m_command.CommandText = value;
    }

    public ISqlTransaction? Transaction { get => m_transaction; set => m_transaction = value; }

    public ISqlReader ExecuteReader() => new SQLiteReader(m_command.ExecuteReader());

    public SQLiteReader ExecuteReaderInternal() => new SQLiteReader(m_command.ExecuteReader());

    public SQLiteCommand(System.Data.SQLite.SQLiteCommand command)
    {
        m_command = command;
    }

    public int ExecuteNonQuery() => m_command.ExecuteNonQuery();

    public object ExecuteScalar() => m_command.ExecuteScalar();

    public void AddParameterWithValue(string parameterName, object? value) => m_command.Parameters.AddWithValue(parameterName, value);

    public void Close()
    {
        m_command.Dispose();
    }
}
