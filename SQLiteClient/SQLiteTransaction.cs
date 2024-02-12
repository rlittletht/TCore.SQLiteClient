using TCore.SqlCore;

namespace TCore.SQLiteClient;

public class SQLiteTransaction: ISqlTransaction
{
    public System.Data.SQLite.SQLiteTransaction _Transaction;

    public SQLiteTransaction(System.Data.SQLite.SQLiteTransaction transaction)
    {
        _Transaction = transaction;
    }

    public void Rollback() => _Transaction.Rollback();
    public void Commit() => _Transaction.Commit();
    public void Dispose() => _Transaction.Dispose();
}
