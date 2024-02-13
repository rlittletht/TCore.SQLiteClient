using TCore.SqlCore;

namespace TCore.SQLiteClient;

public class SQLiteTransaction: ISqlTransaction
{
    public System.Data.SQLite.SQLiteTransaction _Transaction;

    public SQLiteTransaction(System.Data.SQLite.SQLiteTransaction transaction)
    {
        _Transaction = transaction;
    }

    void ISqlTransaction.Rollback() => _Transaction.Rollback();
    void ISqlTransaction.Commit() => _Transaction.Commit();
    void ISqlTransaction.Dispose() => _Transaction.Dispose();
}
