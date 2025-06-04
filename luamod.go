package golumn

import (
	"context"
	"database/sql"
	"fmt"

	lua "github.com/yuin/gopher-lua"
)

const (
	luaMigrateModuleName   = "migrate"
	luaTransactionTypeName = "transaction"
	luaResultTypeName      = "result"
	luaRowsTypeName        = "rows"
	luaRowTypeName         = "row"
)

var isolationLevels = map[string]string{
	"default":          "default",
	"read_uncommitted": "read_uncommitted",
	"read_committed":   "read_committed",
	"write_committed":  "write_committed",
	"repeatable_read":  "repeatable_read",
	"snapshot":         "snapshot",
	"serializable":     "serializable",
	"linearizable":     "linearizable",
}

func LoaderFunc(db *sql.DB) func(L *lua.LState) int {
	exports := map[string]lua.LGFunction{
		"begin":     luaBeginFunc(db),
		"execute":   luaExecuteFunc(db),
		"query":     luaQueryFunc(db),
		"query_row": luaQueryRowFunc(db),
	}

	return func(l *lua.LState) int {
		mtTransaction := l.NewTypeMetatable(luaTransactionTypeName)
		l.SetField(mtTransaction, "__index", l.SetFuncs(l.NewTable(), transactionMethods))

		mtResult := l.NewTypeMetatable(luaResultTypeName)
		l.SetField(mtResult, "__index", l.SetFuncs(l.NewTable(), resultMethods))

		moduleTable := l.SetFuncs(l.NewTable(), exports)

		levelsTable := l.NewTable()
		for k, v := range isolationLevels {
			levelsTable.RawSetString(k, lua.LString(v))
		}
		l.SetField(moduleTable, "isolation_levels", levelsTable)

		l.Push(moduleTable)
		return 1
	}
}

func luaBeginFunc(db *sql.DB) func(*lua.LState) int {
	return func(l *lua.LState) int {
		if db == nil {
			l.RaiseError("DB connection (go *sql.DB) is nil")
		}

		optionsTable := l.OptTable(1, nil)
		var txOptions *sql.TxOptions

		if optionsTable != nil {
			txOptions = &sql.TxOptions{}

			isolationLevel := optionsTable.RawGetString("isolation_level")
			if isolationLevel != lua.LNil {
				if levelStr, ok := isolationLevel.(lua.LString); ok {
					switch levelStr {
					case "default":
						txOptions.Isolation = sql.LevelDefault
					case "read_uncommitted":
						txOptions.Isolation = sql.LevelReadUncommitted
					case "read_committed":
						txOptions.Isolation = sql.LevelReadCommitted
					case "write_committed":
						txOptions.Isolation = sql.LevelWriteCommitted
					case "repeatable_read":
						txOptions.Isolation = sql.LevelRepeatableRead
					case "snapshot":
						txOptions.Isolation = sql.LevelSnapshot
					case "serializable":
						txOptions.Isolation = sql.LevelSerializable
					case "linearizable":
						txOptions.Isolation = sql.LevelLinearizable
					default:
						l.RaiseError("invalid isolation_level: %s", levelStr)
					}
				} else {
					l.RaiseError("isolation_level must be a string")
					return 0
				}
			}

			readOnly := optionsTable.RawGetString("read_only")
			if readOnly != lua.LNil {
				if readonly, ok := readOnly.(lua.LBool); ok {
					txOptions.ReadOnly = bool(readonly)
				} else {
					l.RaiseError("read_only must be a boolean")
				}
			}
		}

		ctx := l.Context()
		if ctx == nil {
			ctx = context.Background()
		}

		tx, err := db.BeginTx(ctx, txOptions)
		if err != nil {
			l.Push(lua.LNil)
			l.Push(lua.LString(fmt.Sprintf("begin transaction: %v", err)))
			return 2
		}

		ud := l.NewUserData()
		ud.Value = tx
		l.SetMetatable(ud, l.GetTypeMetatable(luaTransactionTypeName))
		l.Push(ud)
		return 1
	}
}

func luaExecuteFunc(db *sql.DB) func(*lua.LState) int {
	return func(l *lua.LState) int {
		q, args := checkQueryArgs(l, 1)

		ctx := l.Context()
		if ctx == nil {
			ctx = context.Background()
		}

		res, err := db.ExecContext(ctx, q, args...)
		if err != nil {
			l.Push(lua.LNil)
			l.Push(lua.LString(fmt.Sprintf("exec: %v", err)))
			return 2
		}

		ud := l.NewUserData()
		ud.Value = res
		l.SetMetatable(ud, l.GetTypeMetatable(luaResultTypeName))
		l.Push(ud)
		return 1
	}
}

func luaQueryFunc(db *sql.DB) func(*lua.LState) int {
	return func(l *lua.LState) int {
		q, args := checkQueryArgs(l, 1)

		ctx := l.Context()
		if ctx == nil {
			ctx = context.Background()
		}

		rows, err := db.QueryContext(ctx, q, args...)
		if err != nil {
			l.Push(lua.LNil)
			l.Push(lua.LString(fmt.Sprintf("query: %v", err)))
			return 2
		}

		ud := l.NewUserData()
		ud.Value = rows
		l.SetMetatable(ud, l.GetTypeMetatable(luaRowsTypeName))
		l.Push(ud)
		return 1
	}
}

func luaQueryRowFunc(db *sql.DB) func(*lua.LState) int {
	return func(l *lua.LState) int {
		q, args := checkQueryArgs(l, 1)

		ctx := l.Context()
		if ctx == nil {
			ctx = context.Background()
		}

		row := db.QueryRowContext(ctx, q, args...)

		ud := l.NewUserData()
		ud.Value = row
		l.SetMetatable(ud, l.GetTypeMetatable(luaRowTypeName))
		l.Push(ud)
		return 1
	}
}

var transactionMethods = map[string]lua.LGFunction{
	"execute":   luaTransactionExecute,
	"query":     luaTransactionQuery,
	"query_row": luaTransactionQueryRow,
	"commit":    luaTransactionCommit,
	"rollback":  luaTransactionRollback,
}

func checkTransaction(l *lua.LState) *sql.Tx {
	ud := l.CheckUserData(1)
	if v, ok := ud.Value.(*sql.Tx); ok {
		return v
	}
	l.ArgError(1, "Transaction expected")
	return nil
}

func luaTransactionExecute(l *lua.LState) int { return 0 }

func luaTransactionQuery(l *lua.LState) int { return 0 }

func luaTransactionQueryRow(l *lua.LState) int { return 0 }

func luaTransactionCommit(l *lua.LState) int {
	tx := checkTransaction(l)
	if err := tx.Commit(); err != nil {
		l.Push(lua.LFalse)
		l.Push(lua.LString(fmt.Sprintf("commit transaction: %v", err)))
		return 2
	}
	l.Push(lua.LTrue)
	return 1
}

func luaTransactionRollback(l *lua.LState) int {
	tx := checkTransaction(l)
	if err := tx.Rollback(); err != nil {
		l.Push(lua.LFalse)
		l.Push(lua.LString(fmt.Sprintf("rollback transaction: %v", err)))
		return 2
	}
	l.Push(lua.LTrue)
	return 1
}

var resultMethods = map[string]lua.LGFunction{
	"last_insert_id": luaResultLastInsertId,
	"rows_affected":  luaResultRowsAffected,
}

func luaResultLastInsertId(l *lua.LState) int { return 0 }

func luaResultRowsAffected(l *lua.LState) int { return 0 }

var rowsMethods = map[string]lua.LGFunction{
	"close":   luaRowsClose,
	"columns": luaRowsColumns,
	"err":     luaRowsErr,
	"next":    luaRowsNext,
	"scan":    luaRowsScan,
}

func luaRowsClose(l *lua.LState) int { return 0 }

func luaRowsColumns(l *lua.LState) int { return 0 }

func luaRowsErr(l *lua.LState) int { return 0 }

func luaRowsNext(l *lua.LState) int { return 0 }

func luaRowsScan(l *lua.LState) int { return 0 }

var rowMethods = map[string]lua.LGFunction{
	"scan": luaRowScan,
	"err":  luaRowErr,
}

func luaRowScan(l *lua.LState) int { return 0 }

func luaRowErr(l *lua.LState) int { return 0 }

func checkQueryArgs(l *lua.LState, start int) (string, []any) {
	q := l.CheckString(start)

	var args []any
	top := l.GetTop()
	for i := start + 1; i <= top; i++ {
		lv := l.Get(i)
		switch lv.Type() {
		case lua.LTNil:
			args = append(args, nil)
		case lua.LTBool:
			args = append(args, bool(lv.(lua.LBool)))
		case lua.LTNumber:
			args = append(args, float64(lv.(lua.LNumber)))
		case lua.LTString:
			args = append(args, string(lv.(lua.LString)))
		default:
			l.ArgError(i, fmt.Sprintf("Unsupported type for query param: %s", lv.Type().String()))
		}
	}

	return q, args
}
