package golumn

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"time"

	lua "github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"
)

const (
	luaMigrateModuleName   = "migrate"
	luaTransactionTypeName = "transaction"
	luaResultTypeName      = "result"
)

func Parse(ctx context.Context, r io.Reader, name string) (*Migration, error) {
	proto, err := compileLua(r, name)
	if err != nil {
		return nil, err
	}

	l := lua.NewState()
	defer l.Close()
	l.SetContext(ctx)
	l.PreloadModule("db", LoaderFunc(nil))

	if err := doCompiled(l, proto); err != nil {
		return nil, err
	}

	lv := l.GetGlobal("Version")
	version, ok := lv.(lua.LNumber)
	if !ok {
		return nil, fmt.Errorf("expected Version global to be a number, got %T", lv)
	}

	return &Migration{
		Version: int64(version),
		Name:    name,
		UpFunc: func(ctx context.Context, db *sql.DB) error {
			l := lua.NewState()
			defer l.Close()
			l.SetContext(ctx)
			l.PreloadModule("db", LoaderFunc(db))

			if err := doCompiled(l, proto); err != nil {
				return err
			}

			if err := l.CallByParam(lua.P{
				Fn:      l.GetGlobal("Up"),
				NRet:    0,
				Protect: true,
			}); err != nil {
				return err
			}

			return nil
		},
		DownFunc: func(ctx context.Context, db *sql.DB) error {
			l := lua.NewState()
			defer l.Close()
			l.SetContext(ctx)
			l.PreloadModule("db", LoaderFunc(db))

			if err := doCompiled(l, proto); err != nil {
				return err
			}

			if err := l.CallByParam(lua.P{
				Fn:      l.GetGlobal("Down"),
				NRet:    0,
				Protect: true,
			}); err != nil {
				return err
			}

			return nil
		},
	}, nil
}

func compileLua(r io.Reader, name string) (*lua.FunctionProto, error) {
	chunk, err := parse.Parse(r, name)
	if err != nil {
		return nil, err
	}
	proto, err := lua.Compile(chunk, name)
	if err != nil {
		return nil, err
	}
	return proto, nil
}

func doCompiled(L *lua.LState, proto *lua.FunctionProto) error {
	lfunc := L.NewFunctionFromProto(proto)
	L.Push(lfunc)
	return L.PCall(0, lua.MultRet, nil)
}

func LoaderFunc(db *sql.DB) func(L *lua.LState) int {
	exports := map[string]lua.LGFunction{
		"begin": luaBeginFunc(db),
		"exec":  luaExecFunc(db),
		"query": luaQueryFunc(db),
	}

	return func(l *lua.LState) int {
		mtTransaction := l.NewTypeMetatable(luaTransactionTypeName)
		l.SetField(mtTransaction, "__index", l.SetFuncs(l.NewTable(), transactionMethods))

		mtResult := l.NewTypeMetatable(luaResultTypeName)
		l.SetField(mtResult, "__index", l.SetFuncs(l.NewTable(), resultMethods))

		moduleTable := l.SetFuncs(l.NewTable(), exports)
		l.Push(moduleTable)
		return 1
	}
}

func luaBeginFunc(db *sql.DB) func(*lua.LState) int {
	return func(l *lua.LState) int {
		if db == nil {
			l.RaiseError("DB connection (go *sql.DB) is nil")
			return 0
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
					return 0
				}
			}
		}

		ctx := l.Context()
		if ctx == nil {
			ctx = context.Background()
		}

		tx, err := db.BeginTx(ctx, txOptions)
		if err != nil {
			l.RaiseError("begin transaction: %v", err)
			return 0
		}

		ud := l.NewUserData()
		ud.Value = tx
		l.SetMetatable(ud, l.GetTypeMetatable(luaTransactionTypeName))
		l.Push(ud)
		return 1
	}
}

func luaExecFunc(db *sql.DB) func(*lua.LState) int {
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

func luaRowIterFunc(rows *sql.Rows) func(*lua.LState) int {
	return func(l *lua.LState) int {
		if !rows.Next() {
			rows.Close()
			l.Push(lua.LNil)
			return 1
		}

		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			l.RaiseError("get row columns: %v", err)
			return 0
		}

		values := make([]any, len(columns))
		scanArgs := make([]any, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		err = rows.Scan(scanArgs...)
		if err != nil {
			rows.Close()
			l.RaiseError("scan row: %v", err)
			return 0
		}

		rowTable := l.CreateTable(0, len(columns))
		for i, name := range columns {
			goValue := values[i]
			var luaValue lua.LValue

			if goValue == nil {
				luaValue = lua.LNil
			} else {
				switch v := goValue.(type) {
				case bool:
					luaValue = lua.LBool(v)
				case []byte:
					luaValue = lua.LString(string(v))
				case string:
					luaValue = lua.LString(v)
				case int:
					luaValue = lua.LNumber(v)
				case int8:
					luaValue = lua.LNumber(v)
				case int16:
					luaValue = lua.LNumber(v)
				case int32:
					luaValue = lua.LNumber(v)
				case int64:
					luaValue = lua.LNumber(v)
				case uint:
					luaValue = lua.LNumber(v)
				case uint8:
					luaValue = lua.LNumber(v)
				case uint16:
					luaValue = lua.LNumber(v)
				case uint32:
					luaValue = lua.LNumber(v)
				case uint64:
					luaValue = lua.LNumber(v)
				case float32:
					luaValue = lua.LNumber(v)
				case float64:
					luaValue = lua.LNumber(v)
				case time.Time:
					luaValue = lua.LString(v.Format(time.RFC3339Nano))
				default:
					l.RaiseError("unsupported go type '%T' for column '%s'", v, name)
					return 0
				}
			}
			l.SetField(rowTable, name, luaValue)
		}
		l.Push(rowTable)
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
			l.RaiseError("query: %v", err)
			return 0
		}

		l.Push(l.NewFunction(luaRowIterFunc(rows)))
		return 1
	}
}

var transactionMethods = map[string]lua.LGFunction{
	"exec":     luaTransactionExec,
	"query":    luaTransactionQuery,
	"commit":   luaTransactionCommit,
	"rollback": luaTransactionRollback,
}

func checkTransaction(l *lua.LState) *sql.Tx {
	ud := l.CheckUserData(1)
	if v, ok := ud.Value.(*sql.Tx); ok {
		return v
	}
	l.ArgError(1, "Transaction expected")
	return nil
}

func luaTransactionExec(l *lua.LState) int {
	tx := checkTransaction(l)
	q, args := checkQueryArgs(l, 2)

	ctx := l.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	res, err := tx.ExecContext(ctx, q, args...)
	if err != nil {
		l.RaiseError("exec: %v", err)
		return 0
	}

	ud := l.NewUserData()
	ud.Value = res
	l.SetMetatable(ud, l.GetTypeMetatable(luaResultTypeName))
	l.Push(ud)
	return 1
}

func luaTransactionQuery(l *lua.LState) int {
	tx := checkTransaction(l)
	q, args := checkQueryArgs(l, 2)

	ctx := l.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		l.RaiseError("query: %v", err)
		return 0
	}

	l.Push(l.NewFunction(luaRowIterFunc(rows)))
	return 1
}

func luaTransactionCommit(l *lua.LState) int {
	tx := checkTransaction(l)
	if err := tx.Commit(); err != nil {
		l.RaiseError("commit transaction: %v", err)
		return 0
	}
	l.Push(lua.LTrue)
	return 1
}

func luaTransactionRollback(l *lua.LState) int {
	tx := checkTransaction(l)
	if err := tx.Rollback(); err != nil {
		l.RaiseError("rollback transaction: %v", err)
		return 0
	}
	l.Push(lua.LTrue)
	return 1
}

var resultMethods = map[string]lua.LGFunction{
	"last_insert_id": luaResultLastInsertId,
	"rows_affected":  luaResultRowsAffected,
}

func checkResult(l *lua.LState) sql.Result {
	ud := l.CheckUserData(1)
	if v, ok := ud.Value.(sql.Result); ok {
		return v
	}
	l.ArgError(1, "Result expected")
	return nil
}

func luaResultLastInsertId(l *lua.LState) int {
	res := checkResult(l)
	id, err := res.LastInsertId()
	if err != nil {
		l.RaiseError("get last insert id: %v", err)
		return 0
	}
	l.Push(lua.LNumber(id))
	return 1
}

func luaResultRowsAffected(l *lua.LState) int {
	res := checkResult(l)
	id, err := res.RowsAffected()
	if err != nil {
		l.RaiseError("get rows affected: %v", err)
		return 0
	}
	l.Push(lua.LNumber(id))
	return 1
}

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
