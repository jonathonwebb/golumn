package luamigrate

import (
	"database/sql"

	lua "github.com/yuin/gopher-lua"
)

const (
	luaMigrateModuleName   = "migrate"
	luaTransactionTypeName = "transaction"
	luaResultTypeName      = "result"
	luaRowsTypeName        = "rows"
	luaRowTypeName         = "row"
)

func loaderFunc(db *sql.DB) func(L *lua.LState) int {
	exports := map[string]lua.LGFunction{
		"begin":     luaBegin,
		"execute":   luaExecute,
		"query":     luaQuery,
		"query_row": luaQueryRow,
	}

	return func(L *lua.LState) int {
		mtTransaction := L.NewTypeMetatable(luaTransactionTypeName)
		L.SetField(mtTransaction, "__index", L.SetFuncs(L.NewTable(), transactionMethods))

		mtResult := L.NewTypeMetatable(luaResultTypeName)
		L.SetField(mtResult, "__index", L.SetFuncs(L.NewTable(), resultMethods))

		moduleTable := L.SetFuncs(L.NewTable(), exports)
		L.Push(moduleTable)
		return 1
	}
}

func luaBegin(l *lua.LState) int { return 0 }

func luaExecute(l *lua.LState) int { return 0 }

func luaQuery(l *lua.LState) int { return 0 }

func luaQueryRow(l *lua.LState) int { return 0 }

var transactionMethods = map[string]lua.LGFunction{
	"execute":   luaTransactionExecute,
	"query":     luaTransactionQuery,
	"query_row": luaTransactionQueryRow,
	"commit":    luaTransactionCommit,
	"rollback":  luaTransactionRollback,
}

func luaTransactionExecute(l *lua.LState) int { return 0 }

func luaTransactionQuery(l *lua.LState) int { return 0 }

func luaTransactionQueryRow(l *lua.LState) int { return 0 }

func luaTransactionCommit(l *lua.LState) int { return 0 }

func luaTransactionRollback(l *lua.LState) int { return 0 }

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
