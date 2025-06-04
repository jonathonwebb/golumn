---@meta

---@class Result
local Result = {}

---@return number
function Result:last_insert_id() end

---@return number
function Result:rows_affected() end

---@class Rows
local Rows = {}

---@return boolean
function Rows:close() end

---@return string[]
function Rows:columns() end

---@return boolean, string
function Rows:err() end

---@return boolean
function Rows:next() end

---@param dest table
---@return boolean
function Rows:scan(dest) end

---@class Row
local Row = {}

---@param dest table
---@return boolean
function Row:scan(dest) end

---@return boolean, string
function Row:err() end

---@class Transaction
local Transaction = {}

---@param q string
---@param ... any?
---@return Result
function Transaction:exec(q, ...) end

---@param q string
---@param ... any?
---@return table[]
function Transaction:query(q, ...) end

---@param q string
---@param ... any?
---@return table
function Transaction:query_row(q, ...) end

---@return boolean, string?
function Transaction:commit() end

---@return boolean, string?
function Transaction:rollback() end

---@module 'transaction'
local M = {
    isolation_level = {
        default = "default",
        read_uncommitted = "read_uncommitted",
        read_committed = "read_committed",
        write_committed = "write_committed",
        repeatable_read = "repeatable_read",
        snapshot = "snapshot",
        serializable = "serializable",
        linearizable = "linearizable",
    }
}

---@alias IsolationLevel
---| '"default"'
---| '"read_uncommitted"'
---| '"read_committed"'
---| '"write_committed"'
---| '"repeatable_read"'
---| '"snapshot"'
---| '"serializable"'
---| '"linearizable"'

---@param options? { isolation_level?: IsolationLevel, read_only?: boolean }
---@return Transaction?, string?
function M.begin(options) end

---@param q string
---@param ... any?
---@return Result
function M.exec(q, ...) end

---@param q string
---@param ... any?
---@return table[]
function M.query(q, ...) end

---@param q string
---@param ... any?
---@return table
function M.query_row(q, ...) end

return M
