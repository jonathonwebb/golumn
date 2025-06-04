---@meta

---@class Result
local Result = {}

---@return number
function Result:last_insert_id() end

---@return number
function Result:rows_affected() end

---@alias Rows fun(): table<string, any>, string?

---@class Transaction
local Transaction = {}

---@param q string
---@param ... any?
---@return Result
function Transaction:exec(q, ...) end

---@param q string
---@param ... any?
---@return Rows
function Transaction:query(q, ...) end

---@return boolean
function Transaction:commit() end

---@return boolean
function Transaction:rollback() end

---@module 'transaction'
local M = {}

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
---@return Transaction
function M.begin(options) end

---@param q string
---@param ... any?
---@return Result
function M.exec(q, ...) end

---@param q string
---@param ... any?
---@return Rows
function M.query(q, ...) end

return M
