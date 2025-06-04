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

---@return boolean
function Transaction:commit() end

---@return boolean
function Transaction:rollback() end

---@module 'transaction'
local M = {}

---@return Transaction
function M.begin() end

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
