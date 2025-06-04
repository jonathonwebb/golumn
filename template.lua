local db = require "db"

function Up()
    local tx = db.begin()
    local ok, err = pcall(function()
        error("up migration not implemented")
    end)
    if not ok then
        tx:rollback()
        error(err)
    end
end

function Down()
    local tx = db.begin()
    local ok, err = pcall(function()
        error("down migration not implemented")
    end)
    if not ok then
        tx:rollback()
        error(err)
    end
end
