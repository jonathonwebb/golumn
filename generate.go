package golumn

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"text/template"
	"time"
)

var scriptTmplStr = `local db = require "db"

Version={{.Version}}

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
end`
var scriptTmpl = template.Must(template.New("migration").Parse(scriptTmplStr))

func GenScript(v int64, name string) (string, error) {
	if v < 0 {
		return "", fmt.Errorf("version must be at least zero, got %d", v)
	}

	var buf bytes.Buffer
	if err := scriptTmpl.Execute(&buf, struct {
		Version int64
		Name    string
	}{v, name}); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func WriteScript(v int64, name string, p string) error {
	script, err := GenScript(v, name)
	if err != nil {
		return err
	}
	if err := os.WriteFile(p, []byte(script), 0644); err != nil {
		return err
	}
	return nil
}

func GenScriptTimestamp(name string) (version int64, filename string, script string, err error) {
	version = time.Now().Unix()
	filename = fmt.Sprintf("%010d_%s.lua", version, name)
	script, err = GenScript(version, filename)
	if err != nil {
		return 0, "", "", err
	}
	return version, filename, script, nil
}

func WriteScriptTimestamp(name string, dir string) (version int64, outpath string, err error) {
	version, filename, script, err := GenScriptTimestamp(name)
	outpath = path.Join(dir, filename)
	if err != nil {
		return 0, "", err
	}
	if err := os.WriteFile(outpath, []byte(script), 0644); err != nil {
		return 0, "", err
	}
	return version, outpath, err
}
