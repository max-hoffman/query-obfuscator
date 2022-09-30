package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"io"
	"math/rand"
	"os"
	"path"
	"sort"
	"strings"
	"time"
)

var (
	schemaInput  string
	schemaOutput string
	queryDir     string
	queryOutput  string
)

func init() {
	flag.StringVar(&schemaInput, "schemaInput", "", "path to schema sql dump")
	flag.StringVar(&schemaOutput, schemaOutput, "", "output path for obfuscated schema output")
	flag.StringVar(&queryDir, "queryDir", "", "path to queryies for obfuscating")
	flag.StringVar(&queryOutput, "queryOutput", "", "output path for obfuscated queries")
}

func main() {
	flag.Parse()
	ctx := sql.NewEmptyContext()
	_main(ctx, schemaInput, schemaOutput, queryDir, queryOutput)
}

func _main(ctx *sql.Context, schemaInput, schemaOutput, queryDir, queryOutput string) {
	mappings := newMapping()
	obfuscateSchema(ctx, schemaInput, schemaOutput, mappings)
	applyMappingsToQueries(queryDir, queryOutput, mappings)
}

func obfuscateSchema(ctx *sql.Context, schemaInput, schemaOutput string, mappings *mapping) {
	s, err := os.ReadFile(schemaInput)
	if err != nil {
		panic(err)
	}

	var stmt ast.Statement
	remainder := string(s)
	var parsed string

	deps := make(map[string][]string)
	ddls := make(map[string]*tableDep)
	dbs := make(map[string]struct{})

	for len(remainder) > 0 {
		var ri int
		stmt, ri, err = ast.ParseOne(remainder)
		if err == io.EOF {
			break
		}
		if ri != 0 && ri <= len(remainder) {
			parsed = remainder[:ri]
			parsed = strings.TrimSpace(parsed)
			if strings.HasSuffix(parsed, ";") {
				parsed = parsed[:len(parsed)-1]
			}
			remainder = remainder[ri:]
		}

		if parsed == "" {
			continue
		}
		stmt, err = ast.Parse(parsed)
		if err != nil {
			panic(err)
		}
		dep := replaceInStatement(stmt, mappings)
		ddls[dep.name] = dep
		deps[dep.name] = append(deps[dep.name], dep.deps...)
		dbs[dep.db] = struct{}{}
	}

	// write obfuscated back
	// toposort by fk deps
	b := &strings.Builder{}

	seen := make(map[string]struct{})
	var recurse func(t *tableDep)
	recurse = func(t *tableDep) {
		for _, dep := range t.deps {
			if _, ok := seen[dep]; !ok {
				seen[dep] = struct{}{}
				recurse(ddls[dep])
			}
		}
		writeToBuffer(b, t.ast)
	}
	for _, t := range ddls {
		if _, ok := seen[t.name]; !ok {
			seen[t.name] = struct{}{}
			recurse(t)
		}
	}

	outFile, err := os.OpenFile(schemaOutput, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	outFile.WriteString(b.String())
}

func writeToBuffer(b *strings.Builder, s ast.Statement) {
	ast.Append(b, s)
	b.WriteString(";\n")
}
func applyMappingsToQueries(queryDir, queryOutput string, mappings *mapping) {
	files, err := os.ReadDir(queryDir)
	if err != nil {
		panic(err)
	}

	b := bytes.Buffer{}

	replacements := mappings.replacements()
	for _, f := range files {
		s, err := os.ReadFile(path.Join(queryDir, f.Name()))
		if err != nil {
			panic(err)
		}

		ret := string(s)
		for _, r := range replacements {
			ret = strings.ReplaceAll(ret, r.from, r.to)
		}
		b.WriteString(ret)
	}

	outFile, err := os.OpenFile(queryOutput, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	outFile.Write(b.Bytes())
}

type mapping struct {
	m map[string]string
	r map[string][]string
}

func newMapping() *mapping {
	return &mapping{
		m: make(map[string]string),
		r: make(map[string][]string),
	}
}

const obfNameLen = 4

func (m *mapping) get(s string) string {
	if s == "" {
		return ""
	}
	val, ok := m.m[s]
	if !ok {
		for {
			val = obfuscatedName(obfNameLen)
			if _, ok = m.m[val]; !ok {
				break
			}
		}
		m.m[s] = val
	}
	return val
}

func (m *mapping) ref(from, to string) {
	m.r[from] = append(m.r[from], to)
}

type replace struct {
	from string
	to   string
}

// replacements returns a list of {from:to} name mappings
// sorted by decreasing length to prevent clashes between
// mappings with the same prefix
func (m *mapping) replacements() []*replace {
	r := make([]*replace, len(m.m))
	var i int
	for k, v := range m.m {
		r[i] = &replace{
			from: k,
			to:   v,
		}
		i++
	}
	sort.Slice(r, func(i, j int) bool {
		return len(r[i].from) > len(r[j].from)
	})
	return r
}

type tableDep struct {
	name string
	deps []string
	ast  *ast.DDL
	db   string
}

func replaceInStatement(stmt ast.Statement, m *mapping) *tableDep {
	dep := &tableDep{}
	switch n := stmt.(type) {
	case *ast.DDL:
		dep.ast = n
		// expect  create table
		n.Table.Name = ast.NewTableIdent(m.get(n.Table.Name.String()))
		n.Table.Qualifier = ast.NewTableIdent(m.get(n.Table.Qualifier.String()))

		dep.name = n.Table.Name.String()
		dep.db = n.Table.Qualifier.String()

		for _, col := range n.TableSpec.Columns {
			col.Name = ast.NewColIdent(m.get(col.Name.String()))
		}

		for _, idx := range n.TableSpec.Indexes {
			idx.Info.Name = ast.NewColIdent(m.get(idx.Info.Name.String()))
			for _, col := range idx.Columns {
				col.Column = ast.NewColIdent(m.get(col.Column.String()))
			}
		}

		for _, constraint := range n.TableSpec.Constraints {
			constraint.Name = obfuscatedName(len(constraint.Name))
			switch d := constraint.Details.(type) {
			case *ast.CheckConstraintDefinition:
				panic("check constraint")
			case *ast.ForeignKeyDefinition:
				d.ReferencedTable.Name = ast.NewTableIdent(m.get(d.ReferencedTable.Name.String()))
				dep.deps = append(dep.deps, d.ReferencedTable.Name.String())
				d.ReferencedTable.Qualifier = ast.NewTableIdent(m.get(d.ReferencedTable.Qualifier.String()))

				for i, c := range d.Source {
					d.Source[i] = ast.NewColIdent(m.get(c.String()))
				}

				for i, c := range d.ReferencedColumns {
					d.ReferencedColumns[i] = ast.NewColIdent(m.get(c.String()))

				}
			default:
				panic(fmt.Sprintf("unknown constraint type: %T", d))
			}
		}

	default:
		panic(fmt.Sprintf("unexpected type: %T", n))
	}
	return dep
}

const letters = "abcdefghijklmnopqrstuvwxyz"
const capital = "ABCDEFGHIJKLMNOPQRSTuVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var src = rand.NewSource(time.Now().UnixNano())

func obfuscatedName(n int) string {
	if n < 3 {
		n = 3
	}
	sb := strings.Builder{}
	sb.Grow(n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(capital) {
			sb.WriteByte(capital[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return sb.String()
}
