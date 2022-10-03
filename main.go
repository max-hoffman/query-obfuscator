package main

import (
	"bytes"
	"encoding/base32"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/OneOfOne/xxhash"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"io"
	"os"
	"path"
	"sort"
	"strings"
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
	flag.StringVar(&queryDir, "queryDir", "", "path to queries for obfuscating")
	flag.StringVar(&queryOutput, "queryOutput", "", "output path for obfuscated queries")
}

func main() {
	flag.Parse()
	ctx := sql.NewEmptyContext()
	_main(ctx, schemaInput, schemaOutput, queryDir, queryOutput)
}

func _main(ctx *sql.Context, schemaInput, schemaOutput, queryDir, queryOutput string) {
	mappings := newMapping()

	// do schemas the same way
	// use transform to do selects
	//
	obfuscateSchema(ctx, schemaInput, schemaOutput, mappings)
	applyMappingsToSelects(ctx, queryDir, queryOutput, mappings)
}

func applyMappingsToSelects(ctx *sql.Context, input, output string, m *mapping) {
	files, err := os.ReadDir(input)
	if err != nil {
		panic(err)
	}

	for _, f := range files {
		s, err := os.ReadFile(path.Join(input, f.Name()))
		if err != nil {
			panic(err)
		}

		remainder := string(s)
		var node sql.Node
		for len(remainder) > 0 {
			node, _, remainder, err = parse.ParseOne(ctx, remainder)
			scrapeNodeForAliases(node, m)
		}
	}

	b := bytes.Buffer{}
	replacements := m.replacements()
	for _, f := range files {
		s, err := os.ReadFile(path.Join(input, f.Name()))
		if err != nil {
			panic(err)
		}

		ret := string(s)
		for _, r := range replacements {
			ret = strings.ReplaceAll(ret, r.from, r.to)
		}
		b.WriteString(ret)
	}

	outFile, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	outFile.Write(b.Bytes())
}

func scrapeNodeForAliases(n sql.Node, m *mapping) {
	transform.NodeWithOpaque(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch n := n.(type) {
		case *plan.CreateTable:
			m.get(n.Name())

			ts := n.TableSpec()
			for _, c := range ts.Schema.Schema {
				m.get(c.Name)
			}
			for _, k := range ts.FkDefs {
				m.get(k.Name)
				m.get(k.ParentTable)
			}
			for _, i := range ts.IdxDefs {
				m.get(i.IndexName)
			}
			for _, c := range ts.ChDefs {
				m.get(c.Name)
			}
		case *plan.With:
			for _, e := range n.CTEs {
				m.get(e.Subquery.Name())
				scrapeNodeForAliases(e.Subquery, m)
			}
		case *plan.TableAlias:
			m.get(n.Name())
		case *plan.ResolvedTable:
			m.get(n.Name())
		case *plan.SubqueryAlias:
			m.get(n.Name())
		default:
		}
		return transform.NodeExprs(n, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			switch e := e.(type) {
			case *expression.UnresolvedFunction:
				if e.Window != nil {
					m.get(e.Window.Name)
				}
			case *expression.Literal:
				if val, ok := e.Value().(string); ok {
					return expression.NewLiteral(m.get(val), e.Type()), transform.NewTree, nil
				}
			case *expression.Star:
				if e.Table != "" {
					m.get(e.Table)
				}
			case *expression.Alias:
				if strings.Contains(e.Name(), "COUNT") {
					return e, transform.SameTree, nil
				}
				m.get(e.Name())
			case *plan.Subquery:
				scrapeNodeForAliases(e.Query, m)
			default:
			}
			return e, transform.SameTree, nil
		})
	})
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
		dep := replaceInDDL(stmt, mappings)
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

const obfLenCutoff = 3

func (m *mapping) get(s string) string {
	if s == "" {
		return ""
	}

	val, ok := m.m[s]
	if !ok {
		if len(s) <= obfLenCutoff {
			return s
		}
		val = obfuscateName(s)
		if _, ok = m.m[val]; ok {
			panic("hash collision")
		}
		m.m[s] = val
	}
	return val
}

const obfSeed = 10

var b32Numerals = map[string]string{
	"2": "T",
	"3": "T",
	"4": "F",
	"5": "F",
	"6": "S",
	"7": "S",
}

func obfuscateName(s string) string {
	hash := xxhash.ChecksumString32S(s, obfSeed)
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, hash)
	if err != nil {
		panic(err)
	}
	b := buf.Bytes()
	dst := make([]byte, base32.StdEncoding.EncodedLen(len(b)))
	base32.StdEncoding.Encode(dst, buf.Bytes())
	ret := string(dst)
	if sub, ok := b32Numerals[ret[0:1]]; ok {
		ret = sub + ret[1:]
	}
	return ret[:5]
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

func replaceInDDL(stmt ast.Statement, m *mapping) *tableDep {
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
			constraint.Name = m.get(constraint.Name)
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
