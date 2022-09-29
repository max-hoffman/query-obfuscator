package test

import (
	"bytes"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

func TestObfuscate(t *testing.T) {
	// read create statements first
	// create mappings
	//  - table names -> fake names
	//  - column names -> fake names
	// print to file

	// parse select queries next
	// scan for table and column references
	// convert names from mapping
	// print out into query strings again

	ctx := sql.NewEmptyContext()
	schemasInput := "/Users/max-hoffman/Documents/dolthub/databases/turbine/new_model_db/schemas.sql"
	schemasOutput := ""
	queryDir := ""
	queryOutput := ""

	mappings := new(mapping)
	obfuscateSchema(ctx, schemasInput, schemasOutput, mappings)
	applyMappingsToQueries(queryDir, queryOutput, mappings)

	//parseQueriesInDir(ctx, schemasDir, schemasOutput, func(q string, stmt ast.Statement) string {
	//	replaceInStatement(stmt, mappings)
	//	var b strings.Builder
	//	ast.Append(&b, stmt)
	//	return b.String()
	//})
	//
	//parseQueriesInDir(ctx, queryDir, queryOutput, func(q string, stmt ast.Statement) string {
	//	// regex replace variables found in mappings
	//	ret := q
	//	for k, v := range mappings.m {
	//		ret = strings.ReplaceAll(ret, k, v)
	//	}
	//	return ret
	//})
}

func obfuscateSchema(ctx *sql.Context, schemaInput, schemaOutput string, mappings *mapping) {
	// read schema file
	s, err := os.ReadFile(schemaInput)
	if err != nil {
		panic(err)
	}
	// create and apply mappings
	var b strings.Builder
	var stmt ast.Statement
	remainder := string(s)
	var parsed string

	for len(remainder) > 0 {
		var ri int
		stmt, ri, err = ast.ParseOne(remainder)
		if ri != 0 && ri < len(remainder) {
			parsed = remainder[:ri]
			parsed = strings.TrimSpace(parsed)
			if strings.HasSuffix(parsed, ";") {
				parsed = parsed[:len(parsed)-1]
			}
			remainder = remainder[ri:]
		}
		stmt, err = ast.Parse(string(s))
		if err != nil {
			panic(err)
		}
		replaceInStatement(stmt, mappings)
		ast.Append(&b, stmt)
		b.WriteString(";\n")
	}

	// write obfuscated back
	outFile, err := os.OpenFile(schemaOutput, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	outFile.WriteString(b.String())
}

func applyMappingsToQueries(queryDir, queryOutput string, mappings *mapping) {
	files, err := os.ReadDir(queryDir)
	if err != nil {
		panic(err)
	}

	b := bytes.Buffer{}
	for _, f := range files {
		s, err := os.ReadFile(f.Name())
		if err != nil {
			panic(err)
		}

		ret := s
		for k, v := range mappings.m {
			bytes.ReplaceAll(ret, []byte(k), []byte(v))
		}
		b.Write(ret)
		b.Write([]byte(";\n"))
	}

	outFile, err := os.OpenFile(queryOutput, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}
	outFile.Write(b.Bytes())
}

// parseQueriesInDir attempts to read every file in the directory, executing a callback function
// for every ast.Statement we discover. We write the resulting ast.Statement back to an output
// file.
func parseQueriesInDir(ctx *sql.Context, dir string, output string, cb func(q string, stmt ast.Statement)) string {
	files, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	var stmt ast.Statement
	var q string

	var b strings.Builder
	outFile, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		panic(err)
	}

	for _, f := range files {
		s, err := os.ReadFile(f.Name())
		if err != nil {
			panic(err)
		}

		remainder := string(s)
		var parsed string
		for len(remainder) > 0 {
			var ri int
			stmt, ri, err = ast.ParseOne(remainder)
			if ri != 0 && ri < len(remainder) {
				parsed = remainder[:ri]
				parsed = strings.TrimSpace(parsed)
				if strings.HasSuffix(parsed, ";") {
					parsed = parsed[:len(parsed)-1]
				}
				remainder = remainder[ri:]
			}
			stmt, err = ast.Parse(string(s))
			if err != nil {
				panic(err)
			}
			q = cb(parsed, stmt)

			b.WriteString(fmt.Sprintf("%s;", q))
		}
	}

	outFile.WriteString(b.String())
	outFile.Close()
}

type mapping struct {
	m map[string]string
}

func (m *mapping) get(s string) string {
	val, ok := m.m[s]
	if !ok {
		for {
			val := obfuscatedName(len(s))
			if _, ok := m.m[val]; !ok {
				break
			}
		}
		m.m[val] = s
	}
	return val
}

func replaceInStatement(stmt ast.Statement, m *mapping) {
	switch n := stmt.(type) {
	case *ast.DDL:
		// expect  create table
		n.Table.Name = ast.NewTableIdent(m.get(n.Table.Name.String()))
		n.Table.Qualifier = ast.NewTableIdent(m.get(n.Table.Qualifier.String()))

		for _, col := range n.TableSpec.Columns {
			col.Name = ast.NewColIdent(m.get(col.Name.String()))
		}

		for _, idx := range n.TableSpec.Indexes {
			for _, col := range idx.Columns {
				col.Column = ast.NewColIdent(m.get(col.Column.String()))
			}
		}

		for _, constraint := range n.TableSpec.Constraints {
			constraint.Name = obfuscatedName(len(constraint.Name))
		}

	case *ast.Select:
		if n.With != nil {
			for _, cte := range n.With.Ctes {
				cte, ok := cte.(*ast.CommonTableExpr)
				if !ok {
					panic(fmt.Sprintf("unexpected CTE child: %T", cte))
				}
				sq, ok := cte.AliasedTableExpr.Expr.(*ast.Subquery)
				replaceInStatement(sq.Select, m)
				for i, _ := range sq.Columns {
					sq.Columns[i] = ast.NewColIdent(obfuscatedName(1))
				}
			}
		}

		if n.SelectExprs != nil {

		}

		if n.From != nil {

		}

		if n.Where != nil {

		}

		if n.GroupBy != nil {

		}

		if n.Having != nil {

		}

		if n.Window != nil {

		}

		if n.OrderBy != nil {
			for _, e := range n.OrderBy {
				replaceInExpr(e.Expr, m)
			}
		}

	default:
		panic(fmt.Sprintf("unexpected type: %T", n))
	}
}

func replaceInExpr(e ast.Expr, m *mapping) ast.Expr {
	switch e := e.(type) {
	case *ast.AndExpr:
		return &ast.AndExpr{
			Left:  replaceInExpr(e.Left, m),
			Right: replaceInExpr(e.Right, m),
		}
	case *ast.OrExpr:
		return &ast.OrExpr{
			Left:  replaceInExpr(e.Left, m),
			Right: replaceInExpr(e.Right, m),
		}
	case *ast.XorExpr:
		return &ast.XorExpr{
			Left:  replaceInExpr(e.Left, m),
			Right: replaceInExpr(e.Right, m),
		}
	case *ast.NotExpr:
		return &ast.NotExpr{
			Expr: replaceInExpr(e.Expr, m),
		}
	case *ast.ParenExpr:
		return &ast.ParenExpr{
			Expr: replaceInExpr(e.Expr, m),
		}
	case *ast.ComparisonExpr:
		return &ast.ComparisonExpr{
			Left:     replaceInExpr(e.Left, m),
			Right:    replaceInExpr(e.Right, m),
			Operator: e.Operator,
			Escape:   e.Escape,
		}
	case *ast.RangeCond:
		return &ast.RangeCond{}
	case *ast.IsExpr:
		return &ast.IsExpr{
			Expr:     replaceInExpr(e.Expr, m),
			Operator: e.Operator,
		}
	case *ast.ExistsExpr:
		return &ast.ExistsExpr{}
	case *ast.SQLVal:
		return e
	case *ast.NullVal:
		return e
	case *ast.BoolVal:
		return e
	case *ast.ColName:
		return &ast.ColName{
			Name: ast.NewColIdent(m.get(e.Name.String())),
			Qualifier: ast.TableName{
				Name:      ast.NewTableIdent(m.get(e.Qualifier.Name.String())),
				Qualifier: ast.NewTableIdent(m.get(e.Qualifier.Qualifier.String())),
			},
		}
	case *ast.ValTuple:
		return e
	case *ast.Subquery:
		return &ast.Subquery{}
	case *ast.ListArg:
		return e
	case *ast.BinaryExpr:
		return &ast.BinaryExpr{
			Left:  replaceInExpr(e.Left, m),
			Right: replaceInExpr(e.Right, m),
		}
	case *ast.UnaryExpr:
		return &ast.UnaryExpr{
			Expr: replaceInExpr(e.Expr, m),
		}
	case *ast.IntervalExpr:
		return &ast.IntervalExpr{}
	case *ast.CollateExpr:
		return &ast.CollateExpr{}
	case *ast.FuncExpr:
		return &ast.FuncExpr{}
	case *ast.TimestampFuncExpr:
		return &ast.TimestampFuncExpr{}
	case *ast.CurTimeFuncExpr:
		return &ast.CurTimeFuncExpr{}
	case *ast.CaseExpr:
		return &ast.CaseExpr{}
	case *ast.ValuesFuncExpr:
		return &ast.ValuesFuncExpr{}
	case *ast.ConvertExpr:
		return &ast.ConvertExpr{}
	case *ast.SubstrExpr:
		return &ast.SubstrExpr{}
	case *ast.TrimExpr:
		return &ast.TrimExpr{}
	case *ast.ConvertUsingExpr:
		return &ast.ConvertUsingExpr{}
	case *ast.MatchExpr:
		return &ast.MatchExpr{}
	case *ast.GroupConcatExpr:
		return &ast.GroupConcatExpr{}
	case *ast.Default:
		return &ast.Default{}
	default:
		return e
	}
}

const letters = "abcdefghijklmnopqrstuvwxyz"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var src = rand.NewSource(time.Now().UnixNano())

func obfuscatedName(n int) string {
	sb := strings.Builder{}
	sb.Grow(n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letters) {
			sb.WriteByte(letters[idx])
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return sb.String()
}
