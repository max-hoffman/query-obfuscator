package main

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"os"
	"testing"
)

func TestObfuscate(t *testing.T) {
	ctx := sql.NewEmptyContext()
	schemasInput := "/Users/max-hoffman/Documents/dolthub/databases/turbine/new_model_db/schemas.sql"
	schemasOutput := "/Users/max-hoffman/go/src/github.com/max-hoffman/query-obfuscator/schemas_obf.sql"
	q1Dir := "/Users/max-hoffman/Downloads/dolt_query_pack/build_in_queries/"
	q1Output := "/Users/max-hoffman/go/src/github.com/max-hoffman/query-obfuscator/build_in_queries_obf.sql"
	q2Dir := "/Users/max-hoffman/Downloads/dolt_query_pack/exp_setup_queries/"
	q2Output := "/Users/max-hoffman/go/src/github.com/max-hoffman/query-obfuscator/exp_setup_queries_obf.sql"

	mappings := newMapping()
	obfuscateSchema(ctx, schemasInput, schemasOutput, mappings)
	applyMappingsToQueries(q1Dir, q1Output, mappings)
	applyMappingsToQueries(q2Dir, q2Output, mappings)
}

func scanQueries(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, ';'); i >= 0 {
		// We have a full newline-terminated line.
		return i + 1, data[0:i], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}

func TestWriteQueriesAsPlanTests(t *testing.T) {
	queries := "/Users/max-hoffman/go/src/github.com/max-hoffman/query-obfuscator/exp_setup_queries_obf.sql"
	file, _ := os.Open(queries)
	scanner := bufio.NewScanner(file)
	scanner.Split(scanQueries)

	b := &bytes.Buffer{}
	fmt.Fprintf(b, "")

	for scanner.Scan() {
		fmt.Fprintf(b, "  {\n  Query: `%s`,\n  },\n", scanner.Bytes())
	}

	tmpfile, err := os.CreateTemp("", "example")
	if err != nil {
		panic(err)
	}
	fmt.Println(tmpfile.Name())
	tmpfile.Write(b.Bytes())
}
