package vet

import (
	"flag"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/analysis"

	"github.com/houqp/sqlvet/pkg/config"
	schema "github.com/houqp/sqlvet/pkg/schema"
)

// Analyzer implements a lightweight checker using go/analysis that inspects
// call sites to common SQL APIs and validates constant query strings.
// Note: Intentionally does not support string concatenation or non-constant
// expressions per analyzer-mode limitations.
var Analyzer = &analysis.Analyzer{
	Name: "sqlvet",
	Doc:  "Validate SQL query strings in calls to database/sql and sqlx APIs",
	Run:  run,
}

var (
	configPathFlag string
)

func init() {
	Analyzer.Flags.Init("sqlvet", flag.ContinueOnError)
	Analyzer.Flags.StringVar(&configPathFlag, "f", "", "path to sqlvet.toml (defaults to ./sqlvet.toml)")
}

// analyzer state loaded lazily
var (
	analyzerSchemaLoaded bool
	analyzerSchema       *Schema
)

// allowed packages to inspect, by import path
var allowedPkgPaths = map[string]struct{}{
	"database/sql":            {},
	"github.com/jmoiron/sqlx": {},
}

// function/method name to potential query argument positions
// Positions are zero-based in CallExpr.Args (receiver is implicit for methods)
var funcNameToQueryArgPositions = map[string][]int{
	// database/sql DB methods
	"Query":           {0},
	"QueryRow":        {0},
	"Exec":            {0},
	"QueryContext":    {1},
	"QueryRowContext": {1},
	"ExecContext":     {1},

	// sqlx DB methods
	"NamedExec":         {0},
	"NamedQuery":        {0},
	"NamedExecContext":  {1},
	"NamedQueryContext": {1},
	// Select/Get take destination first
	"Select":        {1},
	"Get":           {1},
	"SelectContext": {2},
	"GetContext":    {2},
}

func run(pass *analysis.Pass) (any, error) {
	// Load config/schema once per process; analyzer runs per package
	if !analyzerSchemaLoaded {
		tables := map[string]schema.Table{}
		// Resolve config path
		cfgPath := configPathFlag
		if cfgPath == "" {
			cfgPath = "sqlvet.toml"
		}
		// Use first file to resolve relative path
		if len(pass.Files) > 0 && !filepath.IsAbs(cfgPath) {
			f := pass.Fset.File(pass.Files[0].Pos())
			if f != nil {
				cfgPath = filepath.Join(filepath.Dir(f.Name()), cfgPath)
			}
		}
		cfg, err := config.Load(filepath.Dir(cfgPath))
		if err == nil && cfg.SchemaPath != "" {
			dbSchema, serr := schema.NewDbSchema(filepath.Join(filepath.Dir(cfgPath), cfg.SchemaPath))
			if serr == nil {
				tables = dbSchema.Tables
			}
		}
		analyzerSchema = &Schema{Tables: tables}
		analyzerSchemaLoaded = true
	}

	// Build ignore comment ranges
	ignoreNodes := collectIgnoreCommentNodes(pass)

	for _, file := range pass.Files {
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if shouldIgnoreNodeSimple(ignoreNodes, call.Lparen) {
				return true
			}

			name, pkgPath := resolveCallee(pass, call)
			if name == "" || pkgPath == "" {
				return true
			}
			if _, ok := allowedPkgPaths[pkgPath]; !ok {
				return true
			}
			positions, ok := funcNameToQueryArgPositions[name]
			if !ok {
				return true
			}

			for _, idx := range positions {
				if idx >= len(call.Args) {
					continue
				}
				arg := call.Args[idx]
				// Do not support string concatenation or non-constant expressions
				if _, isBinary := arg.(*ast.BinaryExpr); isBinary {
					continue
				}
				query, ok := constString(pass, arg)
				if !ok || strings.TrimSpace(query) == "" {
					continue
				}

				// Compile named queries and validate
				qs := &QuerySite{Query: query}
				handleQuery(NewContext(analyzerSchema.Tables), qs)
				if qs.Err != nil {
					reportPos := arg.Pos()
					pass.Reportf(reportPos, "%v", qs.Err)
				}
			}

			return true
		})
	}
	return nil, nil
}

func resolveCallee(pass *analysis.Pass, call *ast.CallExpr) (name string, pkgPath string) {
	switch fun := call.Fun.(type) {
	case *ast.SelectorExpr:
		// Method or qualified function call
		if sel := pass.TypesInfo.Selections[fun]; sel != nil {
			// method on a type
			if fn, ok := sel.Obj().(*types.Func); ok {
				return fn.Name(), fn.Pkg().Path()
			}
		}
		if obj, ok := pass.TypesInfo.Uses[fun.Sel]; ok {
			if fn, ok := obj.(*types.Func); ok && fn.Pkg() != nil {
				return fn.Name(), fn.Pkg().Path()
			}
		}
	case *ast.Ident:
		if obj, ok := pass.TypesInfo.Uses[fun]; ok {
			if fn, ok := obj.(*types.Func); ok && fn.Pkg() != nil {
				return fn.Name(), fn.Pkg().Path()
			}
		}
	}
	return "", ""
}

func constString(pass *analysis.Pass, e ast.Expr) (string, bool) {
	tv, ok := pass.TypesInfo.Types[e]
	if !ok || tv.Value == nil || tv.Value.Kind() != constant.String {
		return "", false
	}
	return constant.StringVal(tv.Value), true
}

func collectIgnoreCommentNodes(pass *analysis.Pass) []ast.Node {
	var nodes []ast.Node
	for _, f := range pass.Files {
		cmap := ast.NewCommentMap(pass.Fset, f, f.Comments)
		for node, cglist := range cmap {
			for _, cg := range cglist {
				if len(cg.List) == 0 {
					continue
				}
				ctext := cg.List[0].Text
				if !strings.HasPrefix(ctext, "//") {
					continue
				}
				ctext = strings.TrimSpace(ctext[2:])
				anno, err := ParseComment(ctext)
				if err != nil {
					continue
				}
				if anno.Ignore {
					nodes = append(nodes, node)
				}
			}
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Pos() < nodes[j].Pos() })
	return nodes
}

func shouldIgnoreNodeSimple(ignoreNodes []ast.Node, pos token.Pos) bool {
	if len(ignoreNodes) == 0 {
		return false
	}
	if pos < ignoreNodes[0].Pos() || pos > ignoreNodes[len(ignoreNodes)-1].End() {
		return false
	}
	for _, n := range ignoreNodes {
		if pos < n.End() && pos > n.Pos() {
			return true
		}
	}
	return false
}
