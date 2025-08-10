package vet

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/houqp/sqlvet/pkg/schema"
	pg_wasm "github.com/wasilibs/go-pgquery"
)

type Schema struct {
	Tables map[string]schema.Table
}

func NewContext(tables map[string]schema.Table) VetContext {
	return VetContext{
		Schema:      Schema{Tables: tables},
		InnerSchema: Schema{Tables: map[string]schema.Table{}},
	}
}

type VetContext struct {
	Schema      Schema
	InnerSchema Schema
	UsedTables  []TableUsed
}

type TableUsed struct {
	Name  string
	Alias string
}

type ColumnUsed struct {
	Column   string
	Table    string
	Location int32
}

type QueryParam struct {
	Number int32
	// TODO: also store related column type info for analysis
}

type PostponedNodes struct {
	RangeSubselectNodes []any
}

func (p *PostponedNodes) Parse(ctx VetContext, parseRe *ParseResult) (err error) {
	for _, r := range p.RangeSubselectNodes {
		if err = parseRangeSubselect(ctx, r, parseRe); err != nil {
			return err
		}
	}
	return nil
}

func (p *PostponedNodes) Append(other *PostponedNodes) {
	if other == nil {
		return
	}
	p.RangeSubselectNodes = append(p.RangeSubselectNodes, other.RangeSubselectNodes...)
}

type ParseResult struct {
	Columns []ColumnUsed
	Tables  []TableUsed
	Params  []QueryParam

	PostponedNodes *PostponedNodes
}

// insert query param based on parameter number and avoid deduplications
func AddQueryParam(target *[]QueryParam, param QueryParam) {
	params := *target
	for i, p := range params {
		if p.Number == param.Number {
			// avoid duplicate params
			return
		} else if p.Number > param.Number {
			*target = append(
				params[:i],
				append(
					[]QueryParam{param},
					params[i:]...,
				)...,
			)
			return
		}
	}
	*target = append(params, param)
}

func AddQueryParams(target *[]QueryParam, params []QueryParam) {
	for _, p := range params {
		AddQueryParam(target, p)
	}
}

func DebugQuery(q string) {
	b, _ := pg_wasm.ParseToJSON(q)
	var pretty bytes.Buffer
	json.Indent(&pretty, []byte(b), "\t", "  ")
	fmt.Println("query: " + q)
	fmt.Println("parsed query: " + pretty.String())
}

// rangeVarToTableUsed is not used in JSON mode

/* =========================================================================================================
	this is where the rewrite to nil checks rather than type refleciton starts
 ========================================================================================================= */

// return nil if no specific column is being referenced
// columnRefToColumnUsed is unused in JSON mode
func columnRefToColumnUsed(_ any) *ColumnUsed { return nil }

func getUsedTablesFromJoinArg(_ any) []TableUsed { return []TableUsed{} }

// extract used tables from FROM clause and JOIN clauses
// TODO ? maybe this should be moved to parseExpression() and be collected in ParseResult
func getUsedTablesFromSelectStmt(_ interface{}) []TableUsed { return []TableUsed{} }

func parseFromClause(_ VetContext, _ interface{}, _ *ParseResult) error { return nil }

func getUsedColumnsFromReturningList(_ interface{}) []ColumnUsed { return []ColumnUsed{} }

func validateTable(ctx VetContext, tname string, notReadOnly bool) error {
	if ctx.Schema.Tables == nil {
		return nil
	}
	t, ok := ctx.Schema.Tables[tname]
	if !ok {
		return fmt.Errorf("invalid table name: %s", tname)
	}
	if notReadOnly && t.ReadOnly {
		return fmt.Errorf("read-only table: %s", tname)
	}
	return nil
}

func validateTableColumns(ctx VetContext, tables []TableUsed, cols []ColumnUsed) error {
	if ctx.Schema.Tables == nil || ctx.InnerSchema.Tables == nil {
		return nil
	}

	var ok bool
	usedTables := map[string]schema.Table{}
	for _, tu := range tables {
		usedTables[tu.Name], ok = ctx.InnerSchema.Tables[tu.Name]
		if !ok {
			usedTables[tu.Name], ok = ctx.Schema.Tables[tu.Name]
			if !ok {
				return fmt.Errorf("invalid table name: %s", tu.Name)
			}
		}
		if tu.Alias != "" {
			usedTables[tu.Alias] = usedTables[tu.Name]
		}
	}

	for _, col := range cols {
		if col.Table != "" {
			table, ok := usedTables[col.Table]
			if !ok {
				return fmt.Errorf("table `%s` not available for query", col.Table)
			}
			_, ok = table.Columns[col.Column]
			if !ok {
				return fmt.Errorf("column `%s` is not defined in table `%s`", col.Column, col.Table)
			}
		} else {
			// no table prefix, try all tables
			found := false
			for _, table := range usedTables {
				_, ok = table.Columns[col.Column]
				if ok {
					found = true
					break
				}
			}
			if !found {
				if len(usedTables) == 1 {
					// to make error message more useful, if only one table is
					// referenced in the query, it's safe to assume user only
					// want to use columns from that table.
					return fmt.Errorf(
						"column `%s` is not defined in table `%s`",
						col.Column, tables[0].Name)
				} else {
					return fmt.Errorf(
						"column `%s` is not defined in any of the table available for query",
						col.Column)
				}
			}
		}
	}

	return nil
}

func validateInsertValues(_ VetContext, _ []ColumnUsed, _ interface{}) error { return nil }

func parseWindowDef(_ VetContext, _ interface{}, _ *ParseResult) error { return nil }

// recursive function to parse expressions including nested expressions
func parseExpression(_ VetContext, _ interface{}, _ *ParseResult) error { return nil }

func parseSublink(_ VetContext, _ interface{}, _ *ParseResult) error { return nil }

func parseRangeSubselect(_ VetContext, _ interface{}, _ *ParseResult) error { return nil }

func parseJoinExpr(_ VetContext, _ interface{}, _ *ParseResult) error { return nil }

// find used column names from where clause
func parseWhereClause(_ VetContext, _ interface{}, _ *ParseResult) error { return nil }

func parseUsingClause(_ VetContext, _ interface{}, _ *ParseResult) error { return nil }

func getUsedColumnsFromNodeList(_ interface{}) []ColumnUsed { return []ColumnUsed{} }

func getUsedColumnsFromSortClause(_ interface{}) []ColumnUsed { return []ColumnUsed{} }

func validateSelectStmt(_ VetContext, _ interface{}) ([]QueryParam, []schema.Column, error) {
	return nil, nil, fmt.Errorf("not used")
}

func validateUpdateStmt(_ VetContext, _ interface{}) ([]QueryParam, []ColumnUsed, error) {
	return nil, nil, fmt.Errorf("not used")
}

func validateInsertStmt(_ VetContext, _ interface{}) ([]QueryParam, []ColumnUsed, error) {
	return nil, nil, fmt.Errorf("not used")
}

func validateDeleteStmt(_ VetContext, _ interface{}) ([]QueryParam, []ColumnUsed, error) {
	return nil, nil, fmt.Errorf("not used")
}

func parseCTE(_ VetContext, _ interface{}) error { return nil }

func ValidateSqlQuery(ctx VetContext, queryStr string) ([]QueryParam, error) {
	j, err := pg_wasm.ParseToJSON(queryStr)
	if err != nil {
		return nil, err
	}
	root, err := parseJSONTree(j)
	if err != nil {
		return nil, err
	}
	params, _, err := jsonValidateQuery(ctx, root)
	return params, err
}

func ValidateSqlQueries(ctx VetContext, queryStr string) ([][]QueryParam, error) {
	j, err := pg_wasm.ParseToJSON(queryStr)
	if err != nil {
		return nil, err
	}
	root, err := parseJSONTree(j)
	if err != nil {
		return nil, err
	}
	stmts := asList(root["stmts"])
	var out [][]QueryParam
	for _, s := range stmts {
		one := map[string]any{"stmts": []any{s}}
		qp, _, err := jsonValidateQuery(ctx, one)
		if err != nil {
			return nil, err
		}
		out = append(out, qp)
	}
	return out, nil
}

func validateSqlQuery(_ VetContext, _ any) ([]QueryParam, []ColumnUsed, error) {
	return nil, nil, fmt.Errorf("not used")
}
