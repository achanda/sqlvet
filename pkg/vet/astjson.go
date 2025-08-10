package vet

import (
	"encoding/json"
	"errors"
	"fmt"

	schema "github.com/houqp/sqlvet/pkg/schema"
)

// JSON-based AST walker for go-pgquery

type jsonNode map[string]any

func parseJSONTree(jsonText string) (map[string]any, error) {
	var root map[string]any
	if err := json.Unmarshal([]byte(jsonText), &root); err != nil {
		return nil, err
	}
	return root, nil
}

func getStringField(obj map[string]any, key string) string {
	if v, ok := obj[key]; ok {
		if s, ok2 := v.(string); ok2 {
			return s
		}
	}
	return ""
}

func getNumberField(obj map[string]any, key string) int32 {
	if obj == nil {
		return 0
	}
	if v, ok := obj[key]; ok {
		if num, ok := v.(float64); ok {
			return int32(num)
		}
	}
	return 0
}

func getBoolField(obj map[string]any, key string) bool {
	if obj == nil {
		return false
	}
	if v, ok := obj[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

func asNode(v any) jsonNode {
	m, _ := v.(map[string]any)
	return m
}

func asList(v any) []any {
	a, _ := v.([]any)
	return a
}

func jNode(m map[string]any, keys ...string) jsonNode {
	for _, k := range keys {
		if n := asNode(m[k]); n != nil {
			return n
		}
	}
	return nil
}

func jList(m map[string]any, keys ...string) []any {
	for _, k := range keys {
		if l := asList(m[k]); l != nil {
			return l
		}
	}
	return nil
}

func getRelationRangeVar(m map[string]any) jsonNode {
	if rv := asNode(m["RangeVar"]); rv != nil {
		return rv
	}
	return m
}

func nodeType(n jsonNode) (string, jsonNode) {
	for k, v := range n {
		return k, asNode(v)
	}
	return "", nil
}

// ---------------------- Collectors ----------------------

func jsonValidateQuery(ctx VetContext, root map[string]any) ([]QueryParam, []ColumnUsed, error) {
	stmts := asList(root["stmts"])
	if len(stmts) == 0 {
		return nil, nil, errors.New("empty statement")
	}
	if len(stmts) > 1 {
		return nil, nil, fmt.Errorf("query contained more than one statement")
	}
	stmtObj := asNode(stmts[0])
	stmt := asNode(stmtObj["stmt"])
	return jsonValidateNode(ctx, stmt)
}

func jsonValidateNode(ctx VetContext, n jsonNode) ([]QueryParam, []ColumnUsed, error) {
	kind, body := nodeType(n)
	switch kind {
	case "SelectStmt":
		return jsonValidateSelect(ctx, body)
	case "UpdateStmt":
		return jsonValidateUpdate(ctx, body)
	case "InsertStmt":
		return jsonValidateInsert(ctx, body)
	case "DeleteStmt":
		return jsonValidateDelete(ctx, body)
	default:
		return nil, nil, fmt.Errorf("unsupported statement: %s", kind)
	}
}

func jsonValidateSelect(ctx VetContext, sel map[string]any) ([]QueryParam, []ColumnUsed, error) {
	usedCols := []ColumnUsed{}
	queryParams := []QueryParam{}
	localTables := []TableUsed{}

	// Create a local copy of the context tables to avoid modifying the original
	localContextTables := make([]TableUsed, len(ctx.UsedTables))
	copy(localContextTables, ctx.UsedTables)

	// Create a ParseResult to store postponed nodes and other results
	re := &ParseResult{}

	// WITH
	if with := jNode(sel, "with_clause", "withClause", "withClause"); with != nil {
		if err := jsonParseCTE(ctx, with); err != nil {
			return nil, nil, err
		}
	}

	// FROM/JOIN tables
	from := jList(sel, "from_clause", "fromClause", "fromClause")
	for _, it := range from {
		re := &ParseResult{}
		if err := jsonParseFromClause(ctx, asNode(it), re); err != nil {
			return nil, nil, err
		}
		if len(re.Columns) > 0 {
			usedCols = append(usedCols, re.Columns...)
		}
		if len(re.Tables) > 0 {
			localTables = append(localTables, re.Tables...)
			// Register table aliases in the local context for column validation
			for _, table := range re.Tables {
				if table.Alias != "" {
					localContextTables = append(localContextTables, table)
				}
			}
		}
		if len(re.Params) > 0 {
			AddQueryParams(&queryParams, re.Params)
		}
	}

	// Targets
	for _, it := range jList(sel, "target_list", "targetList", "targetList") {
		target := asNode(asNode(it)["ResTarget"])
		if target == nil {
			continue
		}
		re := &ParseResult{}
		if err := jsonParseExpr(ctx, asNode(target["val"]), re); err != nil {
			return nil, nil, err
		}
		if len(re.Columns) > 0 {
			usedCols = append(usedCols, re.Columns...)
		}
		if len(re.Params) > 0 {
			AddQueryParams(&queryParams, re.Params)
		}
	}

	// WHERE
	if wc := jNode(sel, "where_clause", "whereClause", "whereClause"); wc != nil {
		re := &ParseResult{}
		if err := jsonParseExpr(ctx, wc, re); err != nil {
			return nil, nil, fmt.Errorf("invalid WHERE clause: %w", err)
		}
		if len(re.Columns) > 0 {
			usedCols = append(usedCols, re.Columns...)
		}
		if len(re.Params) > 0 {
			AddQueryParams(&queryParams, re.Params)
		}
	}

	// GROUP BY
	for _, it := range jList(sel, "group_clause", "groupClause", "groupClause") {
		usedCols = append(usedCols, jsonGetColumnsFromNodeList(asNode(it))...)
	}

	// HAVING
	if hv := jNode(sel, "having_clause", "havingClause", "havingClause"); hv != nil {
		re := &ParseResult{}
		if err := jsonParseExpr(ctx, hv, re); err != nil {
			return nil, nil, err
		}
		if len(re.Columns) > 0 {
			usedCols = append(usedCols, re.Columns...)
		}
		if len(re.Params) > 0 {
			AddQueryParams(&queryParams, re.Params)
		}
	}

	// WINDOW
	if wc := jList(sel, "window_clause", "windowClause", "windowClause"); len(wc) > 0 {
		re := &ParseResult{}
		if err := jsonParseExpr(ctx, asNode(wc[0]), re); err != nil {
			return nil, nil, err
		}
		usedCols = append(usedCols, re.Columns...)
		AddQueryParams(&queryParams, re.Params)
	}

	// ORDER BY
	if sc := jList(sel, "sort_clause", "sortClause", "sortClause"); len(sc) > 0 {
		usedCols = append(usedCols, jsonGetColumnsFromSortClause(sc)...)
	}

	// Process postponed nodes (like LATERAL subqueries) after all FROM clause processing
	if re.PostponedNodes != nil {
		if err := re.PostponedNodes.Parse(ctx, re); err != nil {
			return nil, nil, err
		}
		// Add any additional tables and columns from postponed nodes
		if len(re.Tables) > 0 {
			localTables = append(localTables, re.Tables...)
		}
		if len(re.Columns) > 0 {
			usedCols = append(usedCols, re.Columns...)
		}
		if len(re.Params) > 0 {
			AddQueryParams(&queryParams, re.Params)
		}
	}

	/*DEBUG*/
	_ = ctx.UsedTables
	// Combine local tables with context tables for validation
	allTables := append([]TableUsed{}, localTables...)
	allTables = append(allTables, localContextTables...)

	// Debug: print what tables we have
	fmt.Printf("DEBUG: localTables: %+v\n", localTables)
	fmt.Printf("DEBUG: ctx.UsedTables: %+v\n", ctx.UsedTables)
	fmt.Printf("DEBUG: allTables: %+v\n", allTables)

	return queryParams, usedCols, validateTableColumns(ctx, allTables, usedCols)
}

func jsonValidateUpdate(ctx VetContext, up map[string]any) ([]QueryParam, []ColumnUsed, error) {
	if with := jNode(up, "with_clause", "withClause"); with != nil {
		if err := jsonParseCTE(ctx, with); err != nil {
			return nil, nil, err
		}
	}

	rel := asNode(up["relation"])
	rv := getRelationRangeVar(rel)
	tableName := getStringField(rv, "relname")
	if err := validateTable(ctx, tableName, true); err != nil {
		return nil, nil, err
	}

	tableAlias := getStringField(asNode(rv["alias"]), "aliasname")

	usedTables := []TableUsed{{Name: tableName}}
	usedCols := []ColumnUsed{}
	queryParams := []QueryParam{}

	// Process target list
	for _, it := range jList(up, "target_list", "targetList") {
		rt := asNode(asNode(it)["ResTarget"])
		if rt == nil {
			continue
		}
		usedCols = append(usedCols, ColumnUsed{Table: tableName, Column: getStringField(rt, "name"), Location: getNumberField(rt, "location")})
		if val := asNode(rt["val"]); val != nil {
			switch t, _ := nodeType(val); t {
			case "ColumnRef":
				if cu := jsonColumnRefToColumnUsed(asNode(val["ColumnRef"])); cu != nil {
					usedCols = append(usedCols, *cu)
				}
			case "ParamRef":
				AddQueryParam(&queryParams, QueryParam{Number: getNumberField(asNode(val["ParamRef"]), "number")})
			}
		}
	}

	// Process FROM clause
	if fromClause := jList(up, "from_clause", "fromClause"); len(fromClause) > 0 {
		for _, it := range fromClause {
			n := asNode(it)
			if n == nil {
				continue
			}
			if rv := asNode(n["RangeVar"]); rv != nil {
				usedTables = append(usedTables, jsonRangeVarToTableUsed(rv))
			}
			if je := asNode(n["JoinExpr"]); je != nil {
				usedTables = append(usedTables, jsonGetTablesFromSelectStmt([]any{je["larg"], je["rarg"]})...)
			}
		}
	}

	if wc := jNode(up, "where_clause", "whereClause"); wc != nil {
		re := &ParseResult{}
		if err := jsonParseExpr(ctx, wc, re); err != nil {
			return nil, nil, fmt.Errorf("invalid WHERE clause: %w", err)
		}
		usedCols = append(usedCols, re.Columns...)
		AddQueryParams(&queryParams, re.Params)
	}

	if ret := jList(up, "returning_list", "returningList"); len(ret) > 0 {
		usedCols = append(usedCols, jsonGetColumnsFromReturningList(ret)...)
	}

	if len(usedCols) > 0 {
		usedTables = append(usedTables, TableUsed{Name: tableName, Alias: tableAlias})
		if err := validateTableColumns(ctx, usedTables, usedCols); err != nil {
			return nil, nil, err
		}
	}
	return queryParams, usedCols, nil
}

func jsonValidateInsert(ctx VetContext, ins map[string]any) ([]QueryParam, []ColumnUsed, error) {
	if with := jNode(ins, "with_clause", "withClause"); with != nil {
		if err := jsonParseCTE(ctx, with); err != nil {
			return nil, nil, err
		}
	}
	rel := asNode(ins["relation"])
	rv := getRelationRangeVar(rel)
	tableName := getStringField(rv, "relname")
	if err := validateTable(ctx, tableName, true); err != nil {
		return nil, nil, err
	}
	usedTables := []TableUsed{{Name: tableName}}

	targetCols := []ColumnUsed{}
	for _, it := range asList(ins["cols"]) {
		rt := asNode(asNode(it)["ResTarget"])
		if rt == nil {
			continue
		}
		targetCols = append(targetCols, ColumnUsed{Table: tableName, Column: getStringField(rt, "name"), Location: getNumberField(rt, "location")})
	}

	values := []jsonNode{}
	usedCols := append([]ColumnUsed{}, targetCols...)
	queryParams := []QueryParam{}

	selNode := jNode(ins, "select_stmt", "selectStmt")
	sel := asNode(selNode["SelectStmt"])
	if sel == nil {
		sel = selNode
	}
	if sel == nil {
		return nil, nil, errors.New("missing select_stmt")
	}
	if vls := func() any {
		if v, ok := sel["values_lists"]; ok {
			return v
		}
		return sel["valuesLists"]
	}(); vls != nil {
		for _, list := range asList(vls) {
			items := asList(asNode(list)["List"].(map[string]any)["items"]) // list.List.items
			// Ensure values count matches target columns
			if len(items) != len(targetCols) {
				return nil, nil, fmt.Errorf("column count %d doesn't match value count %d", len(targetCols), len(items))
			}
			for _, vnode := range items {
				re := &ParseResult{}
				if err := jsonParseExpr(ctx, asNode(vnode), re); err != nil {
					return nil, nil, fmt.Errorf("invalid value list: %w", err)
				}
				if len(re.Columns) > 0 {
					usedCols = append(usedCols, re.Columns...)
				}
				if len(re.Params) > 0 {
					AddQueryParams(&queryParams, re.Params)
				}
				values = append(values, asNode(vnode))
			}
		}
	} else {
		usedTables = append(usedTables, jsonGetTablesFromSelectStmt(jList(sel, "from_clause", "fromClause"))...)
		for _, fc := range jList(sel, "from_clause", "fromClause") {
			re := &ParseResult{}
			if err := jsonParseFromClause(ctx, asNode(fc), re); err != nil {
				return nil, nil, err
			}
			if len(re.Columns) > 0 {
				usedCols = append(usedCols, re.Columns...)
			}
			if len(re.Params) > 0 {
				AddQueryParams(&queryParams, re.Params)
			}
		}
		if wc := jNode(sel, "where_clause", "whereClause"); wc != nil {
			re := &ParseResult{}
			if err := jsonParseExpr(ctx, wc, re); err != nil {
				return nil, nil, err
			}
			if len(re.Columns) > 0 {
				usedCols = append(usedCols, re.Columns...)
			}
			if len(re.Params) > 0 {
				AddQueryParams(&queryParams, re.Params)
			}
		}
		for _, it := range jList(sel, "target_list", "targetList") {
			target := asNode(asNode(it)["ResTarget"])["val"]
			values = append(values, asNode(target))
			tv := asNode(target)
			if tv == nil {
				continue
			}
			if _, ok := tv["ColumnRef"]; ok {
				if cu := jsonColumnRefToColumnUsed(asNode(tv["ColumnRef"])); cu != nil {
					usedCols = append(usedCols, *cu)
				}
			} else if sl := asNode(tv["SubLink"]); sl != nil {
				q := asNode(sl["subselect"]) // Node
				qp, _, err := jsonValidateSelect(ctx, asNode(q["SelectStmt"]))
				if err != nil {
					return nil, nil, fmt.Errorf("invalid SELECT query in value list: %w", err)
				}
				if len(qp) > 0 {
					AddQueryParams(&queryParams, qp)
				}
			}
		}
	}

	if ret := jList(ins, "returning_list", "returningList"); len(ret) > 0 {
		usedCols = append(usedCols, jsonGetColumnsFromReturningList(ret)...)
	}
	if err := validateTableColumns(ctx, usedTables, usedCols); err != nil {
		return nil, nil, err
	}
	if err := validateInsertValues(ctx, targetCols, nil /*unused in JSON path*/); err != nil { /* keep same behavior */
	}
	return queryParams, usedCols, nil
}

func jsonValidateDelete(ctx VetContext, del map[string]any) ([]QueryParam, []ColumnUsed, error) {
	if with := jNode(del, "with_clause", "withClause"); with != nil {
		if err := jsonParseCTE(ctx, with); err != nil {
			return nil, nil, err
		}
	}
	rel := asNode(del["relation"])
	rv := getRelationRangeVar(rel)
	tableName := getStringField(rv, "relname")
	if err := validateTable(ctx, tableName, true); err != nil {
		return nil, nil, err
	}

	usedCols := []ColumnUsed{}
	queryParams := []QueryParam{}

	usedTables := []TableUsed{}

	if wc := jNode(del, "where_clause", "whereClause"); wc != nil {
		re := &ParseResult{}
		if err := jsonParseExpr(ctx, wc, re); err != nil {
			return nil, nil, fmt.Errorf("invalid WHERE clause: %w", err)
		}
		if len(re.Columns) > 0 {
			usedCols = append(usedCols, re.Columns...)
		} else {
			return nil, nil, fmt.Errorf("no columns in DELETE's WHERE clause")
		}
		if len(re.Params) > 0 {
			queryParams = re.Params
		}
	} else {
		return nil, nil, fmt.Errorf("no WHERE clause for DELETE")
	}

	for _, u := range jList(del, "using_clause", "usingClause") {
		re := &ParseResult{}
		if err := jsonParseExpr(ctx, asNode(u), re); err != nil {
			return nil, nil, err
		}
		usedTables = append(usedTables, re.Tables...)
	}

	if ret := jList(del, "returning_list", "returningList"); len(ret) > 0 {
		usedCols = append(usedCols, jsonGetColumnsFromReturningList(ret)...)
	}
	if len(usedCols) > 0 {
		usedTables = append(usedTables, TableUsed{Name: tableName, Alias: getStringField(asNode(rv["alias"]), "aliasname")})
		if err := validateTableColumns(ctx, usedTables, usedCols); err != nil {
			return nil, nil, err
		}
	}
	return queryParams, usedCols, nil
}

// -------------- Expression & helpers --------------

func jsonParseFromClause(ctx VetContext, n jsonNode, re *ParseResult) error {
	if n == nil {
		return nil
	}
	kind, body := nodeType(n)
	switch kind {
	case "RangeVar":
		re.Tables = append(re.Tables, jsonRangeVarToTableUsed(body))
	case "JoinExpr":
		// Recursively parse the left and right sides of the join
		if err := jsonParseFromClause(ctx, asNode(body["larg"]), re); err != nil {
			return err
		}
		if err := jsonParseFromClause(ctx, asNode(body["rarg"]), re); err != nil {
			return err
		}
		// Parse the join condition if it exists
		if quals := asNode(body["quals"]); quals != nil {
			if err := jsonParseExpr(ctx, quals, re); err != nil {
				return err
			}
		}
	case "RangeSubselect":
		if re.PostponedNodes == nil {
			re.PostponedNodes = &PostponedNodes{}
		}
		// Process LATERAL subqueries immediately with access to outer query context
		subq := asNode(asNode(body["subquery"])["SelectStmt"])

		// For LATERAL subqueries, we need to ensure outer query table aliases are available
		if getBoolField(body, "lateral") {
			// Create a context that includes the outer query's table aliases
			lateralCtx := VetContext{
				Schema:      ctx.Schema,
				InnerSchema: ctx.InnerSchema,
				UsedTables:  make([]TableUsed, len(re.Tables)),
			}
			copy(lateralCtx.UsedTables, re.Tables)

			qp, targetCols, err := jsonValidateSelect(lateralCtx, subq)
			if err != nil {
				return err
			}
			if len(qp) > 0 {
				AddQueryParams(&re.Params, qp)
			}
			alias := getStringField(asNode(body["alias"]), "aliasname")
			if alias != "" {
				t := schema.Table{Name: alias, ReadOnly: true, Columns: map[string]schema.Column{}}
				for _, c := range targetCols {
					t.Columns[c.Column] = schema.Column{Name: c.Column}
				}
				ctx.InnerSchema.Tables[t.Name] = t
				re.Tables = append(re.Tables, TableUsed{Name: t.Name})
			}
		} else {
			// Non-LATERAL subqueries can be processed with the current context
			qp, targetCols, err := jsonValidateSelect(ctx, subq)
			if err != nil {
				return err
			}
			if len(qp) > 0 {
				AddQueryParams(&re.Params, qp)
			}
			alias := getStringField(asNode(body["alias"]), "aliasname")
			if alias != "" {
				t := schema.Table{Name: alias, ReadOnly: true, Columns: map[string]schema.Column{}}
				for _, c := range targetCols {
					t.Columns[c.Column] = schema.Column{Name: c.Column}
				}
				ctx.InnerSchema.Tables[t.Name] = t
				re.Tables = append(re.Tables, TableUsed{Name: t.Name})
			}
		}
	}
	return nil
}

func jsonParseExpr(ctx VetContext, n jsonNode, re *ParseResult) error {
	if n == nil {
		return nil
	}
	kind, body := nodeType(n)
	switch kind {
	case "A_Expr":
		if err := jsonParseExpr(ctx, asNode(body["lexpr"]), re); err != nil {
			return err
		}
		if err := jsonParseExpr(ctx, asNode(body["rexpr"]), re); err != nil {
			return err
		}
	case "BoolExpr":
		args := asList(body["args"])
		if len(args) > 0 {
			return jsonParseExpr(ctx, asNode(args[0]), re)
		}
	case "NullTest":
		return jsonParseExpr(ctx, asNode(body["arg"]), re)
	case "ColumnRef":
		if cu := jsonColumnRefToColumnUsed(body); cu != nil {
			re.Columns = append(re.Columns, *cu)
		}
	case "ParamRef":
		AddQueryParam(&re.Params, QueryParam{Number: getNumberField(body, "number")})
	case "FuncCall":
		args := asList(body["args"])
		if len(args) > 0 {
			if err := jsonParseExpr(ctx, asNode(args[0]), re); err != nil {
				return err
			}
		}
		if over := asNode(body["over"]); over != nil {
			wd := asNode(over["WindowDef"])
			if wd == nil {
				wd = over
			}
			pc := jList(wd, "partition_clause", "partitionClause")
			if len(pc) > 0 {
				if err := jsonParseExpr(ctx, asNode(pc[0]), re); err != nil {
					return err
				}
			}
			oc := jList(wd, "order_clause", "orderClause")
			if len(oc) > 0 {
				if err := jsonParseExpr(ctx, asNode(oc[0]), re); err != nil {
					return err
				}
			}
		}
	case "TypeCast":
		return jsonParseExpr(ctx, asNode(body["arg"]), re)
	case "List":
		for _, it := range asList(body["items"]) {
			if err := jsonParseExpr(ctx, asNode(it), re); err != nil {
				return err
			}
		}
	case "SubLink":
		sub := asNode(body["subselect"]) // Node
		qp, _, err := jsonValidateSelect(ctx, asNode(sub["SelectStmt"]))
		if err != nil {
			return err
		}
		if len(qp) > 0 {
			AddQueryParams(&re.Params, qp)
		}
	case "CoalesceExpr":
		args := asList(body["args"])
		if len(args) > 0 {
			return jsonParseExpr(ctx, asNode(args[0]), re)
		}
	case "WindowDef":
		pc := jList(body, "partition_clause", "partitionClause")
		if len(pc) > 0 {
			if err := jsonParseExpr(ctx, asNode(pc[0]), re); err != nil {
				return err
			}
		}
		oc := jList(body, "order_clause", "orderClause")
		if len(oc) > 0 {
			if err := jsonParseExpr(ctx, asNode(oc[0]), re); err != nil {
				return err
			}
		}
	case "SortBy":
		return jsonParseExpr(ctx, asNode(body["node"]), re)
	case "JoinExpr":
		return jsonParseFromClause(ctx, n, re)
	case "RangeVar":
		re.Tables = append(re.Tables, jsonRangeVarToTableUsed(body))
	case "RangeSubselect":
		return jsonParseFromClause(ctx, n, re)
	}
	return nil
}

func jsonRangeVarToTableUsed(r map[string]any) TableUsed {
	t := TableUsed{Name: getStringField(r, "relname")}
	if alias := asNode(r["alias"]); alias != nil {
		t.Alias = getStringField(alias, "aliasname")
	}
	return t
}

func jsonColumnRefToColumnUsed(colRef map[string]any) *ColumnUsed {
	cu := ColumnUsed{Location: getNumberField(colRef, "location")}
	fields := asList(colRef["fields"])
	if len(fields) == 0 {
		return nil
	}
	var colField jsonNode
	if len(fields) > 1 {
		if s := asNode(fields[0])["String"]; s != nil {
			cu.Table = getStringField(asNode(s), "sval")
		}
		colField = asNode(fields[1])
	} else {
		colField = asNode(fields[0])
	}
	if s := asNode(colField["String"]); s != nil {
		cu.Column = getStringField(s, "sval")
		return &cu
	}
	if _, star := colField["A_Star"]; star {
		return nil
	}
	return nil
}

func jsonGetTablesFromSelectStmt(from []any) []TableUsed {
	used := []TableUsed{}
	for _, it := range from {
		n := asNode(it)
		if n == nil {
			continue
		}
		if rv := asNode(n["RangeVar"]); rv != nil {
			used = append(used, jsonRangeVarToTableUsed(rv))
		}
		if je := asNode(n["JoinExpr"]); je != nil {
			used = append(used, jsonGetTablesFromSelectStmt([]any{je["larg"], je["rarg"]})...)
		}
	}
	return used
}

func jsonGetColumnsFromNodeList(n jsonNode) []ColumnUsed {
	used := []ColumnUsed{}
	// Node list can be a simple ColumnRef wrapper
	if cr := asNode(n["ColumnRef"]); cr != nil {
		if cu := jsonColumnRefToColumnUsed(cr); cu != nil {
			used = append(used, *cu)
		}
	}
	return used
}

func jsonGetColumnsFromSortClause(sortList []any) []ColumnUsed {
	used := []ColumnUsed{}
	for _, it := range sortList {
		if sb := asNode(asNode(it)["SortBy"]); sb != nil {
			if cr := asNode(asNode(sb["node"])["ColumnRef"]); cr != nil {
				if cu := jsonColumnRefToColumnUsed(cr); cu != nil {
					used = append(used, *cu)
				}
			}
		}
	}
	return used
}

func jsonGetColumnsFromReturningList(lst []any) []ColumnUsed {
	used := []ColumnUsed{}
	for _, it := range lst {
		rt := asNode(asNode(it)["ResTarget"])
		if rt == nil {
			continue
		}
		if cr := asNode(asNode(rt["val"])["ColumnRef"]); cr != nil {
			if cu := jsonColumnRefToColumnUsed(cr); cu != nil {
				used = append(used, *cu)
			}
		}
	}
	return used
}

func jsonParseCTE(ctx VetContext, with map[string]any) error {
	for _, c := range asList(with["ctes"]) {
		cte := asNode(asNode(c)["CommonTableExpr"])
		if cte == nil {
			continue
		}
		q := asNode(cte["ctequery"]) // Node
		qp, cols, err := jsonValidateNode(ctx, q)
		if err != nil {
			return err
		}
		_ = qp
		var columns map[string]schema.Column
		if cols != nil {
			columns = make(map[string]schema.Column)
			for _, col := range cols {
				columns[col.Column] = schema.Column{Name: col.Column}
			}
		}
		ctx.InnerSchema.Tables[getStringField(cte, "ctename")] = schema.Table{
			Name: getStringField(cte, "ctename"), Columns: columns, ReadOnly: true,
		}
	}
	return nil
}
