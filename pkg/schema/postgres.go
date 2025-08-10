package schema

import (
	"os"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	pg_wasm "github.com/wasilibs/go-pgquery"
)

func (s *Db) LoadPostgres(schemaPath string) error {
	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		return err
	}

	s.Tables, err = parsePostgresSchema(string(schemaBytes))
	if err != nil {
		return err
	}

	return nil
}

func parsePostgresSchema(schemaInput string) (map[string]Table, error) {
	tables := map[string]Table{}
	tree, err := pg_wasm.Parse(schemaInput)
	if err != nil {
		return nil, err
	}

	for _, stmt := range tree.GetStmts() {
		if stmt.GetStmt() == nil {
			continue
		}

		// Check if this is a CREATE TABLE statement
		if createStmt := stmt.GetStmt().GetCreateStmt(); createStmt != nil {
			tableName := createStmt.GetRelation().GetRelname()
			table := Table{
				Name:    tableName,
				Columns: map[string]Column{},
			}

			for _, colElem := range createStmt.GetTableElts() {
				if colDef := colElem.GetColumnDef(); colDef != nil {
					typeParts := []string{}
					for _, typNode := range colDef.GetTypeName().GetNames() {
						if tStr := typNode.GetString_(); tStr != nil {
							typeParts = append(typeParts, tStr.GetSval())
						}
					}

					colName := colDef.GetColname()
					table.Columns[colName] = Column{
						Name: colName,
						Type: strings.Join(typeParts, "."),
					}
				}
			}

			tables[tableName] = table
		}

		// Check if this is a CREATE VIEW statement
		if viewStmt := stmt.GetStmt().GetViewStmt(); viewStmt != nil {
			tableName := viewStmt.GetView().GetRelname()
			table := Table{
				Name:     tableName,
				Columns:  map[string]Column{},
				ReadOnly: true,
			}

			// Extract columns from the view's SELECT statement
			columns := extractColumnsFromViewQuery(viewStmt.GetQuery())
			for _, colName := range columns {
				table.Columns[colName] = Column{Name: colName}
			}

			tables[tableName] = table
		}
	}

	return tables, nil
}

// extractColumnsFromViewQuery extracts column names from a view's query
func extractColumnsFromViewQuery(query *pg_query.Node) []string {
	if query == nil {
		return nil
	}

	// Try different approaches to get the query structure
	if selectStmt := query.GetSelectStmt(); selectStmt != nil {
		return extractColumnsFromSelectStmt(selectStmt)
	}

	if queryNode := query.GetQuery(); queryNode != nil {
		return extractColumnsFromQueryNode(queryNode)
	}

	// If neither works, try to explore the node structure
	return nil
}

// extractColumnsFromSelectStmt extracts columns from a SelectStmt
func extractColumnsFromSelectStmt(selectStmt *pg_query.SelectStmt) []string {
	if selectStmt == nil {
		return nil
	}

	var columns []string

	// Get the target list from the select statement
	targetList := selectStmt.GetTargetList()
	if targetList != nil {
		for _, target := range targetList {
			if resTarget := target.GetResTarget(); resTarget != nil {
				colName := extractColumnNameFromResTarget(resTarget)
				if colName != "" {
					columns = append(columns, colName)
				}
			}
		}
	}

	return columns
}

// extractColumnsFromQueryNode extracts columns from a Query node
func extractColumnsFromQueryNode(queryNode *pg_query.Query) []string {
	if queryNode == nil {
		return nil
	}

	// Check if it's a SELECT command (CmdType_CMD_SELECT = 2)
	cmdType := queryNode.GetCommandType()
	if cmdType != 2 { // CmdType_CMD_SELECT
		return nil
	}

	var columns []string

	// Get the target list directly from the Query
	targetList := queryNode.GetTargetList()
	if targetList != nil {
		for _, target := range targetList {
			if resTarget := target.GetResTarget(); resTarget != nil {
				colName := extractColumnNameFromResTarget(resTarget)
				if colName != "" {
					columns = append(columns, colName)
				}
			}
		}
	}

	return columns
}

// extractColumnNameFromResTarget extracts the column name from a ResTarget
func extractColumnNameFromResTarget(resTarget *pg_query.ResTarget) string {
	// Get the name field (this is the column alias)
	name := resTarget.GetName()
	if name != "" {
		return name
	}

	// If no explicit name, try to extract from the value
	val := resTarget.GetVal()
	if val == nil {
		return ""
	}

	return extractColumnNameFromValue(val)
}

// extractColumnNameFromValue extracts column name from various value types
func extractColumnNameFromValue(val *pg_query.Node) string {
	if val == nil {
		return ""
	}

	// Handle ColumnRef (e.g., "u.id", "posts.title")
	if colRef := val.GetColumnRef(); colRef != nil {
		return extractColumnNameFromColumnRef(colRef)
	}

	// Handle A_Star (wildcard *)
	if star := val.GetAStar(); star != nil {
		return "*"
	}

	// Handle FuncCall (e.g., "count(*)")
	if funcCall := val.GetFuncCall(); funcCall != nil {
		return extractColumnNameFromFuncCall(funcCall)
	}

	return ""
}

// extractColumnNameFromColumnRef extracts column name from a ColumnRef
func extractColumnNameFromColumnRef(colRef *pg_query.ColumnRef) string {
	if colRef == nil {
		return ""
	}

	// Get the fields from the ColumnRef
	fields := colRef.GetFields()
	if len(fields) > 0 {
		// The last field is usually the column name
		lastField := fields[len(fields)-1]
		if strNode := lastField.GetString_(); strNode != nil {
			return strNode.GetSval()
		}
	}

	return ""
}

// extractColumnNameFromFuncCall extracts column name from a FuncCall
func extractColumnNameFromFuncCall(funcCall *pg_query.FuncCall) string {
	if funcCall == nil {
		return ""
	}

	// Get the function name
	funcNames := funcCall.GetFuncname()
	if len(funcNames) > 0 {
		lastFuncName := funcNames[len(funcNames)-1]
		if strNode := lastFuncName.GetString_(); strNode != nil {
			return strNode.GetSval()
		}
	}

	return ""
}
