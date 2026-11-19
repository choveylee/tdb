package tdb

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/longbridgeapp/sqlparser"
)

type shardingToken struct {
	kind    sqlparser.Token
	literal string
	offset  int
}

func (token shardingToken) identifier() string {
	if token.kind == sqlparser.QIDENT {
		return token.literal[1 : len(token.literal)-1]
	}

	return token.literal
}

// route resolves one logical table and month before the query is sent. Queries
// mixing shards or using an unsupported form fail with a routing error.
func (plugin *MonthlySharding) route(query string, arguments []any) (routedQuery string, err error) {
	defer func() {
		if err == nil {
			plugin.lastQuery.Store(&routedQuery)
		}
	}()

	tokens, candidate, unsupported := plugin.scanSQL(query)
	if !candidate {
		return query, nil
	}

	if unsupported {
		return "", errors.New("tdb: SQL form does not support monthly sharding")
	}

	statement, err := sqlparser.NewParser(strings.NewReader(query)).ParseStatement()
	if err != nil {
		// Parser errors can include literal SQL values.
		return "", errors.New("tdb: SQL form does not support monthly sharding")
	}

	var target *sqlparser.TableName
	var condition sqlparser.Expr
	var insert *sqlparser.InsertStatement
	var assignments []*sqlparser.Assignment
	switch parsed := statement.(type) {
	case *sqlparser.SelectStatement:
		if parsed.Hint != nil && parsed.Hint.Value == "nosharding" {
			return query, nil
		}

		target, _ = parsed.FromItems.(*sqlparser.TableName)
		condition = parsed.Condition
	case *sqlparser.UpdateStatement:
		target, condition = parsed.TableName, parsed.Condition
		assignments = parsed.Assignments
		if len(parsed.FromList) > 0 {
			return "", errors.New("tdb: SQL form does not support monthly sharding")
		}

	case *sqlparser.DeleteStatement:
		target, condition = parsed.TableName, parsed.Condition
		if len(parsed.UsingList) > 0 {
			return "", errors.New("tdb: SQL form does not support monthly sharding")
		}

	case *sqlparser.InsertStatement:
		target, insert = parsed.TableName, parsed
		if parsed.UpsertClause != nil {
			assignments = parsed.UpsertClause.Assignments
		}
	}

	tableCount, configured := 0, false
	err = sqlparser.Walk(sqlparser.VisitFunc(func(node sqlparser.Node) error {
		if table, ok := node.(*sqlparser.TableName); ok {
			tableCount++
			_, matches := plugin.tablesMap[strings.ToLower(table.Name.Name)]
			configured = configured || matches
		}

		return nil
	}), statement)
	if err != nil {
		return "", errors.New("tdb: inspect SQL tables for monthly sharding")
	}

	if !configured {
		return query, nil
	}

	if target == nil || tableCount != 1 {
		return "", errors.New("tdb: monthly sharding requires a single logical table")
	}

	var suffix string
	if insert != nil {
		suffix, err = plugin.insertSuffix(insert, arguments)
	} else {
		suffix, err = plugin.conditionSuffix(condition, target.TableName(), arguments)
	}

	if err != nil {
		return "", err
	}

	if suffix == "" {
		return "", fmt.Errorf("tdb: monthly sharding requires equality on %q", plugin.key)
	}

	if err := plugin.validateAssignments(assignments, target.TableName(), suffix, arguments); err != nil {
		return "", err
	}

	return appendShardSuffix(query, tokens, target, suffix)
}

// validateAssignments prevents a successful write from stranding a row in the wrong month.
// Self-assignment preserves the routed month. Other expressions
// must resolve to a concrete value in that same month before the query is sent.
func (plugin *MonthlySharding) validateAssignments(assignments []*sqlparser.Assignment, tableName, suffix string, arguments []any) error {
	for _, assignment := range assignments {
		for _, column := range assignment.Columns {
			if !strings.EqualFold(column.Name, plugin.key) {
				continue
			}

			if len(assignment.Columns) != 1 {
				return errors.New("tdb: monthly sharding key must not use a tuple assignment")
			}

			switch expression := assignment.Expr.(type) {
			case *sqlparser.Ident:
				if strings.EqualFold(expression.Name, plugin.key) {
					continue
				}
			case *sqlparser.QualifiedRef:
				if strings.EqualFold(expression.Table.Name, tableName) && strings.EqualFold(expression.Column.Name, plugin.key) {
					continue
				}
			}

			updatedSuffix, err := plugin.valueSuffix(assignment.Expr, arguments)
			if err != nil {
				return fmt.Errorf("tdb: validate monthly sharding key assignment: %w", err)
			}

			if updatedSuffix != suffix {
				return errors.New("tdb: monthly sharding key update must stay in the routed month")
			}
		}
	}

	return nil
}

// scanSQL identifies possible logical table references and rejects executable
// comments, malformed tokens and multiple statements when routing is needed.
func (plugin *MonthlySharding) scanSQL(query string) ([]shardingToken, bool, bool) {
	lowerQuery := strings.ToLower(query)
	possible := false
	words := strings.FieldsFuncSeq(lowerQuery, func(character rune) bool {
		return character != '_' && character != '$' && !unicode.IsLetter(character) && !unicode.IsDigit(character)
	})
	for word := range words {
		if _, matches := plugin.tablesMap[word]; matches {
			possible = true

			break
		}
	}

	if !possible && strings.ContainsAny(lowerQuery, "`\"") {
		for tableName := range plugin.tablesMap {
			backtickName := "`" + strings.ReplaceAll(tableName, "`", "``") + "`"
			quotedName := `"` + strings.ReplaceAll(tableName, `"`, `""`) + `"`
			if strings.Contains(lowerQuery, backtickName) || strings.Contains(lowerQuery, quotedName) {
				possible = true

				break
			}
		}
	}

	if !possible {
		return nil, false, false
	}

	// The parser's lexer does not handle MySQL's backslash-escaped quote.
	if strings.Contains(query, "\\'") || strings.Contains(query, `\"`) {
		return nil, true, true
	}

	lexer := sqlparser.NewLexer(strings.NewReader(query))
	var tokens []shardingToken
	var candidate, ended, unsupported bool
	for {
		position, kind, literal := lexer.Lex()
		switch kind {
		case sqlparser.EOF:
			return tokens, candidate, unsupported
		case sqlparser.SEMI:
			ended = true

			continue
		case sqlparser.MLCOMMENT:
			if strings.HasPrefix(literal, "!") || strings.HasPrefix(literal, "M!") {
				candidate, unsupported = true, true
			}

			continue
		case sqlparser.ILLEGAL:
			// A dialect-specific token can hide the following table name.
			candidate, unsupported = true, true
		}

		token := shardingToken{kind: kind, literal: literal, offset: position.Offset}
		tokens = append(tokens, token)
		unsupported = unsupported || ended
		switch kind {
		case sqlparser.STRING, sqlparser.BLOB, sqlparser.BIND:
		default:
			_, matches := plugin.tablesMap[strings.ToLower(token.identifier())]
			candidate = candidate || matches
		}
	}
}

// insertSuffix verifies that every inserted row has the key and targets the same month.
func (plugin *MonthlySharding) insertSuffix(insert *sqlparser.InsertStatement, arguments []any) (string, error) {
	if insert.Query != nil || len(insert.Expressions) == 0 {
		return "", errors.New("tdb: monthly sharding requires insert values")
	}

	keyIndex := -1
	for index, column := range insert.ColumnNames {
		if strings.EqualFold(column.Name, plugin.key) {
			keyIndex = index

			break
		}
	}

	if keyIndex < 0 {
		return "", fmt.Errorf("tdb: monthly sharding insert requires %q", plugin.key)
	}

	var suffix string
	for _, values := range insert.Expressions {
		if keyIndex >= len(values.Exprs) {
			return "", fmt.Errorf("tdb: monthly sharding insert requires a value for %q", plugin.key)
		}

		rowSuffix, err := plugin.valueSuffix(values.Exprs[keyIndex], arguments)
		if err != nil {
			return "", err
		}

		if suffix != "" && rowSuffix != suffix {
			return "", errors.New("tdb: monthly sharding insert must target one month")
		}

		suffix = rowSuffix
	}

	return suffix, nil
}

// conditionSuffix returns a month only when the predicate confines all
// matching rows to that month, including both branches of an OR.
func (plugin *MonthlySharding) conditionSuffix(condition sqlparser.Expr, tableName string, arguments []any) (string, error) {
	switch expression := condition.(type) {
	case *sqlparser.ParenExpr:
		return plugin.conditionSuffix(expression.X, tableName, arguments)
	case *sqlparser.BinaryExpr:
		switch expression.Op {
		case sqlparser.EQ:
			switch column := expression.X.(type) {
			case *sqlparser.Ident:
				if strings.EqualFold(column.Name, plugin.key) {
					return plugin.valueSuffix(expression.Y, arguments)
				}
			case *sqlparser.QualifiedRef:
				if strings.EqualFold(column.Table.Name, tableName) && strings.EqualFold(column.Column.Name, plugin.key) {
					return plugin.valueSuffix(expression.Y, arguments)
				}
			}
		case sqlparser.AND, sqlparser.OR:
			left, err := plugin.conditionSuffix(expression.X, tableName, arguments)
			if err != nil {
				return "", err
			}

			right, err := plugin.conditionSuffix(expression.Y, tableName, arguments)
			if err != nil {
				return "", err
			}

			if left != "" && right != "" && left != right {
				return "", errors.New("tdb: monthly sharding query must target one month")
			}

			if expression.Op == sqlparser.OR && left != right {
				return "", nil
			}

			if left != "" {
				return left, nil
			}

			return right, nil
		}
	}

	return "", nil
}

func (plugin *MonthlySharding) valueSuffix(value sqlparser.Expr, arguments []any) (string, error) {
	switch expression := value.(type) {
	case *sqlparser.BindExpr:
		if expression.Name == "?" && expression.Pos >= 0 && expression.Pos < len(arguments) {
			return plugin.suffix(arguments[expression.Pos])
		}
	case *sqlparser.StringLit:
		return plugin.suffix(expression.Value)
	}

	return "", fmt.Errorf("tdb: monthly sharding key %q requires a bound value or string literal", plugin.key)
}

// appendShardSuffix changes identifiers in the original SQL so MySQL-specific
// clauses, quoted values and placeholder order remain intact.
func appendShardSuffix(query string, tokens []shardingToken, target *sqlparser.TableName, suffix string) (string, error) {
	tableName := target.Name.Name
	tableIndex, depth := -1, 0
	first := tokens[0].kind
	for index, token := range tokens[:len(tokens)-1] {
		switch token.kind {
		case sqlparser.LP:
			depth++
		case sqlparser.RP:
			depth--
		}

		if depth != 0 || !strings.EqualFold(tokens[index+1].identifier(), tableName) {
			continue
		}

		switch {
		case first == sqlparser.INSERT && (token.kind == sqlparser.INTO || index == 0):
			tableIndex = index + 1
		case first == sqlparser.UPDATE && index == 0:
			tableIndex = index + 1
		case (first == sqlparser.SELECT || first == sqlparser.DELETE) && token.kind == sqlparser.FROM:
			tableIndex = index + 1
		}

		if tableIndex >= 0 {
			break
		}
	}

	if tableIndex < 0 {
		return "", errors.New("tdb: locate logical table in monthly sharding SQL")
	}

	runes := []rune(query)
	var output strings.Builder
	output.Grow(len(query) + len(suffix))
	last := 0
	for index, token := range tokens {
		qualifier := target.Alias == nil && index+1 < len(tokens) && tokens[index+1].kind == sqlparser.DOT &&
			(token.kind == sqlparser.IDENT || token.kind == sqlparser.QIDENT) && strings.EqualFold(token.identifier(), tableName)
		if index != tableIndex && !qualifier {
			continue
		}

		at := token.offset + utf8.RuneCountInString(token.literal)
		if token.kind == sqlparser.QIDENT {
			quote := token.literal[:1]
			at += strings.Count(token.identifier(), quote) - 1
		}

		output.WriteString(string(runes[last:at]))
		output.WriteString(suffix)
		last = at
	}

	output.WriteString(string(runes[last:]))

	return output.String(), nil
}
