package gorm

import (
	"errors"
	"fmt"
	"strings"
)

func init() {
	DefaultCallback.CreateBatch().Register("gorm:create_batch", createBatchCallback)
}

func createBatchCallback(scope *Scope) {

	if scope.HasError() {
		return
	}

	defer scope.trace(NowFunc())

	var (
		columns, placeholders        []string
		blankColumnsWithDefaultValue []string
	)

	for _, field := range scope.Fields() {
		if !scope.changeableField(field) {
			continue
		}

		if field.IsNormal {
			if field.IsBlank && field.HasDefaultValue {
				blankColumnsWithDefaultValue = append(blankColumnsWithDefaultValue, scope.Quote(field.DBName))
				scope.InstanceSet("gorm:blank_columns_with_default_value", blankColumnsWithDefaultValue)
			} else if !field.IsPrimaryKey || !field.IsBlank {
				columns = append(columns, scope.Quote(field.DBName))
				placeholders = append(placeholders, scope.AddToVars(field.Field.Interface()))
			}

		}
	}

	var (
		returningColumn = "*"
		quotedTableName = scope.QuotedTableName()
		primaryField    = scope.PrimaryField()
		extraOption     string
	)

	if str, ok := scope.Get("gorm:insert_option"); ok {
		extraOption = fmt.Sprint(str)
	}

	if primaryField != nil {
		returningColumn = scope.Quote(primaryField.DBName)
	}

	lastInsertIDReturningSuffix := scope.Dialect().LastInsertIDReturningSuffix(quotedTableName, returningColumn)

	if len(columns) == 0 {
		scope.Raw(fmt.Sprintf(
			"INSERT INTO %v DEFAULT VALUES%v%v",
			quotedTableName,
			addExtraSpaceIfExist(extraOption),
			addExtraSpaceIfExist(lastInsertIDReturningSuffix),
		))
	} else {
		scope.Raw(fmt.Sprintf(
			"INSERT INTO %v (%v) VALUES ",
			scope.QuotedTableName(),
			strings.Join(columns, ","),
		))
	}

	vals, ok := scope.Get("all")
	if !ok {
		return
	}

	typedVals, ok := vals.([]interface{})
	if !ok {
		scope.Err(errors.New("createBatch: invalid value, expected []interface{}"))
		return
	}

	limit := len(columns) * 200
	scope.SQLVars = []interface{}{}
	for _, v := range typedVals {

		scope.Value = v
		scope.fields = nil

		fieldCount := 0

		for _, field := range scope.Fields() {
			if !scope.changeableField(field) || !field.IsNormal || (field.IsBlank && field.HasDefaultValue) || field.IsPrimaryKey || field.IsBlank {
				continue
			}

			scope.AddToVars(field.Field.Interface())
			fieldCount++
		}

		if fieldCount != len(columns) {
			scope.Err(errors.New("createBatch: field count does not match column count"))
			return
		}
	}

	placeholder := strings.Replace(fmt.Sprintf("(%v)", strings.Join(placeholders, ",")), "$$$", "?", -1)

	for i := 0; i < len(scope.SQLVars); i += limit {

		if len(scope.SQLVars) < i+limit {

			mapping := strings.Replace(strings.Repeat(placeholder, len(scope.SQLVars[i:])/len(columns)), ")(", "),(", -1)

			if result, err := scope.SQLDB().Exec(scope.SQL+mapping, scope.SQLVars[i:]...); scope.Err(err) == nil {
				// set rows affected count
				scope.db.RowsAffected, _ = result.RowsAffected()

				// set primary value to primary field
				if primaryField != nil && primaryField.IsBlank {
					if primaryValue, err := result.LastInsertId(); scope.Err(err) == nil {
						scope.Err(primaryField.Set(primaryValue))
					}
				}
			}
			break
		}

		mapping := strings.Replace(strings.Repeat(placeholder, len(scope.SQLVars[i:i+limit])/len(columns)), ")(", "),(", -1)
		if result, err := scope.SQLDB().Exec(scope.SQL+mapping, scope.SQLVars[i:i+limit]...); scope.Err(err) == nil {
			// set rows affected count
			scope.db.RowsAffected, _ = result.RowsAffected()

			// set primary value to primary field
			if primaryField != nil && primaryField.IsBlank {
				if primaryValue, err := result.LastInsertId(); scope.Err(err) == nil {
					scope.Err(primaryField.Set(primaryValue))
				}
			}
		}

	}
}
