package zorm

import (
	"fmt"
	"log"
	"reflect"
	"strings"
)

var (
	ANDOR    = [2]string{"AND", "OR"}
	BLANKNOT = [2]string{"", "NOT"}
)

type Filter interface {
	AND | OR
}

type AND map[string]any
type OR map[string]any

type Operator interface {
	OperatorSQL(operator string) string
}

type QuerySet[T Filter] interface {
	GetQuerySet() (string, []any)
	FilterToSQL(filter map[string]any) QuerySet
	ExcludeToSQL(exclude map[string]any) QuerySet
	GetOrderBySQL() string
	OrderByToSQL(orderBy []string) QuerySet
	GetLimitSQL() string
	LimitToSQL(pageSize, pageNum int64) QuerySet
	SelectToSQL(columns []string) QuerySet
	GetSelectSQL() string
	GroupByToSQL(groupBy []string) QuerySet
	GetGroupBySQL() string
}

type queryFilter struct {
	SQL  string
	Args []any
}

type QuerySetImpl struct {
	Operator
	selectColumn     string
	whereCondition   queryFilter
	filterCondition  []queryFilter
	excludeCondition []queryFilter
	orderBySQL       string
	limitSQL         string
	groupSQL         string
}

var _ QuerySet = (*QuerySetImpl)(nil)

func NewQuerySet(op Operator) QuerySet {
	return &QuerySetImpl{
		Operator:         op,
		selectColumn:     "",
		whereCondition:   queryFilter{},
		filterCondition:  make([]queryFilter, 0, 10),
		excludeCondition: make([]queryFilter, 0, 10),
		orderBySQL:       "",
		limitSQL:         "",
	}
}

func (p *QuerySetImpl) GetQuerySet() (sql string, args []any) {
	if p.whereCondition.SQL != "" {
		return " WHERE " + p.whereCondition.SQL, p.whereCondition.Args
	}

	if len(p.filterCondition) == 0 && len(p.excludeCondition) == 0 {
		return "", []any{}
	}

	sql = ""

	if len(p.filterCondition) > 0 {
		for _, v := range p.filterCondition {
			sql += "(" + v.SQL + ") AND "
			args = append(args, v.Args...)
		}
	}

	if len(p.excludeCondition) > 0 {
		for _, v := range p.excludeCondition {
			sql += "NOT (" + v.SQL + ") AND "
			args = append(args, v.Args...)
		}
	}

	return " WHERE " + strings.TrimSpace(sql[:len(sql)-4]), args
}

func (p *QuerySetImpl) WhereToSQL(cond string, args ...any) QuerySet {
	p.whereCondition.SQL = cond
	p.whereCondition.Args = args
	return p
}

func (p *QuerySetImpl) filterHandler(filter ...T) (filterSql string, filterArgs []any) {
	if len(filter) == 0 {
		return
	}

	var (
		baseSQL   = " `%s`%s? "
		fieldName string
		operator  string
		flag      = 0
	)

	filterSql = ""
	filterArgs = []any{}

	for fieldLookups, filedValue := range filter {
		fl := strings.Split(fieldLookups, "__")
		fieldName = fl[0]
		if len(fl) == 1 {
			operator = "exact"
			flag = 0
		} else if len(fl) == 2 {
			if fl[1] != "Q" {
				operator = fl[1]
				flag = 0
			} else {
				operator = "exact"
				flag = 1
			}
		} else if len(fl) == 3 {
			if fl[2] == "Q" {
				operator = fl[1]
				flag = 1
			} else {
				log.Panicf("FieldLookups [%s] is invalid.", fieldLookups)
			}
		}

		op := p.OperatorSQL(operator)
		v := reflect.ValueOf(filedValue)
		switch v.Kind() {
		case reflect.String, reflect.Bool,
			reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Float32, reflect.Float64:

			switch operator {
			case "in", "between":
				log.Panicf("Operator [%s] must be used with slice or array.", operator)
			}

			filterSql += ANDOR[flag]
			filterSql += fmt.Sprintf(baseSQL, fieldName, op)
			filterArgs = append(filterArgs, filedValue)
		case reflect.Slice, reflect.Array:
			if v.Len() == 0 {
				log.Panicf("Empty slice or array for key [%s].", fieldLookups)
			}

			kind := v.Index(0).Kind()
			if kind == reflect.Invalid || kind == reflect.Chan || kind == reflect.Func || kind == reflect.Map ||
				kind == reflect.Ptr || kind == reflect.Uintptr || kind == reflect.UnsafePointer ||
				kind == reflect.Complex64 || kind == reflect.Complex128 {
				log.Panicf("Unsupported slice type [%+v] for key [%s].", kind, fieldLookups)
			}

			filterSql += ANDOR[flag]

			switch operator {
			case "exact", "exclude", "contains", "icontains":
				filterSql += fmt.Sprintf(" ( %s %s ?", fieldName, op) + strings.Repeat(fmt.Sprintf(" %s %s %s ?", ANDOR[flag], fieldName, op), v.Len()-1) + " ) "
			case "in":
				filterSql += fmt.Sprintf(" %s %s %s (?"+strings.Repeat(",?", v.Len()-1)+") ", fieldName, BLANKNOT[flag], op)
			case "between":
				filterSql += fmt.Sprintf(" %s %s %s ? AND ? ", fieldName, BLANKNOT[flag], op)
			default:
				log.Panicf("Unsupported slice operator [%s].", operator)
			}

			for i := 0; i < v.Len(); i++ {
				filterArgs = append(filterArgs, v.Index(i).Interface())
			}
		default:
			log.Panicf("Unsupported data type [%s] for key [%s].", v.Kind(), fieldLookups)
		}
	}

	filterSql = strings.TrimSpace(filterSql)

	if filterSql != "" {
		if strings.HasPrefix(filterSql, "OR") {
			filterSql = filterSql[3:]
		} else if strings.HasPrefix(filterSql, "AND") {
			filterSql = filterSql[4:]
		} else {
			log.Panicf("Generate sql error. %s", filterSql)
		}
	}

	return filterSql, filterArgs
}

func (p *QuerySetImpl) FilterToSQL(filter ...map[string]any) QuerySet {
	filterSQL, filterArgs := p.filterHandler(filter)
	if filterSQL == "" {
		return p
	}
	p.filterCondition = append(p.filterCondition, queryFilter{
		SQL:  filterSQL,
		Args: filterArgs,
	})
	return p
}

func (p *QuerySetImpl) ExcludeToSQL(exclude map[string]any) QuerySet {
	excludeSQL, excludeArgs := p.filterHandler(exclude)
	if excludeSQL == "" {
		return p
	}

	p.excludeCondition = append(p.excludeCondition, queryFilter{
		SQL:  excludeSQL,
		Args: excludeArgs,
	})
	return p
}

func (p *QuerySetImpl) GetOrderBySQL() string {
	if strings.HasPrefix(p.orderBySQL, ",") {
		return " ORDER BY" + p.orderBySQL[1:]
	}
	return ""
}

func (p *QuerySetImpl) OrderByToSQL(orderBy []string) QuerySet {
	if len(orderBy) <= 0 {
		return p
	}

	asc := true
	for _, by := range orderBy {
		p.orderBySQL += ","
		by = strings.TrimSpace(by)
		if strings.HasPrefix(by, "-") {
			by = by[1:]
			asc = false
		}

		if asc {
			p.orderBySQL += fmt.Sprintf(" `%s` ASC", by)
		} else {
			p.orderBySQL += fmt.Sprintf(" `%s` DESC", by)
		}
	}
	if strings.HasSuffix(p.orderBySQL, ",") {
		p.orderBySQL = p.orderBySQL[:len(p.orderBySQL)-1]
	}

	return p
}

func (p *QuerySetImpl) GetLimitSQL() string {
	return p.limitSQL
}

func (p *QuerySetImpl) LimitToSQL(pageSize, pageNum int64) QuerySet {
	if pageSize > 0 && pageNum > 0 {
		var offset, limit int64
		offset = (pageNum - 1) * pageSize
		limit = pageSize
		p.limitSQL = fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)
	}

	return p
}

func (p *QuerySetImpl) SelectToSQL(columns []string) QuerySet {
	p.selectColumn = strings.Join(columns, ",")
	return p
}

func (p *QuerySetImpl) GetSelectSQL() string {
	if p.selectColumn == "" {
		return "*"
	}
	return p.selectColumn
}

func (p *QuerySetImpl) GroupByToSQL(groupBy []string) QuerySet {
	p.groupSQL = "`" + strings.Join(groupBy, "`,`") + "`"
	return p
}

func (p *QuerySetImpl) GetGroupBySQL() string {
	if p.groupSQL != "" {
		return " GROUP BY " + p.groupSQL
	}
	return ""
}

