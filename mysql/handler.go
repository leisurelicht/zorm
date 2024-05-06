package norm

import (
	"context"
	"errors"
	"fmt"
	"github.com/zeromicro/go-zero/core/logc"
	"github.com/zeromicro/go-zero/core/stores/builder"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"log"
	"policy-center/db"
	"policy-center/db/flag"
	"policy-center/utils"
	"reflect"
	"strings"
)

var ErrDuplicateKey = errors.New("duplicate key")

var _ Controller = (*Impl)(nil)

type (
	Controller interface {
		Reset() Controller
		Filter(filter ...map[string]any) Controller
		Exclude(exclude map[string]any) Controller
		OrderBy(orderBy any) Controller
		Limit(pageSize, pageNum int64) Controller
		Select(columns any) Controller
		Where(cond string, args ...any) Controller
		GroupBy(groupBy any) Controller
		Insert(data map[string]any) (id int64, err error)
		InsertModel(model any) (id int64, err error)
		BulkInsert(data []map[string]any, handler sqlx.ResultHandler) (err error)
		BulkInsertModel(modelSlice any, handler sqlx.ResultHandler) (err error)
		Remove() (num int64, err error)
		Update(data map[string]any) (num int64, err error)
		Count() (num int64, err error)
		FindOne() (result map[string]any, err error)
		FindOneModel(modelPtr any) (err error)
		FindAll() (result []map[string]any, err error)
		FindAllModel(modelSlicePtr any) (err error)
		Delete() (num int64, err error)
		Modify(data map[string]any) (num int64, err error)
		Exist() (exist bool, error error)
		List() (num int64, data []map[string]any, err error)
		GetOrCreate(data map[string]any) (result map[string]any, err error)
		CreateOrUpdate(filter map[string]any, data map[string]any) (created bool, num int64, err error)
		GetC2CMap(column1, column2 string) (res map[any]any, err error)
		CreateIfNotExist(data map[string]any) (id int64, created bool, err error)
	}

	Impl struct {
		context      context.Context
		conn         sqlx.SqlConn
		model        any
		modelSlice   any
		table        string
		fieldNameMap map[string]struct{}
		fieldRows    string
		mTag         string
		qs           db.QuerySet
	}
)

// shiftName shift name like DevicePolicyMap to device_policy_map
func shiftName(s string) string {
	res := ""
	for i, c := range s {
		if c >= 'A' && c <= 'Z' {
			if i != 0 {
				res += "_"
			}
			res += string(c + 32)
		} else {
			res += string(c)
		}
	}
	return "`" + res + "`"
}

func NewController(conn sqlx.SqlConn, m any, mSlice any) func(ctx context.Context) Controller {
	t := reflect.TypeOf(m)
	if t.Kind() != reflect.Ptr {
		log.Panicf("model [%s] must be a pointer", t.Name())
		return nil
	}
	name := t.Elem().Name()

	ts := reflect.TypeOf(mSlice)
	if ts.Kind() != reflect.Ptr || ts.Elem().Kind() != reflect.Slice || ts.Elem().Elem().Kind() != reflect.Ptr {
		log.Panicf("model Slice [%s] must be a pointer", ts.Name())
		return nil
	}

	if ts.Elem().Elem().Elem() != t.Elem() {
		log.Panicf("model Slice not equal to model")
	}

	tableName := shiftName(name)
	fieldNameMap := utils.StrSlice2Map(builder.RawFieldNames(m, true))
	fieldRows := strings.Join(builder.RawFieldNames(m), ",")

	return func(ctx context.Context) Controller {
		if ctx == nil {
			ctx = context.Background()
		}
		return &Impl{
			context:      ctx,
			conn:         conn,
			model:        m,
			modelSlice:   mSlice,
			table:        tableName,
			fieldNameMap: fieldNameMap,
			fieldRows:    fieldRows,
			mTag:         flag.DefaultModelTag,
			qs:           db.NewQuerySet(NewOperator()),
		}
	}
}

func (m *Impl) ctx() context.Context {
	return m.context
}

func (m *Impl) values(values []string) string {
	valueRows := ""

	for _, v := range values {
		if _, ok := m.fieldNameMap[v]; !ok {
			logc.Errorf(m.ctx(), "Key [%s] not exist.", v)
			continue
		}
		valueRows += fmt.Sprintf("`%s`,", v)
	}
	valueRows = strings.TrimRight(valueRows, ",")

	return valueRows
}

func (m *Impl) Reset() Controller {
	m.qs = db.NewQuerySet(NewOperator())
	return m
}

func (m *Impl) Filter(filter ...map[string]any) Controller {
	m.qs.FilterToSQL(filter)
	return m
}

func (m *Impl) Exclude(exclude map[string]any) Controller {
	m.qs.ExcludeToSQL(exclude)
	return m
}

func (m *Impl) OrderBy(orderBy any) Controller {
	var (
		orderBySlice   []string
		orderByChecked []string
	)
	v := reflect.ValueOf(orderBy)
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		orderByList, ok := orderBy.([]string)
		if !ok {
			logc.Error(m.ctx(), "Order by type should be string slice or string array")
			return m
		}
		if len(orderByList) == 0 {
			return m
		}
		orderBySlice = orderByList
	case reflect.String:
		if orderBy.(string) == "" {
			return m
		}
		orderBySlice = strings.Split(orderBy.(string), ",")
	default:
		logc.Error(m.ctx(), "Order by type should be string, string slice or string array .")
		return m
	}

	for _, by := range orderBySlice {
		by = strings.TrimSpace(by)
		if strings.HasPrefix(by, "-") {
			if _, ok := m.fieldNameMap[by[1:]]; ok {
				orderByChecked = append(orderByChecked, by)
			} else {
				logc.Errorf(m.ctx(), "Order by key [%s] not exist.", by[1:])
				continue
			}
		} else {
			if _, ok := m.fieldNameMap[by]; ok {
				orderByChecked = append(orderByChecked, by)
			} else {
				logc.Errorf(m.ctx(), "Order by key [%s] not exist.", by)
				continue
			}
		}
	}

	m.qs = m.qs.OrderByToSQL(orderByChecked)
	return m
}

func (m *Impl) Limit(pageSize, pageNum int64) Controller {
	m.qs.LimitToSQL(pageSize, pageNum)
	return m
}

func (m *Impl) Select(columns any) Controller {
	var selectSlice []string
	v := reflect.ValueOf(columns)
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		columnsList, ok := columns.([]string)
		if !ok {
			logc.Error(m.ctx(), "Select columns type should be string slice or string array.")
			return m
		}
		if len(columnsList) == 0 {
			return m
		}
		selectSlice = columnsList
	case reflect.String:
		if columns.(string) == "" {
			return m
		}
		selectSlice = strings.Split(columns.(string), ",")
	default:
		logc.Error(m.ctx(), "Select type should be string, string slice or string array .")
		return m
	}

	m.qs.SelectToSQL(selectSlice)
	return m
}

func (m *Impl) Where(cond string, args ...any) Controller {
	m.qs.WhereToSQL(cond, args)
	return m
}

func (m *Impl) GroupBy(groupBy any) Controller {
	var (
		groupBySlice        []string
		groupBySliceChecked []string
	)
	v := reflect.ValueOf(groupBy)
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		groupByList, ok := groupBy.([]string)
		if !ok {
			logc.Error(m.ctx(), "Group by type should be string slice or string array")
			return m
		}
		if len(groupByList) == 0 {
			return m
		}
		groupBySlice = groupByList
	case reflect.String:
		if groupBy.(string) == "" {
			return m
		}
		groupBySlice = strings.Split(groupBy.(string), ",")
	default:
		logc.Error(m.ctx(), "Group by type should be string, string slice or string array .")
		return m
	}

	for _, by := range groupBySlice {
		by = strings.TrimSpace(by)
		if _, ok := m.fieldNameMap[by]; ok {
			groupBySliceChecked = append(groupBySliceChecked, by)
		} else {
			logc.Errorf(m.ctx(), "Group by key [%s] not exist.", by)
			continue
		}
	}

	m.qs.GroupByToSQL(groupBySliceChecked)
	return m
}

func (m *Impl) Insert(data map[string]any) (id int64, err error) {
	var (
		rows []string
		args []any
	)

	for k, _ := range m.fieldNameMap {
		if _, ok := data[k]; !ok {
			continue
		}
		rows = append(rows, fmt.Sprintf("`%s`", k))
		args = append(args, data[k])
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", m.table, strings.Join(rows, ","), strings.Repeat("?,", len(rows)-1)+"?")

	res, err := m.conn.ExecCtx(m.ctx(), sql, args...)
	if err != nil {
		if strings.Contains(err.Error(), "1062") {
			return 0, ErrDuplicateKey
		}
		logc.Errorf(m.ctx(), "Insert error: %s", err)
		return 0, err
	}

	id, err = res.LastInsertId()
	if err != nil {
		logc.Errorf(m.ctx(), "Get last insert id error: %s", err)
	}

	return id, err
}

func (m *Impl) InsertModel(model any) (id int64, err error) {
	return m.Insert(utils.Struct2Map(model, m.mTag))
}

func (m *Impl) BulkInsert(data []map[string]any, handler sqlx.ResultHandler) (err error) {
	var rows []string
	for k, _ := range m.fieldNameMap {
		if _, ok := data[0][k]; !ok {
			continue
		}
		rows = append(rows, fmt.Sprintf("`%s`", k))
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", m.table, strings.Join(rows, ","), strings.Repeat("?,", len(rows)-1)+"?")

	blk, err := sqlx.NewBulkInserter(m.conn, sql)
	if err != nil {
		logc.Errorf(m.ctx(), "Insert BulkInsert handle error: %+v", err)
		return err
	}
	defer blk.Flush()

	for _, v := range data {
		var args []any
		for _, k := range rows {
			args = append(args, v[k])
		}
		if err := blk.Insert(args...); err != nil {
			logc.Errorf(m.ctx(), "BulkInsert error: %+v", err)
			return err
		}
	}

	if handler != nil {
		blk.SetResultHandler(handler)
	}

	return nil
}

func (m *Impl) BulkInsertModel(modelSlice any, handler sqlx.ResultHandler) (err error) {
	return nil
}

func (m *Impl) Remove() (num int64, err error) {
	sql := fmt.Sprintf("DELETE FROM %s", m.table)

	filterSQL, filterArgs := m.qs.GetQuerySet()
	sql += filterSQL

	res, err := m.conn.ExecCtx(m.ctx(), sql, filterArgs...)
	if err != nil {
		logc.Errorf(m.ctx(), "Remove error: %+v", err)
		return 0, err
	}

	num, err = res.RowsAffected()
	if err != nil {
		logc.Errorf(m.ctx(), "Remove rows affected error: %+v", err)
		return 0, err
	}
	return num, nil
}

func (m *Impl) Update(data map[string]any) (num int64, err error) {
	var (
		args       []any
		updateRows []string
		updateArgs []any
	)

	for k, v := range data {
		if _, ok := m.fieldNameMap[k]; !ok {
			logc.Errorf(m.ctx(), "Key [%s] not exist.", k)
			continue
		}
		updateRows = append(updateRows, fmt.Sprintf("`%s`", k))
		updateArgs = append(updateArgs, v)
	}

	sql := fmt.Sprintf("UPDATE %s SET %s", m.table, strings.Join(updateRows, "=?,")+"=?")
	args = append(args, updateArgs...)

	filterSQL, filterArgs := m.qs.GetQuerySet()
	sql += filterSQL
	args = append(args, filterArgs...)

	res, err := m.conn.Exec(sql, args...)
	if err != nil {
		logc.Errorf(m.ctx(), "Update error: %+v", err)
		return 0, err
	}

	num, err = res.RowsAffected()
	if err != nil {
		logc.Errorf(m.ctx(), "Update rows affected error: %+v", err)
		return 0, err
	}
	return num, nil
}

func (m *Impl) Count() (num int64, err error) {
	query := fmt.Sprintf("SELECT count(1) FROM %s", m.table)

	filterSQL, filterArgs := m.qs.GetQuerySet()

	query += filterSQL

	var resp int64
	err = m.conn.QueryRowCtx(m.ctx(), &resp, query, filterArgs...)

	switch {
	case err == nil:
		return resp, nil
	case errors.Is(err, sqlx.ErrNotFound):
		return 0, nil
	default:
		logc.Errorf(m.ctx(), "Count error: %+v", err)
		return 0, err
	}
}

func (m *Impl) FindOne() (result map[string]any, err error) {
	query := "SELECT %s FROM %s"

	selectRows := m.qs.GetSelectSQL()
	if selectRows != "*" {
		query = fmt.Sprintf(query, selectRows, m.table)
	} else {
		query = fmt.Sprintf(query, m.fieldRows, m.table)
	}

	filterSQL, filterArgs := m.qs.GetQuerySet()

	query += filterSQL
	query += m.qs.GetGroupBySQL()
	query += m.qs.GetOrderBySQL()
	query += " LIMIT 1"

	res, _ := utils.DeepCopy(m.model)

	err = m.conn.QueryRowPartialCtx(m.ctx(), res, query, filterArgs...)

	switch {
	case err == nil:
		return utils.Struct2Map(res, m.mTag), nil
	case errors.Is(err, sqlx.ErrNotFound):
		return map[string]any{}, nil
	default:
		logc.Errorf(m.ctx(), "FindOne error: %+v", err)
		return nil, err
	}
}

func (m *Impl) FindOneModel(modelPtr any) (err error) {
	query := "SELECT %s FROM %s"

	selectRows := m.qs.GetSelectSQL()
	if selectRows != "*" {
		query = fmt.Sprintf(query, selectRows, m.table)
	} else {
		query = fmt.Sprintf(query, m.fieldRows, m.table)
	}

	filterSQL, filterArgs := m.qs.GetQuerySet()

	query += filterSQL
	query += m.qs.GetGroupBySQL()
	query += m.qs.GetOrderBySQL()
	query += " LIMIT 1"

	err = m.conn.QueryRowPartialCtx(m.ctx(), modelPtr, query, filterArgs...)

	switch {
	case err == nil:
		return nil
	case errors.Is(err, sqlx.ErrNotFound):
		return sqlx.ErrNotFound
	default:
		logc.Errorf(m.ctx(), "FindOneModel error: %+v", err)
		return err
	}
}

func (m *Impl) FindAll() (result []map[string]any, err error) {
	query := "SELECT %s FROM %s"

	selectRows := m.qs.GetSelectSQL()
	if selectRows != "*" {
		query = fmt.Sprintf(query, selectRows, m.table)
	} else {
		query = fmt.Sprintf(query, m.fieldRows, m.table)
	}

	filterSQL, filterArgs := m.qs.GetQuerySet()

	query += filterSQL
	query += m.qs.GetGroupBySQL()
	query += m.qs.GetOrderBySQL()
	query += m.qs.GetLimitSQL()

	res, _ := utils.DeepCopy(m.modelSlice)

	err = m.conn.QueryRowsPartialCtx(m.ctx(), res, query, filterArgs...)

	switch {
	case err == nil:
		return utils.StructSlice2MapSlice(res, m.mTag), nil
	case errors.Is(err, sqlx.ErrNotFound):
		return []map[string]any{}, nil
	default:
		logc.Errorf(m.ctx(), "FindAll error: %+v", err)
		return nil, err
	}
}

func (m *Impl) FindAllModel(modelSlicePtr any) (err error) {
	query := "SELECT %s FROM %s"

	selectRows := m.qs.GetSelectSQL()
	if selectRows != "*" {
		query = fmt.Sprintf(query, selectRows, m.table)
	} else {
		query = fmt.Sprintf(query, m.fieldRows, m.table)
	}

	filterSQL, filterArgs := m.qs.GetQuerySet()

	query += filterSQL
	query += m.qs.GetGroupBySQL()
	query += m.qs.GetOrderBySQL()
	query += m.qs.GetLimitSQL()

	err = m.conn.QueryRowsPartialCtx(m.ctx(), modelSlicePtr, query, filterArgs...)

	switch {
	case err != nil:
		logc.Errorf(m.ctx(), "FindOneModel error: %+v", err)
		return err
	case reflect.ValueOf(modelSlicePtr).Elem().Len() == 0:
		return sqlx.ErrNotFound
	default:
		return nil
	}
}

func (m *Impl) Delete() (int64, error) {
	data := map[string]any{"is_deleted": true}

	return m.Update(data)
}

func (m *Impl) Modify(data map[string]any) (num int64, err error) {
	return m.Exclude(data).Update(data)
}

func (m *Impl) Exist() (exist bool, err error) {
	if num, err := m.Count(); err != nil {
		return false, err
	} else if num > 0 {
		return true, nil
	}

	return false, nil
}

func (m *Impl) List() (total int64, data []map[string]any, err error) {
	if total, err = m.Count(); err != nil {
		return
	}

	if data, err = m.FindAll(); err != nil {
		return
	}

	return total, data, nil
}

func (m *Impl) GetOrCreate(data map[string]any) (map[string]any, error) {
	if _, err := m.Insert(data); err != nil {
		if !errors.Is(err, ErrDuplicateKey) {
			return nil, err
		}
	}

	return m.Filter(data).FindOne()
}

func (m *Impl) CreateOrUpdate(filter map[string]any, data map[string]any) (bool, int64, error) {
	if exist, err := m.Filter(filter).Exist(); err != nil {
		return false, 0, err
	} else if exist {
		if num, err := m.Filter(filter).Update(data); err != nil {
			return false, 0, err
		} else {
			return false, num, nil
		}
	}

	id, err := m.Insert(data)
	if err != nil {
		return false, 0, err
	}
	return true, id, nil
}

func (m *Impl) GetC2CMap(column1, column2 string) (res map[any]any, err error) {
	if _, ok := m.fieldNameMap[column1]; !ok {
		err = fmt.Errorf("column [%s] not exist", column1)
		logc.Errorf(m.ctx(), err.Error())
		return nil, err
	}
	if _, ok := m.fieldNameMap[column2]; !ok {
		err = fmt.Errorf("column [%s] not exist", column2)
		logc.Errorf(m.ctx(), err.Error())
		return nil, err
	}

	query := fmt.Sprintf("SELECT `%s`,`%s` FROM %s ", column1, column2, m.table)

	filterSQL, filterArgs := m.qs.GetQuerySet()

	query += filterSQL
	query += m.qs.GetOrderBySQL()
	query += m.qs.GetLimitSQL()

	result, _ := utils.DeepCopy(m.modelSlice)

	if err = m.conn.QueryRowsPartialCtx(m.ctx(), result, query, filterArgs...); err != nil {
		logc.Errorf(m.ctx(), "GetC2CMap error: %+v", err)
		return nil, err
	}

	res = make(map[any]any)
	for _, v := range utils.StructSlice2MapSlice(result, m.mTag) {
		res[v[column1]] = v[column2]
	}

	return res, nil
}

func (m *Impl) CreateIfNotExist(data map[string]any) (id int64, created bool, err error) {
	if exist, err := m.Filter(data).Exist(); err != nil {
		return 0, false, err
	} else if exist {
		return 0, false, nil
	}

	id, err = m.Insert(data)
	if err != nil {
		return 0, false, err
	}

	return id, true, nil
}

