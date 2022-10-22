package ttl

import (
	"fmt"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/logutil"
	"strings"
	"sync"
	"time"
)

var (
	globalTTls = make([]*TTLInfo, 0, 0)
	addMutex   sync.Mutex
)

func stopAllTTls() {
	addMutex.Lock()
	defer addMutex.Unlock()

	for _, ttl := range globalTTls {
		ttl.ticker.Stop()
	}
}

func AddTTl(db, table, column, index model.CIStr) {
	addMutex.Lock()
	defer addMutex.Unlock()

	ttl := &TTLInfo{
		DB:        db,
		Table:     table,
		Column:    column,
		IndexName: index,
		ticker:    time.NewTicker(2 * time.Second),
	}

	globalTTls = append(globalTTls, ttl)
	go func() {
		select {
		case <-ttl.ticker.C:
			// TODO 检查并删除表

		default:
			return
		}
	}()
}

type TTLInfo struct {
	DB        model.CIStr
	Table     model.CIStr
	Column    model.CIStr
	IndexName model.CIStr
	ticker    *time.Ticker
}

type TTlChecker struct {
	schema *infoschema.InfoCache
	ticker *time.Ticker // 定时从 pd 获取需要做 DDL 的表
}

func InitTTlChecker(schema *infoschema.InfoCache) {
	ticker := time.NewTicker(time.Minute)
	checker := &TTlChecker{
		schema: schema,
		ticker: ticker,
	}
	go func() {
		select {
		case <-ticker.C:
			checker.syncTTl()
		}
	}()
}

func (c *TTlChecker) syncTTl() {
	logutil.BgLogger().Warn(fmt.Sprintf("sync ttl"))

	ttls := make([]*TTLInfo, 0, 0)
	schemas := c.schema.GetLatest().AllSchemas()
	for _, db := range schemas {
		logutil.BgLogger().Warn(fmt.Sprintf("check [%+v] db", db.Name))
		sts := c.schema.GetLatest().SchemaTables(db.Name)
		for _, table := range sts {
			for _, index := range table.Meta().Indices {
				if strings.Contains(index.Name.O, "_ttl") || strings.Contains(index.Name.O, "_ttl") {
					logutil.BgLogger().Warn(fmt.Sprintf("add [%+v] to ttl", index.Name))
					ttl := &TTLInfo{
						DB:        db.Name,
						Table:     table.Meta().Name,
						Column:    index.Columns[0].Name,
						IndexName: index.Name,
						ticker:    time.NewTicker(2 * time.Second),
					}
					ttls = append(ttls, ttl)
				}
			}
		}
	}

	stopAllTTls()

	addMutex.Lock()
	defer addMutex.Unlock()
	globalTTls = ttls
}
