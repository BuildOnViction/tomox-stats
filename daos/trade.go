package daos

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/tomochain/viewdex/app"
	"github.com/tomochain/viewdex/types"
)

// TradeDao contains:
// collectionName: MongoDB collection name
// dbName: name of mongodb to interact with
type TradeDao struct {
	collectionName string
	dbName         string
}

// NewTradeDao returns a new instance of TradeDao.
func NewTradeDao() *TradeDao {
	dbName := app.Config.DBName
	collection := "trades"
	return &TradeDao{collection, dbName}
}

// Watch notfy trade record
func (dao *TradeDao) Watch() (*mgo.ChangeStream, *mgo.Session, error) {
	return db.Watch(dao.dbName, dao.collectionName, mgo.ChangeStreamOptions{
		FullDocument:   mgo.UpdateLookup,
		MaxAwaitTimeMS: 500,
		BatchSize:      1000,
	})
}

// GetAll function fetches all the trades in mongodb
func (dao *TradeDao) GetAll() ([]types.Trade, error) {
	var response []types.Trade
	err := db.Get(dao.dbName, dao.collectionName, bson.M{}, 0, 0, &response)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return response, nil
}

// GetByHash fetches the first record that matches a certain hash
func (dao *TradeDao) GetByHash(h common.Hash) (*types.Trade, error) {
	q := bson.M{"hash": h.Hex()}

	res := []*types.Trade{}
	err := db.Get(dao.dbName, dao.collectionName, q, 0, 0, &res)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return res[0], nil
}

// GetTrades filter trade
func (dao *TradeDao) GetTrades(tradeSpec *types.TradeSpec, sortedBy []string, pageOffset int, pageSize int) (*types.TradeRes, error) {
	q := bson.M{}

	if tradeSpec.DateFrom != 0 || tradeSpec.DateTo != 0 {
		dateFilter := bson.M{}
		if tradeSpec.DateFrom != 0 {
			dateFilter["$gte"] = time.Unix(tradeSpec.DateFrom, 0)
		}
		if tradeSpec.DateTo != 0 {
			dateFilter["$lt"] = time.Unix(tradeSpec.DateTo, 0)
		}
		q["createdAt"] = dateFilter
	}
	if tradeSpec.BaseToken != "" {
		q["baseToken"] = tradeSpec.BaseToken
	}
	if tradeSpec.QuoteToken != "" {
		q["quoteToken"] = tradeSpec.QuoteToken
	}

	var res types.TradeRes
	trades := []*types.Trade{}
	c, err := db.GetEx(dao.dbName, dao.collectionName, q, sortedBy, pageOffset, pageSize, &trades)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	res.Total = c
	res.Trades = trades
	return &res, nil
}

// GetTradeByTime get range trade
func (dao *TradeDao) GetTradeByTime(dateFrom, dateTo int64, pageOffset int, pageSize int) ([]*types.Trade, error) {
	q := bson.M{}

	dateFilter := bson.M{}
	dateFilter["$gte"] = time.Unix(dateFrom, 0)
	dateFilter["$lt"] = time.Unix(dateTo, 0)
	q["createdAt"] = dateFilter

	trades := []*types.Trade{}
	_, err := db.GetEx(dao.dbName, dao.collectionName, q, []string{"+createdAt"}, pageOffset, pageSize, &trades)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	return trades, nil
}
