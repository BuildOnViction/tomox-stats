package daos

import (
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/tomochain/tomox-stats/app"
	"github.com/tomochain/tomox-stats/types"
)

// LendingTradeDao contains:
// collectionName: MongoDB collection name
// dbName: name of mongodb to interact with
type LendingTradeDao struct {
	collectionName string
	dbName         string
}

// NewLendingTradeDao returns a new instance of LendingTradeDao.
func NewLendingTradeDao() *LendingTradeDao {
	dbName := app.Config.DBName
	collection := "lending_trades"

	i3 := mgo.Index{
		Key: []string{"createdAt"},
	}
	db.Session.DB(dbName).C(collection).EnsureIndex(i3)

	return &LendingTradeDao{collection, dbName}
}

// GetCollection get trade collection name
func (dao *LendingTradeDao) GetCollection() *mgo.Collection {
	return db.GetCollection(dao.dbName, dao.collectionName)
}

// Watch changing database
func (dao *LendingTradeDao) Watch() (*mgo.ChangeStream, *mgo.Session, error) {
	return db.Watch(dao.dbName, dao.collectionName, mgo.ChangeStreamOptions{
		FullDocument:   mgo.UpdateLookup,
		MaxAwaitTimeMS: 500,
		BatchSize:      1000,
	})
}

// GetAll function fetches all the trades in mongodb
func (dao *LendingTradeDao) GetAll() ([]types.LendingTrade, error) {
	var response []types.LendingTrade
	err := db.Get(dao.dbName, dao.collectionName, bson.M{}, 0, 0, &response)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return response, nil
}

// GetLendingTradeByTime get range trade
func (dao *LendingTradeDao) GetLendingTradeByTime(dateFrom, dateTo int64, pageOffset int, pageSize int) ([]*types.LendingTrade, error) {
	q := bson.M{}

	dateFilter := bson.M{}
	dateFilter["$gte"] = time.Unix(dateFrom, 0)
	dateFilter["$lt"] = time.Unix(dateTo, 0)
	q["createdAt"] = dateFilter

	trades := []*types.LendingTrade{}
	_, err := db.GetEx(dao.dbName, dao.collectionName, q, []string{"-createdAt"}, pageOffset, pageSize, &trades)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	return trades, nil
}
