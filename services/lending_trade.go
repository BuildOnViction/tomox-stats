package services

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/tomochain/tomox-stats/daos"
	"github.com/tomochain/tomox-stats/types"
	"github.com/tomochain/tomox-stats/utils"
)

const (
	lendingCacheFile = "lending.trade.cache"
)

// LendingTradeService struct with daos required, responsible for communicating with daos.
// LendingTradeService functions are responsible for interacting with daos and implements business logics.
type LendingTradeService struct {
	lendingTradeDao   *daos.LendingTradeDao
	lendingTradeCache *lendingTradeCache
	mutex             sync.RWMutex
}

type lendingTradeCache struct {
	lastTime          int64
	relayerUserTrades map[common.Address]map[common.Address]map[int64]*types.LendingUserTrade
}

type cachelendingtradefile struct {
	LastTime          int64                     `json:"lastTime"`
	RelayerUserTrades []*types.LendingUserTrade `json:"relayerUserTrades"`
}

// NewLendingTradeService init new instance
func NewLendingTradeService(lendingTradeDao *daos.LendingTradeDao) *LendingTradeService {

	cache := &lendingTradeCache{
		relayerUserTrades: make(map[common.Address]map[common.Address]map[int64]*types.LendingUserTrade),
	}
	return &LendingTradeService{
		lendingTradeDao:   lendingTradeDao,
		lendingTradeCache: cache,
	}
}

// WatchChanges watch lending trade notify
func (s *LendingTradeService) WatchChanges() {
	ct, sc, err := s.lendingTradeDao.Watch()
	if err != nil {
		logger.Error("Failed to open change stream")
		return
	}

	defer ct.Close()
	defer sc.Close()

	// Watch the event again in case there is error and function returned
	defer s.WatchChanges()

	ctx := context.Background()

	//Handling change stream in a cycle
	for {
		select {
		case <-ctx.Done(): // if parent context was cancelled
			err := ct.Close() // close the stream
			if err != nil {
				logger.Error("Change stream closed")
			}
			return //exiting from the func
		default:
			ev := types.LendingTradeChangeEvent{}

			//getting next item from the steam
			ok := ct.Next(&ev)

			//if data from the stream wasn't un-marshaled, we get ok == false as a result
			//so we need to call Err() method to get info why
			//it'll be nil if we just have no data
			if !ok {
				err := ct.Err()
				if err != nil {
					logger.Error(err)
					return
				}
			}

			//if item from the stream un-marshaled successfully, do something with it
			if ok {
				logger.Debugf("Operation Type: %s", ev.OperationType)
				s.NotifyTrade(ev.FullDocument)
			}
		}
	}
}

// NotifyTrade handle trade insert/update db trigger
func (s *LendingTradeService) NotifyTrade(trade *types.LendingTrade) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.updateRelayerUserTrade(trade)
	return nil
}

// Init init cache
// ensure add current time frame before trade notify come
func (s *LendingTradeService) Init() {
	now := time.Now().Unix()
	s.loadCache()
	if s.lendingTradeCache.lastTime == 0 {
		s.lendingTradeCache.lastTime = time.Now().Unix() - intervalCrawl
	}
	s.fetch(s.lendingTradeCache.lastTime, now)
	s.commitCache()
	ticker := time.NewTicker(60 * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				err := s.commitCache()
				if err != nil {
					logger.Error(err)
				}
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func (s *LendingTradeService) fetch(fromdate int64, todate int64) {
	pageOffset := 0
	size := 1000
	for {
		trades, err := s.lendingTradeDao.GetLendingTradeByTime(fromdate, todate, pageOffset*size, size)
		logger.Debug("FETCH DATA", pageOffset*size)
		if err != nil || len(trades) == 0 {
			break
		}
		s.mutex.Lock()
		for _, trade := range trades {
			s.updateRelayerUserTrade(trade)
			s.lendingTradeCache.lastTime = trade.CreatedAt.Unix()

		}
		s.mutex.Unlock()
		pageOffset = pageOffset + 1
	}
}

func (s *LendingTradeService) flattenRelayerUserTrades() []*types.LendingUserTrade {
	var relayerUserTrades []*types.LendingUserTrade
	for _, tradebyRelayer := range s.lendingTradeCache.relayerUserTrades {
		for _, tradebyUserAddess := range tradebyRelayer {
			for _, trade := range tradebyUserAddess {
				relayerUserTrades = append(relayerUserTrades, trade)
			}
		}
	}
	return relayerUserTrades
}

func (s *LendingTradeService) commitCache() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	relayerUserTrades := s.flattenRelayerUserTrades()
	cachefile := &cachelendingtradefile{
		LastTime:          s.lendingTradeCache.lastTime,
		RelayerUserTrades: relayerUserTrades,
	}
	cacheData, err := json.Marshal(cachefile)
	if err != nil {
		return err
	}
	file, err := os.Create(lendingCacheFile)
	defer file.Close()
	if err == nil {
		_, err = file.Write(cacheData)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *LendingTradeService) loadCache() error {
	file, err := os.Open(lendingCacheFile)
	defer file.Close()
	if err != nil {
		return err
	}
	stats, statsErr := file.Stat()
	if statsErr != nil {
		return statsErr
	}

	size := stats.Size()
	bytes := make([]byte, size)
	bufr := bufio.NewReader(file)
	_, err = bufr.Read(bytes)
	var cache cachelendingtradefile
	err = json.Unmarshal(bytes, &cache)
	if err != nil {
		return err
	}
	for _, t := range cache.RelayerUserTrades {
		s.addRelayerUserTrade(t)
	}
	s.lendingTradeCache.lastTime = cache.LastTime
	return nil
}

func (s *LendingTradeService) getCacheKeyString(term uint64, lendingToken common.Address) string {
	return fmt.Sprintf("%d::%s", term, lendingToken)
}

func (s *LendingTradeService) parsePairString(key string) (common.Address, common.Address, error) {
	tokens := strings.Split(key, "::")
	if len(tokens) == 2 {
		return common.HexToAddress(tokens[0]), common.HexToAddress(tokens[1]), nil
	}
	return common.Address{}, common.Address{}, errors.New("Invalid Key")
}

// updateRelayerTick update lastest tick, need to be lock
func (s *LendingTradeService) updateRelayerUserTrade(trade *types.LendingTrade) error {
	tradeTime := trade.CreatedAt.Unix()
	modTime, _ := utils.GetModTime(tradeTime, duration, unit)
	if trade.BorrowingRelayer.Hex() == trade.InvestingRelayer.Hex() {
		// BorrowingRelayer == InvestingRelayer
		if _, ok := s.lendingTradeCache.relayerUserTrades[trade.BorrowingRelayer]; !ok {
			s.lendingTradeCache.relayerUserTrades[trade.BorrowingRelayer] = make(map[common.Address]map[int64]*types.LendingUserTrade)
		}
		if trade.Investor.Hex() == trade.Borrower.Hex() {
			// BorrowingRelayer == InvestingRelayer, Investor == Borrower
			if _, ok := s.lendingTradeCache.relayerUserTrades[trade.BorrowingRelayer][trade.Borrower]; !ok {
				s.lendingTradeCache.relayerUserTrades[trade.BorrowingRelayer][trade.Borrower] = make(map[int64]*types.LendingUserTrade)
			}
			if last, ok := s.lendingTradeCache.relayerUserTrades[trade.BorrowingRelayer][trade.Borrower][modTime]; !ok {
				userTrade := &types.LendingUserTrade{
					UserAddress:    trade.Investor,
					Count:          big.NewInt(1),
					RelayerAddress: trade.BorrowingRelayer,
				}
				s.lendingTradeCache.relayerUserTrades[trade.BorrowingRelayer][trade.Borrower][modTime] = userTrade
			} else {
				last.Count = new(big.Int).Add(last.Count, big.NewInt(1))
			}
		} else {
			// BorrowingRelayer == InvestingRelayer, Investor # Borrower
			if _, ok := s.lendingTradeCache.relayerUserTrades[trade.BorrowingRelayer][trade.Borrower]; !ok {
				s.lendingTradeCache.relayerUserTrades[trade.BorrowingRelayer][trade.Borrower] = make(map[int64]*types.LendingUserTrade)
			}
			if _, ok := s.lendingTradeCache.relayerUserTrades[trade.InvestingRelayer][trade.Investor]; !ok {
				s.lendingTradeCache.relayerUserTrades[trade.InvestingRelayer][trade.Investor] = make(map[int64]*types.LendingUserTrade)
			}

			if last, ok := s.lendingTradeCache.relayerUserTrades[trade.BorrowingRelayer][trade.Borrower][modTime]; !ok {
				userTrade := &types.LendingUserTrade{
					UserAddress:    trade.Borrower,
					Count:          big.NewInt(1),
					RelayerAddress: trade.BorrowingRelayer,
				}
				s.lendingTradeCache.relayerUserTrades[trade.BorrowingRelayer][trade.Borrower][modTime] = userTrade
			} else {
				last.Count = new(big.Int).Add(last.Count, big.NewInt(1))
			}

			if last, ok := s.lendingTradeCache.relayerUserTrades[trade.InvestingRelayer][trade.Investor][modTime]; !ok {
				userTrade := &types.LendingUserTrade{
					UserAddress:    trade.Investor,
					Count:          big.NewInt(1),
					RelayerAddress: trade.InvestingRelayer,
				}
				s.lendingTradeCache.relayerUserTrades[trade.InvestingRelayer][trade.Investor][modTime] = userTrade
			} else {
				last.Count = new(big.Int).Add(last.Count, big.NewInt(1))
			}

		}
	} else {
		// BorrowingRelayer # InvestingRelayer
		if _, ok := s.lendingTradeCache.relayerUserTrades[trade.BorrowingRelayer]; !ok {
			s.lendingTradeCache.relayerUserTrades[trade.BorrowingRelayer] = make(map[common.Address]map[int64]*types.LendingUserTrade)
		}
		if _, ok := s.lendingTradeCache.relayerUserTrades[trade.InvestingRelayer]; !ok {
			s.lendingTradeCache.relayerUserTrades[trade.InvestingRelayer] = make(map[common.Address]map[int64]*types.LendingUserTrade)
		}
		if _, ok := s.lendingTradeCache.relayerUserTrades[trade.BorrowingRelayer][trade.Borrower]; !ok {
			s.lendingTradeCache.relayerUserTrades[trade.BorrowingRelayer][trade.Borrower] = make(map[int64]*types.LendingUserTrade)
		}
		if _, ok := s.lendingTradeCache.relayerUserTrades[trade.InvestingRelayer][trade.Investor]; !ok {
			s.lendingTradeCache.relayerUserTrades[trade.InvestingRelayer][trade.Investor] = make(map[int64]*types.LendingUserTrade)
		}
		if last, ok := s.lendingTradeCache.relayerUserTrades[trade.InvestingRelayer][trade.Investor][modTime]; !ok {
			userTrade := &types.LendingUserTrade{
				UserAddress:    trade.Investor,
				Count:          big.NewInt(1),
				RelayerAddress: trade.InvestingRelayer,
			}
			s.lendingTradeCache.relayerUserTrades[trade.InvestingRelayer][trade.Investor][modTime] = userTrade
		} else {
			last.Count = new(big.Int).Add(last.Count, big.NewInt(1))
		}

		if last, ok := s.lendingTradeCache.relayerUserTrades[trade.BorrowingRelayer][trade.Borrower][modTime]; !ok {
			userTrade := &types.LendingUserTrade{
				UserAddress:    trade.Borrower,
				Count:          big.NewInt(1),
				RelayerAddress: trade.BorrowingRelayer,
			}
			s.lendingTradeCache.relayerUserTrades[trade.BorrowingRelayer][trade.Borrower][modTime] = userTrade
		} else {
			last.Count = new(big.Int).Add(last.Count, big.NewInt(1))
		}
	}

	return nil
}

func (s *LendingTradeService) addRelayerUserTrade(userTrade *types.LendingUserTrade) {
	if _, ok := s.lendingTradeCache.relayerUserTrades[userTrade.RelayerAddress]; !ok {
		s.lendingTradeCache.relayerUserTrades[userTrade.RelayerAddress] = make(map[common.Address]map[int64]*types.LendingUserTrade)
	}
	if _, ok := s.lendingTradeCache.relayerUserTrades[userTrade.RelayerAddress][userTrade.UserAddress]; !ok {
		s.lendingTradeCache.relayerUserTrades[userTrade.RelayerAddress][userTrade.UserAddress] = make(map[int64]*types.LendingUserTrade)
	}
	s.lendingTradeCache.relayerUserTrades[userTrade.RelayerAddress][userTrade.UserAddress][userTrade.TimeStamp] = userTrade
}

// GetNumberTraderByTime get number trader bytime
func (s *LendingTradeService) GetNumberTraderByTime(relayerAddress common.Address, dateFrom, dateTo int64) int {
	users := make(map[common.Address]bool)
	if tradebyrelayerAddress, ok := s.lendingTradeCache.relayerUserTrades[relayerAddress]; ok {
		for address, tradeuserbyaddress := range tradebyrelayerAddress {
			for t := range tradeuserbyaddress {
				if (t >= dateFrom || dateFrom == 0) && (t <= dateTo || dateTo == 0) {
					users[address] = true
				}
			}
		}
	}
	return len(users)
}
