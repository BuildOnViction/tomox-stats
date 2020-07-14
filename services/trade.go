package services

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/tomochain/tomox-stats/daos"
	"github.com/tomochain/tomox-stats/types"
	"github.com/tomochain/tomox-stats/utils"
)

const (
	duration         = 1
	unit             = "hour"
	sideBuy          = "BUY"
	sideSell         = "SELL"
	cacheTimeLifeMax = 15 * 50
	intervalCrawl    = 60 * 24 * 60 * 60
)

var e1 = []string{
	"0xbfc6e92daae38d49a978245e04acc98178770a36",
	"0x9d62a70c8e3587f0051ad56a111feb738a43103e",
	"0x0798ca0782fddf1a57d09587cc2dea4c139294f7",
	"0xab45d2804945a5f443189a435d61808d4659447c",
	"0xc9c75e98d79a8987d5a0b1ed9e2226e637570587",
	"0xf17278d65fd53ae68661ab0361fcefb0919b64cf",
	"0x881e1caae2f97601701c762d73f1c83fd186cf0c",
	"0x92875d7345d8447eb9cba401caa4959343772336",
	"0x2a941348945811697d669802d7abaaa4fc544af2",
	"0xc5352eff365260a640fc237d2c22ccfeb68c44e6",
	"0x95a05686f861be57823bbb308e2f71fbaab05cc6",
	"0xb76dbf54d84954f0a5da6f9ea00047541853688c",
	"0xa1f9950b0673b3df6aecec0e040c835c6d78b51a",
	"0xe681eeb30bad216e1d679cdfdb7eda5d9c362f9b",
	"0xd081777a391b9bbb15b0ae1e4103fd18689bdb3c",
	"0xf17278d65fd53ae68661ab0361fcefb0919b64cf",
}

var e2 = []string{
	"0x9d62A70c8E3587f0051AD56A111fEb738a43103E",
	"0xbfc6e92daae38d49a978245e04acc98178770a36",
	"0x90cBE91913075dD48ceC8528bD82e47BA15ffab4",
	"0x62bb16DC0aED004EF877acE866E8f558E6e351A2",
	"0xab45d2804945a5f443189a435d61808d4659447c",
	"0xe36dEd7Bc36f60D0Cd6D1A0dEBda6dD999BBf199",
	"0xf996aE46adad51f2Bb0C0879670CbAe466202E4D",
	"0x02e45aDF6025553d6df357415FF624d375Abf2B8",
	"0x717f876aD79773AB3290A2bcf9244B1C8F9602e1",
	"0xf742536b95B2E3bCfC48543AE93d4Ebf93f3c537",
	"0xB76dbf54d84954F0A5DA6F9EA00047541853688c",
	"0x95a05686f861be57823bbb308e2f71fbaab05cc6",
	"0x0798ca0782fddF1a57d09587CC2Dea4C139294f7",
	"0x0798ca0782fddF1a57d09587CC2Dea4C139294f7",
	"0xe04AEc262fBbc434cF1781c49d4236914765a33b",
	"0xab45d2804945a5f443189a435d61808d4659447c",
}

var bot = []string{
	"0x9E7c130D6EA105450dD8DdD51dd6fAB4b5c955d7",
	"0xF8ec1939AF7F37F53156d554fe8187E2Cdf4A060",
	"0x0F8469Ead31Ffd4b6b504dba35B0Cf3C8DFf4e14",
	"0x569874387d94F9efF87Bbe1e94f2308681F64223",
	"0x01F294DBdD3207fE86Af1b68a697dA4d1296B75B",

	"0x0cd9d70a38b71ed056e5d20fef6e3c9d2f6bc253",
	"0xc923f5f834b45674bac07c85f0328fa713de3fe2",
	"0xf742536b95b2e3bcfc48543ae93d4ebf93f3c537",
	"0xf742536b95b2e3bcfc48543ae93d4ebf93f3c537",
	"0xf996ae46adad51f2bb0c0879670cbae466202e4d",

	"0x988cE4471422Af884Ef42c7d6dd6B6986F1Eaf80",
	"0xA13948aD209FD8967da18B7c2E4841133f725F01",
	"0xa9E590c7B76be6f473a308669B9373199219581C",
	"0xa2191f5aeA9aCAF5875E5b42A6DbA378C5f0fbed",
	"0xC65Ac99fC0c96330504B733EB01e9d58dAD93FA6",

	"0x90CA4896779C8BC4cAf9085Cfc73F54B17Bc0e09",
	"0x02e45aDF6025553d6df357415FF624d375Abf2B8",
	"0x42224E7404D0c037B6D7aB7027A74b22f3190302",
	"0x0BDbC4E0e19CE1C8129612CcD1EB4eBa94FE064B",
	"0xF8032b6ef6843cE4938EB742742F4cB5C91236A0",

	"0x548bA11486a2bC522Ab864773a5fC6Ffd9777A1f",
	"0xb10010Bf1C3AA108170e1C3fE2662B8a462E6040",
	"0x859695a2C648014a31d307e73CF688e009317F9E",
	"0x717f876aD79773AB3290A2bcf9244B1C8F9602e1",
	"0x628Cd98302aBD2Cf30a8e54539410D726Bb0e5F6",

	"0xc683608c1125717F0C459442d5830DDCd321704C",
	"0x95A47f00F14AEbE3C893F6978957af036Aae4627",
	"0xe92a0B625E8Be02AD88918B67d752274737Db09b",
	"0x0bF9BDEE7d4f572033187D001dEc079331D833Ed",
	"0xa46fbe3Bf444ffFb8BDAd3F682bda5cBec7F0ebC",
}

// TradeService struct with daos required, responsible for communicating with daos.
// TradeService functions are responsible for interacting with daos and implements business logics.
type TradeService struct {
	tradeDao      *daos.TradeDao
	tokenDao      *daos.TokenDao
	tradeCache    *tradeCache
	tokenCache    map[common.Address]*tokenCache
	lastPairPrice map[string]*big.Int
	mutex         sync.RWMutex
}

type tradeCache struct {
	lastTime int64
	// pairAddress => userAddress =>  time => UserTrade
	userTrades map[string]map[common.Address]map[int64]*types.UserTrade
	// relayerAddress => pairAddress => time => RelayerTrade
	relayerTrades map[common.Address]map[string]map[int64]*types.RelayerTrade
	// relayerAddress => pairAddress => userAddress => time => UserTrade
	relayerUserTrades map[common.Address]map[string]map[common.Address]map[int64]*types.UserTrade
}

type cachetradefile struct {
	LastTime          int64              `json:"lastTime"`
	UserTrades        []*types.UserTrade `json:"userTrades"`
	RelayerUserTrades []*types.UserTrade `json:"relayerUserTrades"`
}
type tokenCache struct {
	token    *types.Token
	timelife int64
}

// NewTradeService init new instance
func NewTradeService(tokenDao *daos.TokenDao, tradeDao *daos.TradeDao) *TradeService {

	cache := &tradeCache{
		userTrades:        make(map[string]map[common.Address]map[int64]*types.UserTrade),
		relayerTrades:     make(map[common.Address]map[string]map[int64]*types.RelayerTrade),
		relayerUserTrades: make(map[common.Address]map[string]map[common.Address]map[int64]*types.UserTrade),
	}
	return &TradeService{
		tokenDao:      tokenDao,
		tradeDao:      tradeDao,
		tradeCache:    cache,
		tokenCache:    make(map[common.Address]*tokenCache),
		lastPairPrice: make(map[string]*big.Int),
	}
}

func (s *TradeService) getTokenByAddress(token common.Address) (*types.Token, error) {
	now := time.Now().Unix()
	if tokenCache, ok := s.tokenCache[token]; ok {
		if now-tokenCache.timelife < cacheTimeLifeMax {
			return tokenCache.token, nil
		}
		delete(s.tokenCache, token)
	}
	t, err := s.tokenDao.GetByAddress(token)
	if err == nil && t != nil {
		s.tokenCache[token] = &tokenCache{
			token:    t,
			timelife: now,
		}
	}
	return t, err
}

// WatchChanges watch trade record insert/update
func (s *TradeService) WatchChanges() {

	ct, sc, err := s.tradeDao.Watch()

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
			ev := types.TradeChangeEvent{}

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
func (s *TradeService) NotifyTrade(trade *types.Trade) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	key := s.getPairString(trade.BaseToken, trade.QuoteToken)
	s.lastPairPrice[key] = trade.PricePoint
	s.updateRelayerUserTrade(trade)
	s.updateUserTrade(trade)
	return nil
}

// Init init cache
// ensure add current time frame before trade notify come
func (s *TradeService) Init() {
	logger.Info("OHLCV init starting...")
	now := time.Now().Unix()
	s.loadCache()
	if s.tradeCache.lastTime == 0 {
		s.tradeCache.lastTime = time.Now().Unix() - intervalCrawl
	}
	s.fetch(s.tradeCache.lastTime, now)
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

	logger.Info("OHLCV finished")
}

func (s *TradeService) fetch(fromdate int64, todate int64) {
	pageOffset := 0
	size := 1000
	for {
		trades, err := s.tradeDao.GetTradeByTime(fromdate, todate, pageOffset*size, size)
		logger.Debug("FETCH DATA", pageOffset*size)
		if err != nil || len(trades) == 0 {
			break
		}
		s.mutex.Lock()
		for _, trade := range trades {
			s.updateUserTrade(trade)
			s.updateRelayerUserTrade(trade)
			s.tradeCache.lastTime = trade.CreatedAt.Unix()

		}
		s.mutex.Unlock()
		pageOffset = pageOffset + 1
	}
}

func (s *TradeService) flattenUserTrades() []*types.UserTrade {
	var userTrades []*types.UserTrade
	for _, tradebyUserAddress := range s.tradeCache.userTrades {
		for _, tradebyTime := range tradebyUserAddress {
			for _, trade := range tradebyTime {
				userTrades = append(userTrades, trade)
			}
		}
	}
	return userTrades
}

func (s *TradeService) flattenRelayerUserTrades() []*types.UserTrade {
	var relayerUserTrades []*types.UserTrade
	for _, tradebyRelayer := range s.tradeCache.relayerUserTrades {
		for _, tradebyUserAddess := range tradebyRelayer {
			for _, tradeBytime := range tradebyUserAddess {
				for _, trade := range tradeBytime {
					relayerUserTrades = append(relayerUserTrades, trade)
				}
			}
		}
	}
	return relayerUserTrades
}

func (s *TradeService) commitCache() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	logger.Info("commit trade cache")
	userTrades := s.flattenUserTrades()
	relayerUserTrades := s.flattenRelayerUserTrades()
	cachefile := &cachetradefile{
		LastTime:          s.tradeCache.lastTime,
		UserTrades:        userTrades,
		RelayerUserTrades: relayerUserTrades,
	}
	cacheData, err := json.Marshal(cachefile)
	if err != nil {
		return err
	}
	file, err := os.Create("trade.cache")
	defer file.Close()
	if err == nil {
		_, err = file.Write(cacheData)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *TradeService) loadCache() error {
	file, err := os.Open("trade.cache")
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
	var cache cachetradefile
	err = json.Unmarshal(bytes, &cache)
	if err != nil {
		return err
	}
	for _, t := range cache.UserTrades {
		s.addUserTrade(t)
	}
	for _, t := range cache.RelayerUserTrades {
		s.addRelayerUserTrade(t)
	}
	s.tradeCache.lastTime = cache.LastTime
	return nil
}

func (s *TradeService) getPairString(baseToken, quoteToken common.Address) string {
	return fmt.Sprintf("%s::%s", baseToken.Hex(), quoteToken.Hex())
}

func (s *TradeService) parsePairString(key string) (common.Address, common.Address, error) {
	tokens := strings.Split(key, "::")
	if len(tokens) == 2 {
		return common.HexToAddress(tokens[0]), common.HexToAddress(tokens[1]), nil
	}
	return common.Address{}, common.Address{}, errors.New("Invalid Key")
}

func (s *TradeService) getVolumeByQuote(baseToken, quoteToken common.Address, amount *big.Int, price *big.Int) *big.Int {
	token, err := s.getTokenByAddress(baseToken)
	if err == nil && token != nil {
		baseTokenDecimalBig := big.NewInt(int64(math.Pow10(token.Decimals)))
		p := new(big.Int).Mul(amount, price)
		return new(big.Int).Div(p, baseTokenDecimalBig)
	}
	return big.NewInt(0)
}

// updateUserTrade user trade in all relayer
func (s *TradeService) updateUserTrade(trade *types.Trade) error {
	tradeTime := trade.CreatedAt.Unix()
	key := s.getPairString(trade.BaseToken, trade.QuoteToken)
	if _, ok := s.tradeCache.userTrades[key]; !ok {
		s.tradeCache.userTrades[key] = make(map[common.Address]map[int64]*types.UserTrade)
	}

	if _, ok := s.tradeCache.userTrades[key][trade.Maker]; !ok {
		s.tradeCache.userTrades[key][trade.Maker] = make(map[int64]*types.UserTrade)
	}
	if _, ok := s.tradeCache.userTrades[key][trade.Taker]; !ok {
		s.tradeCache.userTrades[key][trade.Taker] = make(map[int64]*types.UserTrade)
	}

	modTime, _ := utils.GetModTime(tradeTime, duration, unit)
	volumeByQuote := s.getVolumeByQuote(trade.BaseToken, trade.QuoteToken, trade.Amount, trade.PricePoint)

	if trade.Taker.Hex() == trade.Maker.Hex() {
		if last, ok2 := s.tradeCache.userTrades[key][trade.Taker][modTime]; ok2 {
			last.Count = last.Count.Add(last.Count, big.NewInt(1))
			last.Volume = big.NewInt(0).Add(last.Volume, trade.Amount)
			last.VolumeByQuote = big.NewInt(0).Add(last.VolumeByQuote, volumeByQuote)
			last.VolumeAsk = big.NewInt(0).Add(last.VolumeAsk, trade.Amount)
			last.VolumeBid = big.NewInt(0).Add(last.VolumeBid, trade.Amount)
			last.VolumeAskByQuote = big.NewInt(0).Add(last.VolumeAskByQuote, volumeByQuote)
			last.VolumeBidByQuote = big.NewInt(0).Add(last.VolumeBidByQuote, volumeByQuote)

		} else {
			userTrade := &types.UserTrade{
				UserAddress:      trade.Maker,
				Count:            big.NewInt(1),
				Volume:           trade.Amount,
				VolumeByQuote:    volumeByQuote,
				VolumeAskByQuote: volumeByQuote,
				VolumeAsk:        trade.Amount,
				VolumeBid:        trade.Amount,
				VolumeBidByQuote: volumeByQuote,
				BaseToken:        trade.BaseToken,
				QuoteToken:       trade.QuoteToken,
				TimeStamp:        modTime,
			}
			s.tradeCache.userTrades[key][trade.Taker][modTime] = userTrade
		}
	} else {

		if last, ok2 := s.tradeCache.userTrades[key][trade.Taker][modTime]; ok2 {
			last.Count = last.Count.Add(last.Count, big.NewInt(1))
			last.Volume = big.NewInt(0).Add(last.Volume, trade.Amount)
			last.VolumeByQuote = big.NewInt(0).Add(last.VolumeByQuote, volumeByQuote)
			if trade.TakerOrderSide == sideBuy {
				last.VolumeAsk = big.NewInt(0).Add(last.VolumeAsk, trade.Amount)
				last.VolumeAskByQuote = big.NewInt(0).Add(last.VolumeAskByQuote, volumeByQuote)
			} else {
				last.VolumeBid = big.NewInt(0).Add(last.VolumeBid, trade.Amount)
				last.VolumeBidByQuote = big.NewInt(0).Add(last.VolumeBidByQuote, volumeByQuote)
			}

		} else {
			userTrade := &types.UserTrade{
				UserAddress:   trade.Taker,
				Count:         big.NewInt(1),
				Volume:        trade.Amount,
				VolumeByQuote: volumeByQuote,
				BaseToken:     trade.BaseToken,
				QuoteToken:    trade.QuoteToken,
				TimeStamp:     modTime,
			}
			if trade.TakerOrderSide == sideBuy {
				userTrade.VolumeBid = trade.Amount
				userTrade.VolumeBidByQuote = volumeByQuote
				userTrade.VolumeAsk = big.NewInt(0)
				userTrade.VolumeAskByQuote = big.NewInt(0)
			} else {
				userTrade.VolumeBid = big.NewInt(0)
				userTrade.VolumeBidByQuote = big.NewInt(0)
				userTrade.VolumeAsk = trade.Amount
				userTrade.VolumeAskByQuote = volumeByQuote
			}
			s.tradeCache.userTrades[key][trade.Taker][modTime] = userTrade
		}

		if last, ok2 := s.tradeCache.userTrades[key][trade.Maker][modTime]; ok2 {
			last.Count = last.Count.Add(last.Count, big.NewInt(1))
			last.Volume = big.NewInt(0).Add(last.Volume, trade.Amount)
			last.VolumeByQuote = big.NewInt(0).Add(last.VolumeByQuote, volumeByQuote)
			if trade.TakerOrderSide == sideSell {
				last.VolumeAsk = big.NewInt(0).Add(last.VolumeAsk, trade.Amount)
				last.VolumeAskByQuote = big.NewInt(0).Add(last.VolumeAskByQuote, volumeByQuote)
			} else {
				last.VolumeBid = big.NewInt(0).Add(last.VolumeBid, trade.Amount)
				last.VolumeBidByQuote = big.NewInt(0).Add(last.VolumeBidByQuote, volumeByQuote)
			}

		} else {
			userTrade := &types.UserTrade{
				UserAddress:   trade.Maker,
				Count:         big.NewInt(1),
				Volume:        trade.Amount,
				VolumeByQuote: volumeByQuote,
				BaseToken:     trade.BaseToken,
				QuoteToken:    trade.QuoteToken,
				TimeStamp:     modTime,
			}
			if trade.TakerOrderSide == sideSell {
				userTrade.VolumeBid = trade.Amount
				userTrade.VolumeBidByQuote = volumeByQuote
				userTrade.VolumeAsk = big.NewInt(0)
				userTrade.VolumeAskByQuote = big.NewInt(0)
			} else {
				userTrade.VolumeBid = big.NewInt(0)
				userTrade.VolumeBidByQuote = big.NewInt(0)
				userTrade.VolumeAsk = trade.Amount
				userTrade.VolumeAskByQuote = volumeByQuote
			}
			s.tradeCache.userTrades[key][trade.Maker][modTime] = userTrade
		}

	}
	return nil
}
func (s *TradeService) isBotAddress(t common.Address) bool {
	for _, v := range bot {
		if strings.ToLower(v) == strings.ToLower(t.Hex()) {
			return true
		}
	}
	return false
}
func (s *TradeService) isWashTrade(t1, t2 common.Address) bool {
	if t1.Hex() == t2.Hex() {
		return true
	}
	for i, v := range e1 {
		if strings.ToLower(v) == strings.ToLower(t1.Hex()) {
			if strings.ToLower(e2[i]) == strings.ToLower(t2.Hex()) {
				return true
			}
		}
		if strings.ToLower(e2[i]) == strings.ToLower(t1.Hex()) {
			if strings.ToLower(v) == strings.ToLower(t2.Hex()) {
				return true
			}
		}

	}
	return false
}

// updateRelayerTick update lastest tick, need to be lock
func (s *TradeService) updateRelayerUserTrade(trade *types.Trade) error {
	if s.isWashTrade(trade.Maker, trade.Taker) {
		return nil
	}
	tradeTime := trade.CreatedAt.Unix()
	key := s.getPairString(trade.BaseToken, trade.QuoteToken)
	exchange := make(map[common.Address]bool)
	exchange[trade.MakerExchange] = true
	exchange[trade.TakerExchange] = true
	for addr := range exchange {
		if _, ok := s.tradeCache.relayerUserTrades[addr]; !ok {
			s.tradeCache.relayerUserTrades[addr] = make(map[string]map[common.Address]map[int64]*types.UserTrade)
		}
		if _, ok := s.tradeCache.relayerUserTrades[addr][key]; !ok {
			s.tradeCache.relayerUserTrades[addr][key] = make(map[common.Address]map[int64]*types.UserTrade)
		}
		if _, ok := s.tradeCache.relayerUserTrades[addr][key][trade.Maker]; !ok {
			s.tradeCache.relayerUserTrades[addr][key][trade.Maker] = make(map[int64]*types.UserTrade)
		}
		if _, ok := s.tradeCache.relayerUserTrades[addr][key][trade.Taker]; !ok {
			s.tradeCache.relayerUserTrades[addr][key][trade.Taker] = make(map[int64]*types.UserTrade)
		}
	}

	modTime, _ := utils.GetModTime(tradeTime, duration, unit)
	volumeByQuote := s.getVolumeByQuote(trade.BaseToken, trade.QuoteToken, trade.Amount, trade.PricePoint)

	for addr := range exchange {
		if trade.Taker.Hex() == trade.Maker.Hex() {
			if last, ok2 := s.tradeCache.relayerUserTrades[addr][key][trade.Taker][modTime]; ok2 {
				last.Count = last.Count.Add(last.Count, big.NewInt(1))
				last.Volume = big.NewInt(0).Add(last.Volume, trade.Amount)
				last.VolumeByQuote = big.NewInt(0).Add(last.VolumeByQuote, volumeByQuote)
				last.VolumeAsk = big.NewInt(0).Add(last.VolumeAsk, trade.Amount)
				last.VolumeBid = big.NewInt(0).Add(last.VolumeBid, trade.Amount)
				last.VolumeAskByQuote = big.NewInt(0).Add(last.VolumeAskByQuote, volumeByQuote)
				last.VolumeBidByQuote = big.NewInt(0).Add(last.VolumeBidByQuote, volumeByQuote)

			} else {
				userTrade := &types.UserTrade{
					UserAddress:      trade.Maker,
					Count:            big.NewInt(1),
					Volume:           utils.CloneBigInt(trade.Amount),
					VolumeByQuote:    utils.CloneBigInt(volumeByQuote),
					VolumeAskByQuote: utils.CloneBigInt(volumeByQuote),
					VolumeAsk:        utils.CloneBigInt(trade.Amount),
					VolumeBid:        utils.CloneBigInt(trade.Amount),
					VolumeBidByQuote: utils.CloneBigInt(volumeByQuote),
					TimeStamp:        modTime,
					RelayerAddress:   addr,
					BaseToken:        trade.BaseToken,
					QuoteToken:       trade.QuoteToken,
				}
				s.tradeCache.relayerUserTrades[addr][key][trade.Taker][modTime] = userTrade
			}
		} else {

			if last, ok2 := s.tradeCache.relayerUserTrades[addr][key][trade.Taker][modTime]; ok2 {
				last.Count = last.Count.Add(last.Count, big.NewInt(1))
				last.Volume = big.NewInt(0).Add(last.Volume, trade.Amount)
				last.VolumeByQuote = big.NewInt(0).Add(last.VolumeByQuote, volumeByQuote)
				if trade.TakerOrderSide == sideBuy {
					last.VolumeAsk = big.NewInt(0).Add(last.VolumeAsk, trade.Amount)
					last.VolumeAskByQuote = big.NewInt(0).Add(last.VolumeAskByQuote, volumeByQuote)
				} else {
					last.VolumeBid = big.NewInt(0).Add(last.VolumeBid, trade.Amount)
					last.VolumeBidByQuote = big.NewInt(0).Add(last.VolumeBidByQuote, volumeByQuote)
				}

			} else {
				userTrade := &types.UserTrade{
					UserAddress:    trade.Taker,
					Count:          big.NewInt(1),
					Volume:         utils.CloneBigInt(trade.Amount),
					VolumeByQuote:  utils.CloneBigInt(volumeByQuote),
					BaseToken:      trade.BaseToken,
					QuoteToken:     trade.QuoteToken,
					TimeStamp:      modTime,
					RelayerAddress: addr,
				}
				if trade.TakerOrderSide == sideBuy {
					userTrade.VolumeBid = utils.CloneBigInt(trade.Amount)
					userTrade.VolumeBidByQuote = utils.CloneBigInt(volumeByQuote)
					userTrade.VolumeAsk = big.NewInt(0)
					userTrade.VolumeAskByQuote = big.NewInt(0)
				} else {
					userTrade.VolumeBid = big.NewInt(0)
					userTrade.VolumeBidByQuote = big.NewInt(0)
					userTrade.VolumeAsk = utils.CloneBigInt(trade.Amount)
					userTrade.VolumeAskByQuote = utils.CloneBigInt(volumeByQuote)
				}
				s.tradeCache.relayerUserTrades[addr][key][trade.Taker][modTime] = userTrade
			}

			if last, ok2 := s.tradeCache.relayerUserTrades[addr][key][trade.Maker][modTime]; ok2 {
				last.Count = last.Count.Add(last.Count, big.NewInt(1))
				last.Volume = big.NewInt(0).Add(last.Volume, trade.Amount)
				last.VolumeByQuote = big.NewInt(0).Add(last.VolumeByQuote, volumeByQuote)
				if trade.TakerOrderSide == sideSell {
					last.VolumeAsk = big.NewInt(0).Add(last.VolumeAsk, trade.Amount)
					last.VolumeAskByQuote = big.NewInt(0).Add(last.VolumeAskByQuote, volumeByQuote)
				} else {
					last.VolumeBid = big.NewInt(0).Add(last.VolumeBid, trade.Amount)
					last.VolumeBidByQuote = big.NewInt(0).Add(last.VolumeBidByQuote, volumeByQuote)
				}

			} else {
				userTrade := &types.UserTrade{
					UserAddress:    trade.Maker,
					Count:          big.NewInt(1),
					Volume:         utils.CloneBigInt(trade.Amount),
					VolumeByQuote:  utils.CloneBigInt(volumeByQuote),
					BaseToken:      trade.BaseToken,
					QuoteToken:     trade.QuoteToken,
					TimeStamp:      modTime,
					RelayerAddress: addr,
				}
				if trade.TakerOrderSide == sideSell {
					userTrade.VolumeBid = utils.CloneBigInt(trade.Amount)
					userTrade.VolumeBidByQuote = utils.CloneBigInt(volumeByQuote)
					userTrade.VolumeAsk = big.NewInt(0)
					userTrade.VolumeAskByQuote = big.NewInt(0)
				} else {
					userTrade.VolumeBid = big.NewInt(0)
					userTrade.VolumeBidByQuote = big.NewInt(0)
					userTrade.VolumeAsk = utils.CloneBigInt(trade.Amount)
					userTrade.VolumeAskByQuote = utils.CloneBigInt(volumeByQuote)
				}
				s.tradeCache.relayerUserTrades[addr][key][trade.Maker][modTime] = userTrade
			}

		}
	}
	return nil
}
func (s *TradeService) addUserTrade(userTrade *types.UserTrade) {
	key := s.getPairString(userTrade.BaseToken, userTrade.QuoteToken)
	if _, ok := s.tradeCache.userTrades[key]; !ok {
		s.tradeCache.userTrades[key] = make(map[common.Address]map[int64]*types.UserTrade)
	}
	if _, ok := s.tradeCache.userTrades[key][userTrade.UserAddress]; !ok {
		s.tradeCache.userTrades[key][userTrade.UserAddress] = make(map[int64]*types.UserTrade)
	}
	s.tradeCache.userTrades[key][userTrade.UserAddress][userTrade.TimeStamp] = userTrade
}

func (s *TradeService) addRelayerUserTrade(userTrade *types.UserTrade) {
	key := s.getPairString(userTrade.BaseToken, userTrade.QuoteToken)
	if _, ok := s.tradeCache.relayerUserTrades[userTrade.RelayerAddress]; !ok {
		s.tradeCache.relayerUserTrades[userTrade.RelayerAddress] = make(map[string]map[common.Address]map[int64]*types.UserTrade)
	}
	if _, ok := s.tradeCache.relayerUserTrades[userTrade.RelayerAddress][key]; !ok {
		s.tradeCache.relayerUserTrades[userTrade.RelayerAddress][key] = make(map[common.Address]map[int64]*types.UserTrade)
	}
	if _, ok := s.tradeCache.relayerUserTrades[userTrade.RelayerAddress][key][userTrade.UserAddress]; !ok {
		s.tradeCache.relayerUserTrades[userTrade.RelayerAddress][key][userTrade.UserAddress] = make(map[int64]*types.UserTrade)
	}
	s.tradeCache.relayerUserTrades[userTrade.RelayerAddress][key][userTrade.UserAddress][userTrade.TimeStamp] = userTrade
}

func (s *TradeService) filterRelayerUserTrade() {

}

// GetTopRelayerUserTradeVoumeByPair get top user trade volume by pair
func (s *TradeService) GetTopRelayerUserTradeVoumeByPair(relayerAddress common.Address, baseToken, quoteToken common.Address, from, to int64, top int) []*types.UserVolume {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if top == 0 {
		top = 10
	}
	var users []*types.UserVolume
	key := s.getPairString(baseToken, quoteToken)
	if tradebyRelayer, ok := s.tradeCache.relayerUserTrades[relayerAddress]; ok {
		if tradebyUserAddess, ok := tradebyRelayer[key]; ok {
			for address, tradeBytime := range tradebyUserAddess {
				volume := big.NewInt(0)
				for t, trade := range tradeBytime {
					if (from == 0 || t >= from) && (to == 0 || t <= to) {
						volume = volume.Add(volume, trade.VolumeByQuote)
					}
				}
				users = append(users, &types.UserVolume{
					UserAddress: address,
					Volume:      volume,
				})
			}
		}
	}
	sort.Slice(users, func(i, j int) bool {
		if users[i].Volume.Cmp(users[j].Volume) > 0 {
			return true
		}
		return false
	})
	if top >= len(users) {
		top = len(users)
	}
	return users[0:top]
}

// QueryTotal get total infomation
func (s *TradeService) QueryTotal(relayerAddress common.Address, baseTokens []common.Address, quoteToken common.Address, from, to int64) *types.TradeVolume {
	totalVolume := big.NewInt(0)
	traderCount := big.NewInt(0)

	for relayer, tradebyRelayer := range s.tradeCache.relayerUserTrades {
		if (relayerAddress == common.Address{} || relayer.Hex() == relayerAddress.Hex()) {
			for key, tradebyUserAddess := range tradebyRelayer {
				bToken, qToken, err := s.parsePairString(key)
				if err == nil && quoteToken.Hex() == qToken.Hex() && (utils.ContainsAddress(baseTokens, bToken) || len(baseTokens) == 0) {
					for _, tradeBytime := range tradebyUserAddess {
						for t, trade := range tradeBytime {
							if (from == 0 || t >= from) && (to == 0 || t <= to) {
								totalVolume = totalVolume.Add(totalVolume, trade.VolumeByQuote)
							}
						}
						traderCount = traderCount.Add(traderCount, big.NewInt(1))

					}
				}
			}
		}
	}
	return &types.TradeVolume{
		TotalVolume: totalVolume,
		Trader:      traderCount,
	}

}

// Query24hVolume get user 24h volume
func (s *TradeService) Query24hVolume(relayerAddress common.Address, userAddress common.Address, baseTokens []common.Address, quoteToken common.Address, top int) []*types.UserVolume {
	now := time.Now().Unix() - 24*60*60
	day, _ := utils.GetModTime(now, 1, unit)
	return s.queryVolume(relayerAddress, userAddress, baseTokens, quoteToken, day, now, top)

}

// QueryVolume get user volume total by quote token
// ensure basetokens element is un
func (s *TradeService) QueryVolume(relayerAddress common.Address, userAddress common.Address, baseTokens []common.Address, quoteToken common.Address, from, to int64, top int) []*types.UserVolume {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	logger.Info("QueryVolume: baseToken len: ", len(baseTokens))
	return s.queryVolume(relayerAddress, userAddress, baseTokens, quoteToken, from, to, top)
}

func (s *TradeService) queryVolume(relayerAddress common.Address, userAddress common.Address, baseTokens []common.Address, quoteToken common.Address, from, to int64, top int) []*types.UserVolume {
	if top == 0 {
		top = 10
	}

	var users []*types.UserVolume
	userVolumes := make(map[common.Address]*big.Int)

	for relayer, tradebyRelayer := range s.tradeCache.relayerUserTrades {
		if (relayerAddress == common.Address{} || relayer.Hex() == relayerAddress.Hex()) {
			for key, tradebyUserAddess := range tradebyRelayer {
				bToken, qToken, err := s.parsePairString(key)
				if err == nil && quoteToken.Hex() == qToken.Hex() && (utils.ContainsAddress(baseTokens, bToken) || len(baseTokens) == 0) {
					for address, tradeBytime := range tradebyUserAddess {
						volume := big.NewInt(0)
						for t, trade := range tradeBytime {
							if (from == 0 || t >= from) && (to == 0 || t <= to) {
								volume = volume.Add(volume, trade.VolumeByQuote)
							}
						}
						if v, ok := userVolumes[address]; ok {
							v = v.Add(v, volume)
						} else {
							userVolumes[address] = volume
						}

					}
				}
			}
		}
	}
	for a, v := range userVolumes {
		if !s.isBotAddress(a) {
			users = append(users, &types.UserVolume{
				UserAddress: a,
				Volume:      v,
			})
		}
	}
	sort.Slice(users, func(i, j int) bool {
		if users[i].Volume.Cmp(users[j].Volume) > 0 {
			return true
		}
		return false
	})
	var res []*types.UserVolume
	for i, u := range users {

		if (userAddress == common.Address{} || u.UserAddress.Hex() == userAddress.Hex()) {
			res = append(res, &types.UserVolume{
				UserAddress: u.UserAddress,
				Volume:      u.Volume,
				Rank:        i + 1,
			})
		}

	}

	if top >= len(res) {
		top = len(res)
	}
	return res[0:top]
}

// GetTopRelayerUserPnL get top PnL user trade
func (s *TradeService) GetTopRelayerUserPnL(relayerAddress common.Address, baseToken, quoteToken common.Address, top int) []*types.UserPnL {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if top == 0 {
		top = 10
	}
	var users []*types.UserPnL
	key := s.getPairString(baseToken, quoteToken)
	var lastPrice *big.Int
	lastPrice = big.NewInt(0)
	if tradebyRelayer, ok := s.tradeCache.relayerUserTrades[relayerAddress]; ok {
		if tradebyUserAddess, ok := tradebyRelayer[key]; ok {
			for address, tradeBytime := range tradebyUserAddess {

				volumeAskByQuote := big.NewInt(0)
				volumeBidByQuote := big.NewInt(0)
				volumeAsk := big.NewInt(0)
				volumeBid := big.NewInt(0)
				pnl := big.NewInt(0)

				for _, trade := range tradeBytime {
					volumeAskByQuote = volumeAskByQuote.Add(volumeAskByQuote, trade.VolumeAskByQuote)
					volumeBidByQuote = volumeBidByQuote.Add(volumeBidByQuote, trade.VolumeBidByQuote)
					volumeAsk = volumeAsk.Add(volumeAsk, trade.VolumeAsk)
					volumeBid = volumeBid.Add(volumeBid, trade.VolumeBid)
				}
				if volumeBid.Cmp(volumeAsk) >= 0 {
					if l, ok := s.lastPairPrice[key]; ok {
						lastPrice = l
						volumeRemain := new(big.Int).Sub(volumeBid, volumeAsk)
						volumeRemainByQuote := s.getVolumeByQuote(baseToken, quoteToken, volumeRemain, lastPrice)
						pnl = new(big.Int).Add(volumeRemainByQuote, volumeAskByQuote)
						pnl = new(big.Int).Sub(pnl, volumeBidByQuote)
					}

				}

				users = append(users, &types.UserPnL{
					UserAddress:      address,
					VolumeAskByQuote: volumeAskByQuote,
					VolumeBidByQuote: volumeBidByQuote,
					VolumeAsk:        volumeAsk,
					VolumeBid:        volumeBid,
					PnL:              pnl,
					CurrentPrice:     lastPrice,
				})
			}
		}
	}
	sort.Slice(users, func(i, j int) bool {
		if users[i].PnL.Cmp(users[j].PnL) > 0 {
			return true
		}
		return false
	})
	if top >= len(users) {
		top = len(users)
	}
	return users[0:top]
}

// GetNumberUsers get total trader
func (s *TradeService) GetNumberUsers(relayerAddress common.Address) int {
	users := make(map[common.Address]bool)
	for relayer, tradebyRelayer := range s.tradeCache.relayerUserTrades {
		if (relayerAddress == common.Address{} || relayer.Hex() == relayerAddress.Hex()) {
			for _, tradebyUserAddess := range tradebyRelayer {
				for address := range tradebyUserAddess {
					users[address] = true
				}

			}
		}
	}
	return len(users)
}

// GetNumberTraderByTime get number trader bytime
func (s *TradeService) GetNumberTraderByTime(relayerAddress common.Address, dateFrom, dateTo int64, excludeBot bool) int {
	users := make(map[common.Address]bool)
	if tradenypair, ok := s.tradeCache.relayerUserTrades[relayerAddress]; ok {
		for _, tradeuserbyaddress := range tradenypair {
			for address, tradebytime := range tradeuserbyaddress {
				if excludeBot {
					if s.isBotAddress(address) {
						continue
					}
				}

				for t := range tradebytime {
					if (t >= dateFrom || dateFrom == 0) && (t <= dateTo || dateTo == 0) {
						users[address] = true
					}
				}
			}
		}
	}
	return len(users)
}
