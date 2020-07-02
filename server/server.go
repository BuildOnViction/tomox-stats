package server

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/tomochain/tomox-stats/endpoints"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/tomochain/tomox-stats/app"
	"github.com/tomochain/tomox-stats/crons"
	"github.com/tomochain/tomox-stats/daos"
	"github.com/tomochain/tomox-stats/errors"
	"github.com/tomochain/tomox-stats/relayer"
	"github.com/tomochain/tomox-stats/services"
	"github.com/tomochain/tomox-stats/utils"
)

const (
	swaggerUIDir = "/swaggerui/"
)

var logger = utils.Logger

// Start start server
func Start() {
	env := os.Getenv("GO_ENV")

	if err := app.LoadConfig("./config", env); err != nil {
		panic(err)
	}

	utils.InitLogger(app.Config.LogLevel)

	if err := errors.LoadMessages(app.Config.ErrorFile); err != nil {
		panic(err)
	}

	logger.Infof("Server port: %v", app.Config.ServerPort)
	logger.Infof("Tomochain node HTTP url: %v", app.Config.Tomochain["http_url"])
	logger.Infof("Tomochain node WS url: %v", app.Config.Tomochain["ws_url"])
	logger.Infof("MongoDB url: %v", app.Config.MongoURL)
	logger.Infof("RabbitMQ url: %v", app.Config.RabbitMQURL)
	logger.Infof("Exchange contract address: %v", app.Config.Tomochain["exchange_address"])
	logger.Infof("Env: %v", app.Config.Env)

	_, err := daos.InitSession(nil)
	if err != nil {
		panic(err)
	}

	router := NewRouter()

	// start the server
	address := fmt.Sprintf(":%v", app.Config.ServerPort)
	log.Printf("server %v is started at %v\n", app.Version, address)

	allowedHeaders := handlers.AllowedHeaders([]string{"Content-Type", "Accept", "Authorization", "Access-Control-Allow-Origin"})
	allowedOrigins := handlers.AllowedOrigins([]string{"*"})
	allowedMethods := handlers.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS"})

	panic(http.ListenAndServe(address, handlers.CORS(allowedHeaders, allowedOrigins, allowedMethods)(router)))
}

// NewRouter create route hander
func NewRouter() *mux.Router {

	r := mux.NewRouter()

	// get daos for dependency injection
	tokenDao := daos.NewTokenDao()
	pairDao := daos.NewPairDao()
	tradeDao := daos.NewTradeDao()
	relayerDao := daos.NewRelayerDao()
	tradeService := services.NewTradeService(tokenDao, tradeDao)
	tradeService.Init()

	exchangeAddress := common.HexToAddress(app.Config.Tomochain["exchange_address"])
	contractAddress := common.HexToAddress(app.Config.Tomochain["exchange_contract_address"])
	lendingContractAddress := common.HexToAddress(app.Config.Tomochain["lending_contract_address"])
	relayerEngine := relayer.NewRelayer(app.Config.Tomochain["http_url"], exchangeAddress, contractAddress, lendingContractAddress)
	relayerService := services.NewRelayerService(relayerEngine, tokenDao, pairDao, relayerDao)
	endpoints.ServeTradeResource(r, tradeService)
	// deploy http and ws endpoints

	cronService := crons.NewCronService(relayerService)
	// initialize MongoDB Change Streams
	go tradeService.WatchChanges()

	cronService.InitCrons()
	return r
}
