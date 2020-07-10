package endpoints

import (
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gorilla/mux"
	"github.com/tomochain/tomox-stats/services"
	"github.com/tomochain/tomox-stats/utils/httputils"
)

type lendingTradeEndpoint struct {
	lendingtradeService *services.LendingTradeService
}

// ServeLendingTradeResource sets up the routing of trade endpoints and the corresponding handlers.
// TODO trim down to one single endpoint with the 3 following params: base, quote, address
func ServeLendingTradeResource(
	r *mux.Router,
	lendingtradeService *services.LendingTradeService,
) {
	e := &lendingTradeEndpoint{lendingtradeService}
	r.HandleFunc("/stats/lending/users/count", e.handleGetNumberUser)
}

func (e *lendingTradeEndpoint) handleGetNumberUser(w http.ResponseWriter, r *http.Request) {

	var relayerAddress common.Address
	v := r.URL.Query()
	rAddress := v.Get("relayerAddress")
	duration := v.Get("duration")

	type NumberTrader struct {
		ActiveUser int    `json:"activeUser"`
		Duration   string `json:"duration"`
	}
	var res NumberTrader
	if rAddress != "" {
		if !common.IsHexAddress(rAddress) {
			httputils.WriteError(w, http.StatusBadRequest, "Invalid relayer address")
			return
		}
		relayerAddress = common.HexToAddress(rAddress)
	}
	if duration == "" {
		res.Duration = duration
		res.ActiveUser = e.lendingtradeService.GetNumberTraderByTime(relayerAddress, 0, 0)
		httputils.WriteJSON(w, http.StatusOK, res)
		return
	}
	if duration == "1d" {
		res.Duration = duration
		res.ActiveUser = e.lendingtradeService.GetNumberTraderByTime(relayerAddress, time.Now().AddDate(0, 0, -1).Unix(), 0)
		httputils.WriteJSON(w, http.StatusOK, res)
		return
	}
	if duration == "7d" {
		res.Duration = duration
		res.ActiveUser = e.lendingtradeService.GetNumberTraderByTime(relayerAddress, time.Now().AddDate(0, 0, -7).Unix(), 0)
		httputils.WriteJSON(w, http.StatusOK, res)
		return
	}
	if duration == "30d" {
		res.Duration = duration
		res.ActiveUser = e.lendingtradeService.GetNumberTraderByTime(relayerAddress, time.Now().AddDate(0, 0, -30).Unix(), 0)
		httputils.WriteJSON(w, http.StatusOK, res)
		return
	}
	httputils.WriteJSON(w, http.StatusBadRequest, "duration must be empty/1d/7d/30d")
}
