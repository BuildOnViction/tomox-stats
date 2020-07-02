package types

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/tomochain/tomox-stats/utils/math"

	"github.com/globalsign/mgo/bson"
	validation "github.com/go-ozzo/ozzo-validation"
)

// Pair struct is used to model the pair data in the system and DB
type Pair struct {
	ID                 bson.ObjectId  `json:"-" bson:"_id"`
	BaseTokenSymbol    string         `json:"baseTokenSymbol,omitempty" bson:"baseTokenSymbol"`
	BaseTokenAddress   common.Address `json:"baseTokenAddress,omitempty" bson:"baseTokenAddress"`
	BaseTokenDecimals  int            `json:"baseTokenDecimals,omitempty" bson:"baseTokenDecimals"`
	QuoteTokenSymbol   string         `json:"quoteTokenSymbol,omitempty" bson:"quoteTokenSymbol"`
	QuoteTokenAddress  common.Address `json:"quoteTokenAddress,omitempty" bson:"quoteTokenAddress"`
	QuoteTokenDecimals int            `json:"quoteTokenDecimals,omitempty" bson:"quoteTokenDecimals"`
	Listed             bool           `json:"listed,omitempty" bson:"listed"`
	Active             bool           `json:"active,omitempty" bson:"active"`
	Rank               int            `json:"rank,omitempty" bson:"rank"`
	MakeFee            *big.Int       `json:"makeFee,omitempty" bson:"makeFee"`
	TakeFee            *big.Int       `json:"takeFee,omitempty" bson:"takeFee"`
	RelayerAddress     common.Address `json:"relayerAddress,omitempty" bson:"relayerAddress"`
	CreatedAt          time.Time      `json:"-" bson:"createdAt"`
	UpdatedAt          time.Time      `json:"-" bson:"updatedAt"`
}

func (p *Pair) UnmarshalJSON(b []byte) error {
	pair := map[string]interface{}{}

	err := json.Unmarshal(b, &pair)
	if err != nil {
		return err
	}

	if pair["baseTokenAddress"] != nil {
		p.BaseTokenAddress = common.HexToAddress(pair["baseTokenAddress"].(string))
	}

	if pair["quoteTokenAddress"] != nil {
		p.QuoteTokenAddress = common.HexToAddress(pair["quoteTokenAddress"].(string))
	}

	if pair["relayerAddress"] != nil {
		p.RelayerAddress = common.HexToAddress(pair["relayerAddress"].(string))
	}

	if pair["baseTokenSymbol"] != nil {
		p.BaseTokenSymbol = pair["baseTokenSymbol"].(string)
	}

	if pair["quoteTokenSymbol"] != nil {
		p.QuoteTokenSymbol = pair["quoteTokenSymbol"].(string)
	}

	if pair["baseTokenDecimals"] != nil {
		p.BaseTokenDecimals = pair["baseTokenDecimals"].(int)
	}

	if pair["quoteTokenDecimals"] != nil {
		p.QuoteTokenDecimals = pair["quoteTokenDecimals"].(int)
	}

	if pair["rank"] != nil {
		p.Rank = pair["rank"].(int)
	}

	return nil
	//TODO do we need the rest of the fields ?
}

func (p *Pair) MarshalJSON() ([]byte, error) {
	pair := map[string]interface{}{
		"baseTokenSymbol":    p.BaseTokenSymbol,
		"baseTokenDecimals":  p.BaseTokenDecimals,
		"quoteTokenSymbol":   p.QuoteTokenSymbol,
		"quoteTokenDecimals": p.QuoteTokenDecimals,
		"baseTokenAddress":   p.BaseTokenAddress,
		"quoteTokenAddress":  p.QuoteTokenAddress,
		"relayerAddress":     p.RelayerAddress,
		"rank":               p.Rank,
		"active":             p.Active,
		"listed":             p.Listed,
	}

	if p.MakeFee != nil {
		pair["makeFee"] = p.MakeFee.String()
	}

	if p.TakeFee != nil {
		pair["takeFee"] = p.TakeFee.String()
	}

	return json.Marshal(pair)
}

func (p *Pair) SetBSON(raw bson.Raw) error {
	decoded := &PairRecord{}

	err := raw.Unmarshal(decoded)
	if err != nil {
		return err
	}

	makeFee := big.NewInt(0)
	makeFee, _ = makeFee.SetString(decoded.MakeFee, 10)

	takeFee := big.NewInt(0)
	takeFee, _ = takeFee.SetString(decoded.TakeFee, 10)

	p.ID = decoded.ID
	p.BaseTokenSymbol = decoded.BaseTokenSymbol
	p.BaseTokenAddress = common.HexToAddress(decoded.BaseTokenAddress)
	p.BaseTokenDecimals = decoded.BaseTokenDecimals
	p.QuoteTokenSymbol = decoded.QuoteTokenSymbol
	p.QuoteTokenAddress = common.HexToAddress(decoded.QuoteTokenAddress)
	p.QuoteTokenDecimals = decoded.QuoteTokenDecimals
	p.RelayerAddress = common.HexToAddress(decoded.RelayerAddress)
	p.Listed = decoded.Listed
	p.Active = decoded.Active
	p.Rank = decoded.Rank
	p.MakeFee = makeFee
	p.TakeFee = takeFee

	p.CreatedAt = decoded.CreatedAt
	p.UpdatedAt = decoded.UpdatedAt
	return nil
}

func (p *Pair) GetBSON() (interface{}, error) {
	return &PairRecord{
		ID:                 p.ID,
		BaseTokenSymbol:    p.BaseTokenSymbol,
		BaseTokenAddress:   p.BaseTokenAddress.Hex(),
		BaseTokenDecimals:  p.BaseTokenDecimals,
		QuoteTokenSymbol:   p.QuoteTokenSymbol,
		QuoteTokenAddress:  p.QuoteTokenAddress.Hex(),
		QuoteTokenDecimals: p.QuoteTokenDecimals,
		RelayerAddress:     p.RelayerAddress.Hex(),
		Active:             p.Active,
		Listed:             p.Listed,
		Rank:               p.Rank,
		MakeFee:            p.MakeFee.String(),
		TakeFee:            p.TakeFee.String(),
		CreatedAt:          p.CreatedAt,
		UpdatedAt:          p.UpdatedAt,
	}, nil
}

func (p *Pair) BaseTokenMultiplier() *big.Int {
	return math.Exp(big.NewInt(10), big.NewInt(int64(p.BaseTokenDecimals)))
}

func (p *Pair) QuoteTokenMultiplier() *big.Int {
	return math.Exp(big.NewInt(10), big.NewInt(int64(p.QuoteTokenDecimals)))
}

func (p *Pair) PairMultiplier() *big.Int {
	defaultMultiplier := math.Exp(big.NewInt(10), big.NewInt(18))
	baseTokenMultiplier := math.Exp(big.NewInt(10), big.NewInt(int64(p.BaseTokenDecimals)))

	return math.Mul(defaultMultiplier, baseTokenMultiplier)
}

func (p *Pair) PricepointMultiplier() *big.Int {
	baseTokenMultiplier := math.Exp(big.NewInt(10), big.NewInt(int64(p.BaseTokenDecimals)))
	quoteTokenMultiplier := math.Exp(big.NewInt(10), big.NewInt(int64(p.QuoteTokenDecimals)))
	defaultMultiplier := math.Exp(big.NewInt(10), big.NewInt(9))

	return math.Div(math.Mul(baseTokenMultiplier, defaultMultiplier), quoteTokenMultiplier)
}

func (p *Pair) DecimalsMultiplier() *big.Int {
	decimalsDiff := math.Sub(big.NewInt(int64(p.BaseTokenDecimals)), big.NewInt(int64(p.QuoteTokenDecimals)))
	return math.Exp(big.NewInt(10), decimalsDiff)
}

func (p *Pair) Code() string {
	code := p.BaseTokenAddress.Hex() + "::" + p.QuoteTokenAddress.Hex()
	return code
}

func (p *Pair) AddressCode() string {
	code := p.BaseTokenAddress.Hex() + "::" + p.QuoteTokenAddress.Hex()
	return code
}

func (p *Pair) Name() string {
	name := p.BaseTokenSymbol + "/" + p.QuoteTokenSymbol
	return name
}

func (p *Pair) EncodedTopic() string {
	b := []byte(p.AddressCode())
	s := hex.EncodeToString(b)

	return fmt.Sprintf("0x%s", s)
}

func (p *Pair) ParseAmount(a *big.Int) float64 {
	nominator := a
	denominator := p.BaseTokenMultiplier()
	amount := math.DivideToFloat(nominator, denominator)

	return amount
}

func (p *Pair) ParsePricePoint(pp *big.Int) float64 {
	nominator := pp
	denominator := math.Mul(math.Exp(big.NewInt(10), big.NewInt(18)), p.QuoteTokenMultiplier())
	price := math.DivideToFloat(nominator, denominator)

	return price
}

func (p *Pair) MinQuoteAmount() *big.Int {
	return math.Add(math.Mul(big.NewInt(2), p.MakeFee), math.Mul(big.NewInt(2), p.TakeFee))
}

func (p Pair) ValidateAddresses() error {
	return validation.ValidateStruct(&p,
		validation.Field(&p.BaseTokenAddress, validation.Required),
		validation.Field(&p.QuoteTokenAddress, validation.Required),
	)
}

// Validate function is used to verify if an instance of
// struct satisfies all the conditions for a valid instance
func (p Pair) Validate() error {
	return validation.ValidateStruct(&p,
		validation.Field(&p.BaseTokenAddress, validation.Required),
		validation.Field(&p.QuoteTokenAddress, validation.Required),
		validation.Field(&p.BaseTokenSymbol, validation.Required),
		validation.Field(&p.QuoteTokenSymbol, validation.Required),
	)
}

// GetOrderBookKeys returns the orderbook price point keys for corresponding pair
// It is used to fetch the orderbook of a pair
func (p *Pair) GetOrderBookKeys() (sell, buy string) {
	return p.GetKVPrefix() + "::SELL", p.GetKVPrefix() + "::BUY"
}

func (p *Pair) GetKVPrefix() string {
	return p.BaseTokenAddress.Hex() + "::" + p.QuoteTokenAddress.Hex()
}

type PairAddresses struct {
	Name       string         `json:"name" bson:"name"`
	BaseToken  common.Address `json:"baseToken" bson:"baseToken"`
	QuoteToken common.Address `json:"quoteToken" bson:"quoteToken"`
}

type PairAddressesRecord struct {
	Name       string `json:"name" bson:"name"`
	BaseToken  string `json:"baseToken" bson:"baseToken"`
	QuoteToken string `json:"quoteToken" bson:"quoteToken"`
}

type PairRecord struct {
	ID bson.ObjectId `json:"id" bson:"_id"`

	BaseTokenSymbol    string    `json:"baseTokenSymbol" bson:"baseTokenSymbol"`
	BaseTokenAddress   string    `json:"baseTokenAddress" bson:"baseTokenAddress"`
	BaseTokenDecimals  int       `json:"baseTokenDecimals" bson:"baseTokenDecimals"`
	QuoteTokenSymbol   string    `json:"quoteTokenSymbol" bson:"quoteTokenSymbol"`
	QuoteTokenAddress  string    `json:"quoteTokenAddress" bson:"quoteTokenAddress"`
	QuoteTokenDecimals int       `json:"quoteTokenDecimals" bson:"quoteTokenDecimals"`
	RelayerAddress     string    `json:"relayerAddress" bson:"relayerAddress"`
	Active             bool      `json:"active" bson:"active"`
	Listed             bool      `json:"listed" bson:"listed"`
	MakeFee            string    `json:"makeFee" bson:"makeFee"`
	TakeFee            string    `json:"takeFee" bson:"takeFee"`
	Rank               int       `json:"rank" bson:"rank"`
	CreatedAt          time.Time `json:"createdAt" bson:"createdAt"`
	UpdatedAt          time.Time `json:"updatedAt" bson:"updatedAt"`
}
