package market

import "fmt"

// Side 表示委托方向。
type Side int

const (
	SideUnknown Side = 0
	SideBuy     Side = 1
	SideSell    Side = 2
)

func (s Side) String() string {
	switch s {
	case SideBuy:
		return "BUY"
	case SideSell:
		return "SELL"
	default:
		return "UNKNOWN"
	}
}

// OrderType 表示委托类型。
type OrderType int

const (
	OrderTypeUnknown OrderType = 0
	OrderTypeLimit   OrderType = 1
	OrderTypeMarket  OrderType = 2
)

func (t OrderType) String() string {
	switch t {
	case OrderTypeLimit:
		return "LIMIT"
	case OrderTypeMarket:
		return "MARKET"
	default:
		return "UNKNOWN"
	}
}

// RejectReason 委托被拒绝的可区分原因。
type RejectReason int

const (
	RejectNone              RejectReason = 0
	RejectInvalidQuantity   RejectReason = 1 // 数量非法
	RejectInvalidPrice      RejectReason = 2 // 价格越界
	RejectUnacceptableOrder RejectReason = 3 // 不可接受的委托（未知方向/类型、市价无对手等）
	RejectRiskPositionLimit RejectReason = 4 // 持仓限额风控
	RejectRiskExposureLimit RejectReason = 5 // 敞口限额风控
	RejectUnknownAccount    RejectReason = 6 // 未知账户
	RejectDuplicateOrderID  RejectReason = 7 // 订单ID重复
	RejectOrderNotFound     RejectReason = 8 // 撤单时找不到订单
)

func (r RejectReason) String() string {
	switch r {
	case RejectInvalidQuantity:
		return "INVALID_QUANTITY"
	case RejectInvalidPrice:
		return "INVALID_PRICE"
	case RejectUnacceptableOrder:
		return "UNACCEPTABLE_ORDER"
	case RejectRiskPositionLimit:
		return "RISK_POSITION_LIMIT"
	case RejectRiskExposureLimit:
		return "RISK_EXPOSURE_LIMIT"
	case RejectUnknownAccount:
		return "UNKNOWN_ACCOUNT"
	case RejectDuplicateOrderID:
		return "DUPLICATE_ORDER_ID"
	case RejectOrderNotFound:
		return "ORDER_NOT_FOUND"
	default:
		return "NONE"
	}
}

// Order 簿内委托。
type Order struct {
	ID        string
	AccountID string
	Symbol    string
	Side      Side
	Type      OrderType
	Price     int64 // 限价单价（Tick 单位）；市价单忽略
	Qty       int64 // 剩余数量
}

func (o *Order) String() string {
	return fmt.Sprintf("Order{id=%s acct=%s %s %s qty=%d price=%d}",
		o.ID, o.AccountID, o.Side, o.Type, o.Qty, o.Price)
}

// Limits 品种的价格/数量边界。
type Limits struct {
	MinTickSize int64 // 最小价格变动单位；价格必须是其整数倍
	MinPrice    int64
	MaxPrice    int64
	MinQty      int64
	MaxQty      int64
}

// Validate 对委托做基础合法性校验，返回可区分的拒绝原因；RejectNone 表示通过。
// 仅校验方向/类型/数量/价格等静态属性，不涉及账户、风控与簿内状态。
func Validate(o *Order, lim Limits) RejectReason {
	if o.Side != SideBuy && o.Side != SideSell {
		return RejectUnacceptableOrder
	}
	switch o.Type {
	case OrderTypeLimit:
		// 价格必须为正、落在 [MinPrice, MaxPrice]，且为最小变动单位的整数倍。
		if o.Price <= 0 {
			return RejectInvalidPrice
		}
		if lim.MinPrice > 0 && o.Price < lim.MinPrice {
			return RejectInvalidPrice
		}
		if lim.MaxPrice > 0 && o.Price > lim.MaxPrice {
			return RejectInvalidPrice
		}
		if lim.MinTickSize > 0 && o.Price%lim.MinTickSize != 0 {
			return RejectInvalidPrice
		}
	case OrderTypeMarket:
		// 市价单不携带限价。
		if o.Price != 0 {
			return RejectInvalidPrice
		}
	default:
		return RejectUnacceptableOrder
	}
	if o.Qty <= 0 {
		return RejectInvalidQuantity
	}
	if lim.MinQty > 0 && o.Qty < lim.MinQty {
		return RejectInvalidQuantity
	}
	if lim.MaxQty > 0 && o.Qty > lim.MaxQty {
		return RejectInvalidQuantity
	}
	if o.ID == "" || o.AccountID == "" || o.Symbol == "" {
		return RejectUnacceptableOrder
	}
	return RejectNone
}
