// Package domain 定义撮合引擎的核心领域模型：委托、成交与拒绝原因。
package domain

// Side 表示委托方向。
type Side int

const (
	SideBuy Side = iota + 1
	SideSell
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
	OrderLimit OrderType = iota + 1
	OrderMarket
)

func (t OrderType) String() string {
	switch t {
	case OrderLimit:
		return "LIMIT"
	case OrderMarket:
		return "MARKET"
	default:
		return "UNKNOWN"
	}
}

// OrderStatus 表示委托生命周期状态。
type OrderStatus int

const (
	StatusNew OrderStatus = iota + 1
	StatusPartiallyFilled
	StatusFilled
	StatusCancelled
)

func (s OrderStatus) String() string {
	switch s {
	case StatusNew:
		return "NEW"
	case StatusPartiallyFilled:
		return "PARTIALLY_FILLED"
	case StatusFilled:
		return "FILLED"
	case StatusCancelled:
		return "CANCELLED"
	default:
		return "UNKNOWN"
	}
}

// RejectReason 为可区分的拒绝原因编码。
type RejectReason string

const (
	RejectInvalidQuantity RejectReason = "INVALID_QUANTITY" // 数量非法
	RejectInvalidPrice    RejectReason = "INVALID_PRICE"    // 价格越界
	RejectUnknownAccount  RejectReason = "UNKNOWN_ACCOUNT"  // 不可接受的委托（账户未登记）
	RejectUnknownSymbol   RejectReason = "UNKNOWN_SYMBOL"   // 不可接受的委托（品种未登记）
	RejectDuplicateID     RejectReason = "DUPLICATE_ORDER_ID"
	RejectNoLiquidity     RejectReason = "NO_LIQUIDITY" // 市价单无对手盘
	RejectRiskMaxPosition RejectReason = "RISK_MAX_POSITION"
	RejectRiskMaxOrders   RejectReason = "RISK_MAX_OPEN_ORDERS"
	RejectRiskMaxExposure RejectReason = "RISK_MAX_EXPOSURE"
	RejectRiskMaxNotional RejectReason = "RISK_MAX_SINGLE_ORDER_NOTIONAL"
	RejectCancelNotFound  RejectReason = "CANCEL_ORDER_NOT_FOUND"
)

// Order 是一笔委托的不可变标识与可变撮合状态。
type Order struct {
	ID        string
	AccountID string
	Symbol    string
	Side      Side
	Type      OrderType
	Price     int64 // 限价单价格（最小报价单位的整数倍）；市价单为 0
	Quantity  int64 // 初始委托数量
	FilledQty int64 // 已成交数量
	Status    OrderStatus
	Sequence  int64 // 进入撮合核心的确定性顺序
}

// Remaining 返回未成交数量。
func (o *Order) Remaining() int64 { return o.Quantity - o.FilledQty }

// Fill 是一笔撮合成交（吃单方与挂单方各持有同一 Fill 的视角）。
type Fill struct {
	Sequence     int64 // 事件序号
	TradeID      string
	TakerOrderID string
	MakerOrderID string
	AccountTaker string
	AccountMaker string
	Symbol       string
	TakerSide    Side
	Price        int64
	Quantity     int64
}
