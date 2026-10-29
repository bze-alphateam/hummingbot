from decimal import Decimal

from hummingbot.core.data_type.common import PriceType

from hummingbot.strategy.asset_price_delegate cimport AssetPriceDelegate

s_decimal_NaN = Decimal("NaN")


cdef class BeezeePoolPriceDelegate(AssetPriceDelegate):
    """The pair's tradebin liquidity pool price as a strategy's price source (e.g. PMM's `asset_price_delegate`),
    in place of the order-book mid. Every price type returns the pool price: a pool has no bid/ask spread.
    NaN while the pool is unknown or empty. The connector reloads the pool's reserves on every new block."""

    cdef:
        object _market
        str _trading_pair

    def __init__(self, market, trading_pair: str):
        super().__init__()
        self._market = market
        self._trading_pair = trading_pair

    cdef object c_get_mid_price(self):
        price = self._market.get_pool_price(self._trading_pair)
        return s_decimal_NaN if price is None else price

    def get_price_by_type(self, price_type: PriceType) -> Decimal:
        return self.c_get_mid_price()

    @property
    def ready(self) -> bool:
        return self._market.ready and self._market.get_pool_price(self._trading_pair) is not None

    @property
    def market(self):
        return self._market

    @property
    def trading_pair(self) -> str:
        return self._trading_pair
