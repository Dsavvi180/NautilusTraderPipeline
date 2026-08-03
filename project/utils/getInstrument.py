from decimal import Decimal
import requests
from nautilus_trader.model.identifiers import InstrumentId, Symbol
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.objects import Currency, Price, Quantity, Money

# Only supports Bybit linear crypto perpetual instruments for now
class GetInstrument:
    def __init__(self, symbol: str, type: str="LINEAR", testnet: bool = False):
        self.symbol = symbol
        self.type = type
        self.testnet = testnet

    def __get_bybit_linear_instrument_info(self, symbol: str, testnet: bool = False) -> dict:
        base_url = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"

        params = {
            "category": "linear",
            "symbol": symbol.upper(),
        }

        r = requests.get(
            f"{base_url}/v5/market/instruments-info",
            params=params,
            timeout=20,
        )
        r.raise_for_status()

        payload = r.json()

        if payload["retCode"] != 0:
            raise RuntimeError(payload)

        instruments = payload["result"]["list"]

        if not instruments:
            raise ValueError(f"No Bybit linear instrument found for {symbol}")

        return instruments[0]
    
    def __getLinearCryptoPerpetualInstrument(self) -> dict:
        info = self.__get_bybit_linear_instrument_info(self.symbol, self.testnet)

        symbol = info["symbol"]              
        base_coin = info["baseCoin"]         
        quote_coin = info["quoteCoin"]      
        settle_coin = info["settleCoin"]     

        tick_size = info["priceFilter"]["tickSize"]
        qty_step = info["lotSizeFilter"]["qtyStep"]

        price_precision = int(info["priceScale"])
        size_precision = abs(Decimal(qty_step).as_tuple().exponent)

        min_qty = info["lotSizeFilter"]["minOrderQty"]
        max_qty = info["lotSizeFilter"]["maxOrderQty"]
        min_notional = info["lotSizeFilter"]["minNotionalValue"]

        min_price = info["priceFilter"]["minPrice"]
        max_price = info["priceFilter"]["maxPrice"]

        max_leverage = Decimal(info["leverageFilter"]["maxLeverage"])
        margin_init = Decimal("1") / max_leverage

        CRYPTOPERP_INSTRUMENT = CryptoPerpetual(
            instrument_id=InstrumentId.from_str(f"{symbol}-LINEAR.BYBIT"),
            raw_symbol=Symbol(symbol),

            base_currency=Currency.from_str(base_coin),
            quote_currency=Currency.from_str(quote_coin),
            settlement_currency=Currency.from_str(settle_coin),

            is_inverse=False,

            price_precision=price_precision,
            size_precision=size_precision,

            price_increment=Price.from_str(tick_size),
            size_increment=Quantity.from_str(qty_step),

            multiplier=Quantity.from_str("1"),
            lot_size=Quantity.from_str("1"),

            min_quantity=Quantity.from_str(min_qty),
            max_quantity=Quantity.from_str(max_qty),

            min_notional=Money.from_str(f"{min_notional} {quote_coin}"),
            max_notional=None,

            min_price=Price.from_str(min_price),
            max_price=Price.from_str(max_price),

            margin_init=margin_init,
            margin_maint=Decimal("0"),

            maker_fee=Decimal("0.0002"),
            taker_fee=Decimal("0.00055"),

            ts_event=0,
            ts_init=0,

            info=info,
        )

        return CRYPTOPERP_INSTRUMENT
    
    def getInstrument(self) -> dict:
        if self.type == "LINEAR":
            return self.__getLinearCryptoPerpetualInstrument()
        else:
            raise ValueError(f"Unsupported instrument type: {self.type}")



