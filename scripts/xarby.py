import os
import traceback
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Set

from pydantic import Field

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.exchange_base import PriceType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.executors.arbitrage_executor.arbitrage_executor import ArbitrageExecutor
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.data_types import ConnectorPair


class XarbyConfig(StrategyV2ConfigBase):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    controller_name: str = "xarby_strategy"
    candles_config: List[CandlesConfig] = []
    controllers_config: List[str] = []

    source_connector_name: str = Field(
        default="binance_paper_trade",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the source connector (e.g., binance): ",
            prompt_on_new=True
        ))
    source_trading_pair: str = Field(
        default="XRP-USDT",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the source pair (e.g., XRP-USDT): ",
            prompt_on_new=True
        ))
    dest_connector_name: str = Field(
        default="kucoin_paper_trade",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the dest connector (e.g., Kucoin): ",
            prompt_on_new=True
        ))
    dest_trading_pair: str = Field(
        default="XRP-USDT",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the dest trading pair (e.g., XRP-USDT): ",
            prompt_on_new=True
        ))
    position_size_quote: Decimal = Field(
        default=1000,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the position size in quote currency: ",
            prompt_on_new=True
        ))
    min_profitability: Decimal = Field(
        default=0.0001,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the profitability to take profit (including PNL of positions and funding received) ",
            prompt_on_new=True
        ))

    def update_markets(self, markets: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
        if self.source_connector_name not in markets:
            markets[self.source_connector_name] = set()
        markets[self.source_connector_name].add(self.source_trading_pair)
        if self.dest_connector_name not in markets:
            markets[self.dest_connector_name] = set()
        markets[self.dest_connector_name].add(self.dest_trading_pair)
        return markets


class Xarby(StrategyV2Base):

    def __init__(self, connectors: Dict[str, ConnectorBase], config: XarbyConfig):
        super().__init__(connectors, config)
        self.config = config
        self.active_buy_arbitrages = []
        self.active_sell_arbitrages = []
        self.closed_arbitrage_executors = []
        self.min_profitability = config.min_profitability
        self.exchange_pair_1 = ConnectorPair(connector_name=config.source_connector_name,
                                             trading_pair=config.source_trading_pair)
        self.exchange_pair_2 = ConnectorPair(connector_name=config.dest_connector_name,
                                             trading_pair=config.dest_trading_pair)
        self.order_amount = config.position_size_quote
        self.min_profitability = config.min_profitability
        self.markets = {self.exchange_pair_1.connector_name: {self.exchange_pair_1.trading_pair},
                        self.exchange_pair_2.connector_name: {self.exchange_pair_2.trading_pair}}
        self.active_buy_arbitrages = []
        self.active_sell_arbitrages = []
        self.closed_arbitrage_executors = []

    def on_tick(self):
        self.cleanup_arbitrages()

        if len(self.active_buy_arbitrages) < 1:
            buy_arbitrage_executor = self.create_arbitrage_executor(
                buying_exchange_pair=self.exchange_pair_1,
                selling_exchange_pair=self.exchange_pair_2,
            )
            if buy_arbitrage_executor:
                self.active_buy_arbitrages.append(buy_arbitrage_executor)
        # if len(self.active_sell_arbitrages) < 1:
        #     sell_arbitrage_executor = self.create_arbitrage_executor(
        #         buying_exchange_pair=self.exchange_pair_2,
        #         selling_exchange_pair=self.exchange_pair_1,
        #     )
        #     if sell_arbitrage_executor:
        #         self.active_sell_arbitrages.append(sell_arbitrage_executor)

    def on_stop(self):
        for arbitrage in self.active_buy_arbitrages:
            arbitrage.stop()
        # for arbitrage in self.active_sell_arbitrages:
        #     arbitrage.stop()

    def get_market_price(self, pair: ConnectorPair) -> Decimal:
        reference_price = self.market_data_provider.get_price_by_type(pair.connector_name,
                                                                      pair.trading_pair, PriceType.MidPrice)
        return reference_price

    def create_arbitrage_executor(self, buying_exchange_pair: ConnectorPair, selling_exchange_pair: ConnectorPair):
        try:
            base_asset_for_selling_exchange = self.connectors[
                selling_exchange_pair.connector_name].get_available_balance(
                selling_exchange_pair.trading_pair.split("-")[0])
            if self.order_amount > base_asset_for_selling_exchange:
                self.logger().info(f"Insufficient balance in exchange {selling_exchange_pair.connector_name} "
                                   f"to sell {selling_exchange_pair.trading_pair.split('-')[0]} "
                                   f"Actual: {base_asset_for_selling_exchange} --> Needed: {self.order_amount}")
                return

            # Hardcoded for now since we don't have a price oracle for WMATIC (CoinMarketCap rate source is requested and coming)
            price = self.get_market_price(buying_exchange_pair)
            quote_asset_for_buying_exchange = self.connectors[
                buying_exchange_pair.connector_name].get_available_balance(
                buying_exchange_pair.trading_pair.split("-")[1])
            if self.order_amount * price > quote_asset_for_buying_exchange:
                self.logger().info(f"Insufficient balance in exchange {buying_exchange_pair.connector_name} "
                                   f"to buy {buying_exchange_pair.trading_pair.split('-')[1]} "
                                   f"Actual: {quote_asset_for_buying_exchange} --> Needed: {self.order_amount * price}")
                return

            try:
                arbitrage_config = ArbitrageExecutorConfig(
                    buying_market=buying_exchange_pair,
                    selling_market=selling_exchange_pair,
                    order_amount=self.order_amount,
                    min_profitability=self.min_profitability,
                    timestamp=datetime.now().timestamp()
                )
            except Exception as e:
                self.logger().error(f"Error creating arbitrage executor config + {e}")
                return

            arbitrage_executor = ArbitrageExecutor(strategy=self,
                                                   config=arbitrage_config)
            arbitrage_executor.start()
            return arbitrage_executor
        except Exception as e:
            self.logger().error(
                f"Error creating executor to buy on {buying_exchange_pair.connector_name} and sell on {selling_exchange_pair.connector_name} + {e}")
            traceback.print_exc()

    def format_status(self) -> str:
        status = []
        status.extend([f"Closed Arbitrages: {len(self.closed_arbitrage_executors)}"])
        for arbitrage in self.closed_arbitrage_executors:
            status.extend(arbitrage.to_format_status())
        status.extend([f"Active Arbitrages: {len(self.active_sell_arbitrages) + len(self.active_buy_arbitrages)}"])
        for arbitrage in self.active_sell_arbitrages:
            status.extend(arbitrage.to_format_status())
        for arbitrage in self.active_buy_arbitrages:
            status.extend(arbitrage.to_format_status())
        return "\n".join(status)

    def cleanup_arbitrages(self):
        for arbitrage in self.active_buy_arbitrages:
            if arbitrage.is_closed:
                self.closed_arbitrage_executors.append(arbitrage)
                self.active_buy_arbitrages.remove(arbitrage)
        # for arbitrage in self.active_sell_arbitrages:
        #     if arbitrage.is_closed:
        #         self.closed_arbitrage_executors.append(arbitrage)
        #         self.active_sell_arbitrages.remove(arbitrage)
