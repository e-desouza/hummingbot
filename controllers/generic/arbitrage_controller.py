import os
from decimal import Decimal
from typing import List

import pandas as pd
from pydantic import Field, validator

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.client.ui.interface_utils import format_df_for_printout
from hummingbot.core.data_type.common import PriceType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction


class ArbitrageControllerConfig(ControllerConfigBase):
    """
    This class represents the configuration required to run the Xarby Strategy.
    """
    controller_type = "generic"
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    controller_name: str = "arbitrage"
    candles_config: List[CandlesConfig] = []
    controllers_config: List[str] = []

    source_connector_name: str = Field(
        default="xrpl",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the source connector (e.g., xrpl): ",
            prompt_on_new=True
        ))
    source_trading_pair: str = Field(
        default="XRP-USD",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the source pair (e.g., XRP-USD): ",
            prompt_on_new=True
        ))
    dest_connector_name: str = Field(
        default="kucoin",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the dest connector (e.g., Kucoin): ",
            prompt_on_new=True
        ))
    dest_trading_pair: str = Field(
        default="XRP-USDC",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the dest trading pair (e.g., XRP-USDC): ",
            prompt_on_new=True
        ))
    position_size_quote: Decimal = Field(
        default=20,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the position size in quote currency: ",
            prompt_on_new=True
        ))
    min_profitability: Decimal = Field(
        default=0.001,
        client_data=ClientFieldData(
            prompt=lambda
            e: "Enter the profitability to take profit (including PNL of positions and funding received): ",
            prompt_on_new=True
        ))
    target_max_executors: Decimal = Field(
        default=1,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the maximum number of executors to run concurrently: ",
            prompt_on_new=True
        ))
    max_trades: Decimal = Field(
        default=20,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the maximum number of successful trades to execute before stopping: ",
            prompt_on_new=True
        ))
    executor_refresh_time: Decimal = Field(
        default=20,
        client_data=ClientFieldData(
            prompt=lambda e: "Enter the time in seconds to refresh the executor: ",
            prompt_on_new=True
        ))
    connector_name: str = Field(
        default="xrpl",
        client_data=ClientFieldData(
            prompt=lambda e: "dummy variable, to ensure controller variable shows up in Deploy v2",
            prompt_on_new=False
        ))
    trading_pair: str = Field(
        default="XRP-USD",
        client_data=ClientFieldData(
            prompt=lambda e: "dummy variable, to ensure controller variable shows up in Deploy v2",
            prompt_on_new=False
        ))

    @validator("position_size_quote", "min_profitability", pre=True, always=True)
    def validate_decimal_fields(cls, v):
        if isinstance(v, str):
            if v == "":
                return None
            return Decimal(v)
        return v


class ArbitrageController(ControllerBase):

    def __init__(self, config: ArbitrageControllerConfig, *args, **kwargs):
        self.config = config
        self.source_connector_name = config.source_connector_name
        self.source_trading_pair = config.source_trading_pair
        self.source_connector = ConnectorPair(connector_name=self.source_connector_name,
                                              trading_pair=self.source_trading_pair)
        self.dest_connector_name = config.dest_connector_name
        self.dest_trading_pair = config.dest_trading_pair
        self.dest_connector = ConnectorPair(connector_name=self.dest_connector_name,
                                            trading_pair=self.dest_trading_pair)
        self.position_size_quote = config.position_size_quote
        self.min_profitability = config.min_profitability
        self.running_executors = 0

        super().__init__(config, *args, **kwargs)

    async def update_processed_data(self):
        pass

    def get_market_price(self, pair: ConnectorPair) -> Decimal:
        reference_price = self.market_data_provider.get_price_by_type(pair.connector_name,
                                                                      pair.trading_pair, PriceType.MidPrice)
        return reference_price

    def get_executor_config(self) -> ArbitrageExecutorConfig | None:
        price = self.get_market_price(self.source_connector)
        quote_asset_for_buying_exchange = self.market_data_provider.connectors[
            self.source_connector_name].get_available_balance(
            self.source_trading_pair.split("-")[1])

        if self.position_size_quote * price > quote_asset_for_buying_exchange:
            self.logger().error(f"Insufficient balance for {self.source_trading_pair.split('-')[1]} in exchange {self.source_connector_name} "
                                f"to buy {self.source_trading_pair.split('-')[0]} "
                                f"Actual: {quote_asset_for_buying_exchange} --> Needed: {self.position_size_quote * price}")
            return None

        try:
            return ArbitrageExecutorConfig(
                controller_id=self.config.id,
                timestamp=self.market_data_provider.time(),
                buying_market=ConnectorPair(connector_name=self.source_connector_name,
                                            trading_pair=self.source_trading_pair),
                selling_market=ConnectorPair(connector_name=self.dest_connector_name,
                                             trading_pair=self.dest_trading_pair),
                order_amount=self.position_size_quote,
                min_profitability=self.min_profitability,
            )
        except Exception as e:
            self.logger().error(f"Error creating arbitrage executor config + {e}")
            return None

    def running_executors_count(self) -> int:
        running_executors = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda
            x: not x.is_active)
        return len(running_executors)

    def create_actions_proposal(self) -> List[ExecutorAction]:
        executor_actions = []

        if self.running_executors_count() < self.config.target_max_executors:
            config = self.get_executor_config()
            if config is not None:
                executor_actions.append(
                    CreateExecutorAction(executor_config=self.get_executor_config(), controller_id=self.config.id))

        return executor_actions

    def determine_executor_actions(self) -> List[ExecutorAction]:
        """
        Determine actions based on the provided executor handler report.
        """
        actions = []
        actions.extend(self.create_actions_proposal())
        actions.extend(self.stop_actions_proposal())
        return actions

    def stop_actions_proposal(self) -> List[ExecutorAction]:
        """
        Create a list of actions to stop the executors based on order refresh and early stop conditions.
        """
        stop_actions = []
        stop_actions.extend(self.executors_to_refresh())
        return stop_actions

    def executors_to_refresh(self) -> List[ExecutorAction]:
        executors_to_refresh = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda
            x: not x.is_trading and x.is_active and self.market_data_provider.time() - x.timestamp > self.config.executor_refresh_time)

        return [StopExecutorAction(
            controller_id=self.config.id,
            executor_id=executor.id) for executor in executors_to_refresh]

    def to_format_status(self) -> List[str]:
        all_executors_custom_info = pd.DataFrame(e.custom_info for e in self.executors_info)
        return [format_df_for_printout(all_executors_custom_info, table_format="psql", max_col_width=20)]
