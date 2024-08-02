import os
import time
from typing import Dict, List, Optional

from pydantic import Field

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.clock import Clock
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.remote_iface.mqtt import ETopicPublisher
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, StopExecutorAction


class GenericV2StrategyWithCashOutConfig(StrategyV2ConfigBase):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    candles_config: List[CandlesConfig] = []
    # markets: Dict[str, Set[str]] = {}
    time_to_cash_out: Optional[int] = None

    # source_connector_name: str = Field(
    #     default="binance_paper_trade",
    #     client_data=ClientFieldData(
    #         prompt=lambda e: "Enter the source connector (e.g., binance): ",
    #         prompt_on_new=True
    #     ))
    # source_trading_pair: str = Field(
    #     default="XRP-USDT",
    #     client_data=ClientFieldData(
    #         prompt=lambda e: "Enter the source pair (e.g., XRP-USDT): ",
    #         prompt_on_new=True
    #     ))
    # dest_connector_name: str = Field(
    #     default="kucoin_paper_trade",
    #     client_data=ClientFieldData(
    #         prompt=lambda e: "Enter the dest connector (e.g., Kucoin): ",
    #         prompt_on_new=True
    #     ))
    # dest_trading_pair: str = Field(
    #     default="XRP-USDT",
    #     client_data=ClientFieldData(
    #         prompt=lambda e: "Enter the dest trading pair (e.g., XRP-USDT): ",
    #         prompt_on_new=True
    #     ))
    # position_size_quote: Decimal = Field(
    #     default=1000,
    #     client_data=ClientFieldData(
    #         prompt=lambda e: "Enter the position size in quote currency: ",
    #         prompt_on_new=True
    #     ))
    # min_profitability: Decimal = Field(
    #     default=0.0001,
    #     client_data=ClientFieldData(
    #         prompt=lambda
    #             e: "Enter the profitability to take profit (including PNL of positions and funding received) ",
    #         prompt_on_new=True
    #     ))

    # def update_markets(self, markets: Dict[str, Set[str]]) -> Dict[str, Set[str]]:
    #     if self.source_connector_name not in markets:
    #         markets[self.source_connector_name] = set()
    #     markets[self.source_connector_name].add(self.source_trading_pair)
    #     if self.dest_connector_name not in markets:
    #         markets[self.dest_connector_name] = set()
    #     markets[self.dest_connector_name].add(self.dest_trading_pair)
    #     return markets


class GenericV2StrategyWithCashOut(StrategyV2Base):
    """
    This script runs a generic strategy with cash out feature. Will also check if the controllers configs have been
    updated and apply the new settings.
    The cash out of the script can be set by the time_to_cash_out parameter in the config file. If set, the script will
    stop the controllers after the specified time has passed, and wait until the active executors finalize their
    execution.
    The controllers will also have a parameter to manually cash out. In that scenario, the main strategy will stop the
    specific controller and wait until the active executors finalize their execution. The rest of the executors will
    wait until the main strategy stops them.
    """

    def __init__(self, connectors: Dict[str, ConnectorBase], config: GenericV2StrategyWithCashOutConfig):
        super().__init__(connectors, config)
        self._last_timestamp = None
        self.config = config
        self.cashing_out = False
        self.closed_executors_buffer: int = 30
        self.performance_report_interval: int = 1
        self._last_performance_report_timestamp = 0
        hb_app = HummingbotApplication.main_application()
        self.mqtt_enabled = hb_app._mqtt is not None
        self._pub: Optional[ETopicPublisher] = None
        if self.config.time_to_cash_out:
            self.cash_out_time = self.config.time_to_cash_out + time.time()
        else:
            self.cash_out_time = None

        # self.exchange_pair_1 = ConnectorPair(connector_name=config.source_connector_name,
        #                                      trading_pair=config.source_trading_pair)
        # self.exchange_pair_2 = ConnectorPair(connector_name=config.dest_connector_name,
        #                                      trading_pair=config.dest_trading_pair)
        # self.order_amount = config.position_size_quote
        # self.min_profitability = config.min_profitability
        # self.markets = {self.exchange_pair_1.connector_name: {self.exchange_pair_1.trading_pair},
        #                 self.exchange_pair_2.connector_name: {self.exchange_pair_2.trading_pair}}

    def start(self, clock: Clock, timestamp: float) -> None:
        """
        Start the strategy.
        :param clock: Clock to use.
        :param timestamp: Current time.
        """
        self._last_timestamp = timestamp
        self.apply_initial_setting()
        if self.mqtt_enabled:
            self._pub = ETopicPublisher("performance", use_bot_prefix=True)

    def on_stop(self):
        if self.mqtt_enabled:
            self._pub({controller_id: {} for controller_id in self.controllers.keys()})
            self._pub = None

    def on_tick(self):
        super().on_tick()
        self.control_cash_out()
        self.send_performance_report()

    def send_performance_report(self):
        if self.current_timestamp - self._last_performance_report_timestamp >= self.performance_report_interval and self.mqtt_enabled:
            performance_reports = {controller_id: self.executor_orchestrator.generate_performance_report(
                controller_id=controller_id).dict() for controller_id in self.controllers.keys()}
            self._pub(performance_reports)
            self._last_performance_report_timestamp = self.current_timestamp

    def control_cash_out(self):
        self.evaluate_cash_out_time()
        if self.cashing_out:
            self.check_executors_status()
        else:
            self.check_manual_cash_out()

    def evaluate_cash_out_time(self):
        if self.cash_out_time and self.current_timestamp >= self.cash_out_time and not self.cashing_out:
            self.logger().info("Cash out time reached. Stopping the controllers.")
            for controller_id, controller in self.controllers.items():
                if controller.status == RunnableStatus.RUNNING:
                    self.logger().info(f"Cash out for controller {controller_id}.")
                    controller.stop()
            self.cashing_out = True

    def check_manual_cash_out(self):
        for controller_id, controller in self.controllers.items():
            if controller.config.manual_kill_switch and controller.status == RunnableStatus.RUNNING:
                self.logger().info(f"Manual cash out for controller {controller_id}.")
                controller.stop()
                executors_to_stop = self.get_executors_by_controller(controller_id)
                self.executor_orchestrator.execute_actions(
                    [StopExecutorAction(executor_id=executor.id,
                                        controller_id=executor.controller_id) for executor in executors_to_stop])
            if not controller.config.manual_kill_switch and controller.status == RunnableStatus.TERMINATED:
                self.logger().info(f"Restarting controller {controller_id}.")
                controller.start()

    def check_executors_status(self):
        active_executors = self.filter_executors(
            executors=self.get_all_executors(),
            filter_func=lambda executor: executor.status == RunnableStatus.RUNNING
        )
        if not active_executors:
            self.logger().info("All executors have finalized their execution. Stopping the strategy.")
            HummingbotApplication.main_application().stop()
        else:
            non_trading_executors = self.filter_executors(
                executors=active_executors,
                filter_func=lambda executor: not executor.is_trading
            )
            self.executor_orchestrator.execute_actions(
                [StopExecutorAction(executor_id=executor.id,
                                    controller_id=executor.controller_id) for executor in non_trading_executors])

    def create_actions_proposal(self) -> List[CreateExecutorAction]:
        return []

    def stop_actions_proposal(self) -> List[StopExecutorAction]:
        return []

    def apply_initial_setting(self):
        connectors_position_mode = {}
        for controller_id, controller in self.controllers.items():
            config_dict = controller.config.dict()
            if "connector_name" in config_dict:
                if self.is_perpetual(config_dict["connector_name"]):
                    if "position_mode" in config_dict:
                        connectors_position_mode[config_dict["connector_name"]] = config_dict["position_mode"]
                    if "leverage" in config_dict:
                        self.set_leverage(config_dict["connector_name"], leverage=config_dict["leverage"],
                                          trading_pair=config_dict[
                                              "trading_pair"])
        for connector_name, position_mode in connectors_position_mode.items():
            self.set_position_mode(connector_name, position_mode)
