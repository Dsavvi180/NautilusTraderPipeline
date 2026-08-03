import sys
from pathlib import Path

from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.config import (
    BacktestDataConfig,
    BacktestEngineConfig,
    BacktestRunConfig,
    BacktestVenueConfig,
    ImportableActorConfig,
    LoggingConfig,
)
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId


class BacktestRunner:
    """Reusable Nautilus backtest runner.

    Pass in the actor(s) you are working on and call ``run()``. Everything
    else (catalog, instrument, data class, time range, venue, chunking) is
    configurable via constructor arguments with sensible defaults.
    """

    def __init__(
        self,
        actors: list[ImportableActorConfig],
        catalog_path: str | None = None,
        instrument_id: str = "ETHUSDT-LINEAR.BYBIT",
        data_cls: type = TradeTick,
        start: str | None = None,
        end: str | None = None,
        chunk_size: int = 1_000_000,
        starting_balances: list[str] | None = None,
        oms_type: str = "NETTING",
        account_type: str = "MARGIN",
        venue_name: str = "BYBIT",
        log_level: str = "INFO",
        raise_exception: bool = True,
    ) -> None:
        # Ensure actor modules in the working dir are importable by the node.
        sys.path.insert(0, str(Path.cwd()))

        self.actors = actors
        self.catalog_path = catalog_path or str(
            (Path.cwd().parent / "nautilusDataCatalog").resolve()
        )
        self.instrument_id = instrument_id
        self.data_cls = data_cls
        self.start = start
        self.end = end
        self.chunk_size = chunk_size
        self.starting_balances = starting_balances or ["1000000 USDT"]
        self.oms_type = oms_type
        self.account_type = account_type
        self.venue_name = venue_name
        self.log_level = log_level
        self.raise_exception = raise_exception

    def _build_node(self) -> BacktestNode:
        """Assemble venue, data, engine and run configs into a BacktestNode."""
        venue = BacktestVenueConfig(
            name=self.venue_name,
            oms_type=self.oms_type,
            account_type=self.account_type,
            starting_balances=self.starting_balances,
        )

        data = BacktestDataConfig(
            catalog_path=self.catalog_path,
            data_cls=self.data_cls,
            instrument_id=InstrumentId.from_str(self.instrument_id),
            start_time=self.start,
            end_time=self.end,
        )

        engine = BacktestEngineConfig(
            logging=LoggingConfig(log_level=self.log_level),
            actors=self.actors,
        )

        run_config = BacktestRunConfig(
            venues=[venue],
            data=[data],
            engine=engine,
            chunk_size=self.chunk_size,
            raise_exception=self.raise_exception,
        )

        return BacktestNode(configs=[run_config])

    def run(self):
        """Build the node and run the backtest. Equivalent to ``node.run()``."""
        return self._build_node().run()