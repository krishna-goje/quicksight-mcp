"""Service layer — 11 focused modules extracted from the monolithic client.py.

Each service receives ``AwsClient`` and ``TTLCache`` via constructor.
Services that modify analyses also receive ``AnalysisService`` (the
central write gateway).

Use ``create_services()`` to instantiate the full service graph.
"""

from __future__ import annotations

from quicksight_mcp.config import Settings
from quicksight_mcp.core.aws_client import AwsClient
from quicksight_mcp.core.cache import TTLCache

from quicksight_mcp.services.datasets import DatasetService
from quicksight_mcp.services.analyses import AnalysisService
from quicksight_mcp.services.dashboards import DashboardService
from quicksight_mcp.services.calculated_fields import CalculatedFieldService
from quicksight_mcp.services.sheets import SheetService
from quicksight_mcp.services.visuals import VisualService
from quicksight_mcp.services.parameters import ParameterService
from quicksight_mcp.services.filters import FilterService
from quicksight_mcp.services.backup import BackupService
from quicksight_mcp.services.chart_builders import ChartBuilderService
from quicksight_mcp.services.snapshots import SnapshotService


class ServiceContainer:
    """Holds all service instances with proper dependency wiring."""

    def __init__(self, settings: Settings | None = None):
        self.settings = settings or Settings()
        self.settings.ensure_dirs()

        self.aws = AwsClient(self.settings)
        self.cache = TTLCache(ttl=self.settings.cache_ttl_seconds)

        # Core services (no cross-service deps)
        self.datasets = DatasetService(self.aws, self.cache, self.settings)
        self.analyses = AnalysisService(self.aws, self.cache, self.settings)
        self.dashboards = DashboardService(self.aws, self.cache)

        # Services that depend on AnalysisService for writes
        self.calculated_fields = CalculatedFieldService(
            self.aws, self.cache, self.analyses
        )
        self.sheets = SheetService(self.aws, self.cache, self.analyses)
        self.visuals = VisualService(self.aws, self.cache, self.analyses)
        self.parameters = ParameterService(self.aws, self.cache, self.analyses)
        self.filters = FilterService(self.aws, self.cache, self.analyses)
        self.backup = BackupService(
            self.aws, self.cache, self.settings, self.analyses
        )
        self.chart_builders = ChartBuilderService(
            self.aws, self.cache, self.analyses
        )
        self.snapshots = SnapshotService(self.aws, self.cache, self.analyses)


def create_services(settings: Settings | None = None) -> ServiceContainer:
    """Create the full service graph with proper dependency injection."""
    return ServiceContainer(settings)


__all__ = [
    "ServiceContainer",
    "create_services",
    "DatasetService",
    "AnalysisService",
    "DashboardService",
    "CalculatedFieldService",
    "SheetService",
    "VisualService",
    "ParameterService",
    "FilterService",
    "BackupService",
    "ChartBuilderService",
    "SnapshotService",
]
