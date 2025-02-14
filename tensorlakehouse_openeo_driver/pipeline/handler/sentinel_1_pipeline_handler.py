from typing import Optional
from tensorlakehouse_openeo_driver.constants import (
    MS_PLANETARY_COMPUTER_STAC,
    SENTINEL_1_GRD,
    logger,
)
from tensorlakehouse_openeo_driver.model.item import Item
from tensorlakehouse_openeo_driver.pipeline.handler.abstract_sentinel_handler import (
    DatabasePipelineHandler,
)


class Sentinel1PipelineHandler(DatabasePipelineHandler):

    SLEEP_TIME = 90

    def __init__(self) -> None:
        super().__init__()
        self.collection_id = SENTINEL_1_GRD

        self._pipeline_name = "sentinel_1_pipeline"
        self._version_name = ""
        self.external_collection = "sentinel-1-grd"
        self.datasource_stac_url = MS_PLANETARY_COMPUTER_STAC
        self._filter_cql = None
        self.tasks_table = "sentinel_1_tasks"
        self.downloads_table = "sentinel_1_downloads"
        self._temporal_buffer = 15

    def are_equivalent_items(self, external_item: Item, internal_item: Item) -> bool:

        return internal_item.item_id.startswith(external_item.item_id)

    def _select_status_from_downloads_table(
        self, tile: Optional[str], item_id: Optional[str], polygon: Optional[str]
    ) -> Optional[str]:
        try:
            # Connect to the PostgreSQL database
            conn = self.create_database_conn()
            rows = conn.select(
                table=self.downloads_table,
                columns=["status"],
                conditions={"polygon": polygon},
                like_conditions={"zip_name": f"{item_id}%"},
                fetch="fetchall",
            )
            # if no row matches the conditions, return None
            if rows is None:
                return None
            status = None
            # create a list of task status
            task_status_list = [r[0] for r in rows]

            # it is impossible to identify a task uniquely by only the polygon and zip_name columns,
            # so we query all rows that have the same polygon and if there is any task that is
            # pending, then this method returns pending
            if any(t == DatabasePipelineHandler.PENDING for t in task_status_list):
                status = DatabasePipelineHandler.PENDING
            # if there is none pending, but there is a fail task, then return fail
            elif any(t == DatabasePipelineHandler.FAIL for t in task_status_list):
                status = DatabasePipelineHandler.FAIL
            else:
                status = DatabasePipelineHandler.DONE
            assert isinstance(status, str)
            logger.debug(f"{status=}")
            return status
        except Exception as error:
            logger.debug(f"Error adding task: {error}")
            raise error
