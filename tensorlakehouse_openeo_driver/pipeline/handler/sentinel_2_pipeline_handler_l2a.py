from datetime import timedelta
from typing import Optional

from tensorlakehouse_openeo_driver.constants import (
    COPERNICUS_STAC,
    SENTINEL_2_L2A,
    logger,
)

from tensorlakehouse_openeo_driver.model.item import Item, TiledItem
from tensorlakehouse_openeo_driver.pipeline.handler.abstract_sentinel_handler import (
    DatabasePipelineHandler,
)


class Sentinel2PipelineHandlerL2A(DatabasePipelineHandler):

    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"
    PENDING = "pending"
    DONE = "done"
    FAIL = "fail"

    SAFE = ".SAFE"

    def __init__(self) -> None:
        super().__init__()
        self.collection_id = SENTINEL_2_L2A
        self._pipeline_name = "sentinel_2_pipeline"
        self._version_name = ""
        self.external_collection = "SENTINEL-2"
        self.datasource_stac_url = COPERNICUS_STAC
        self._filter_cql = {"op": "=", "args": [{"property": "productType"}, "S2MSI2A"]}
        self.tasks_table = "sentinel_2_tasks"
        self.downloads_table = "sentinel_2_downloads"

    def are_equivalent_items(self, external_item: Item, internal_item: Item) -> bool:

        delta = timedelta(seconds=2)
        if (
            isinstance(external_item, TiledItem)
            and isinstance(internal_item, TiledItem)
            and external_item.tile_id == internal_item.tile_id
            and internal_item.datetime is not None
            and external_item.datetime is not None
            and (
                internal_item.datetime - delta
                <= external_item.datetime
                <= internal_item.datetime + delta
            )
        ):
            return True
        return False

    @staticmethod
    def _get_pattern(safe_name: str) -> str:
        fields = safe_name.split("_")
        assert len(fields) >= 5, f"Error! Unexpected STAC id format: {safe_name=}"
        pattern = f"{fields[0]}_%_{fields[2]}_{fields[3]}_{fields[4]}%"
        return pattern

    def _select_status_from_downloads_table(
        self, tile: Optional[str], item_id: Optional[str], polygon: Optional[str]
    ) -> Optional[str]:
        try:
            # Connect to the PostgreSQL database
            conn = self.create_database_conn()
            rows = conn.select(
                table=self.downloads_table,
                columns=["status"],
                conditions={"tile": tile},
                like_conditions={"safe_name": f"{item_id}%"},
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
