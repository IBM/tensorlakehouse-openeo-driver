from datetime import datetime, timedelta
import time
from typing import List, Optional, Tuple

import pytz
from tensorlakehouse_openeo_driver.constants import (
    SENTINEL_DB_HOST,
    SENTINEL_DB_NAME,
    SENTINEL_DB_PASSWORD,
    SENTINEL_DB_PORT,
    SENTINEL_DB_USER,
    logger,
)
from tensorlakehouse_openeo_driver.pipeline.handler.pipeline_handler import (
    PipelineHandler,
)
from tensorlakehouse_openeo_driver.model.item import Item, TiledItem
from tensorlakehouse_openeo_driver.pipeline.postgres_conn import DataBaseConn


class DatabasePipelineHandler(PipelineHandler):

    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"
    PENDING = "pending"
    DONE = "done"
    FAIL = "fail"
    STATUSES = [PENDING, DONE, FAIL]
    MAX_ITERATIONS = 100
    SLEEP_TIME = 60

    def __init__(self) -> None:
        super().__init__()
        self.collection_id = ""
        self._pipeline_name = ""
        self._version_name = ""
        self.external_collection = ""
        self.datasource_stac_url = ""
        self._filter_cql = None
        self.tasks_table = ""
        self.downloads_table = ""
        self.dbname: Optional[str] = SENTINEL_DB_NAME
        self.host: Optional[str] = SENTINEL_DB_HOST
        assert isinstance(
            SENTINEL_DB_PORT, int
        ), f"Error! SENTINEL2_DB_PORT is not an int: {SENTINEL_DB_PORT=}"
        self.port: Optional[int] = SENTINEL_DB_PORT
        self.user: Optional[str] = SENTINEL_DB_USER
        self.password: Optional[str] = SENTINEL_DB_PASSWORD

    def create_database_conn(self) -> DataBaseConn:
        # Connect to the PostgreSQL database
        assert self.host is not None
        assert self.port is not None
        assert self.dbname is not None
        assert self.user is not None
        assert self.password is not None
        conn = DataBaseConn(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )

        return conn

    def are_equivalent_items(self, external_item: Item, internal_item: Item) -> bool:
        raise NotImplementedError()

    @staticmethod
    def _get_polygon_as_str(item: Item) -> str:
        polygon: str = ""
        for i, coords in enumerate(list(item.geometry.exterior.coords)):
            lon, lat = coords
            polygon += f"{lon} {lat}"
            if i < len(list(item.geometry.exterior.coords)) - 1:
                polygon += ","
        return polygon

    def run_pipeline(self, unregistered_items: List[Item]) -> List[str]:
        """trigger the Sentinel 1 and 2 pipelines by converting the list of unregistered items
        to a list of tasks that are inserted into a table

        Args:
            unregistered_items (List[Item]): list of unregistered items

        Returns:
            List[str]: _description_
        """
        logger.debug(
            f"Running pipeline: collection_id={self.collection_id} external collection={self.external_collection}"
        )
        task_params = list()
        # for each item, insert a new row into tasks table
        for item in unregistered_items:
            assert isinstance(item, TiledItem), f"Error! Unexpected type: {type(item)}"
            status: str = DatabasePipelineHandler.PENDING
            task_type: str = "single"
            process_date = datetime.now(tz=pytz.UTC).strftime(
                DatabasePipelineHandler.DATETIME_FORMAT
            )
            tile = item.tile_id

            polygon_str = DatabasePipelineHandler._get_polygon_as_str(item=item)
            if item.datetime is None:

                assert item.start_datetime is not None
                start_time = item.start_datetime.to_pydatetime()
                if item.end_datetime is None:
                    end_time = None
                else:
                    end_time = item.end_datetime.to_pydatetime()
            else:
                # expand temporal extent to make sure it includes the item
                delta = timedelta(seconds=1)
                start_time = item.datetime.to_pydatetime() - delta
                end_time = item.datetime.to_pydatetime() + delta
            assert isinstance(end_time, datetime)
            # check if task has already been inserted
            task_status = self._select_status_from_tasks_table(
                tile=tile, start_time=start_time, end_time=end_time, polygon=polygon_str
            )
            # if task_status is different than None, task has already been inserted
            if task_status is None:
                self._insert_into_tasks_table(
                    polygon=polygon_str,
                    start_time=start_time,
                    end_time=end_time,
                    status=status,
                    process_date=process_date,
                    task_type=task_type,
                    tile=tile,
                )
                task_params.append(
                    (tile, item.item_id, start_time, end_time, polygon_str)
                )
        self._wait_tasks(task_params=task_params, max_num_queries=self.MAX_ITERATIONS)
        return [i.item_id.replace(".SAFE", "") for i in unregistered_items]

    def _wait_tasks(
        self,
        task_params: List[Tuple[Optional[str], str, datetime, datetime, str]],
        max_num_queries: int = 100,
    ) -> None:
        """query sentinel_2_tasks table to get the status of the task. If task has been processed,
        then remove it from task_params. When task_params is empty, all tasks have been processed

        Args:
            task_params (List[Tuple[str, datetime, datetime]]): tile, start time, end time
            max_num_queries (int, optional): _description_. Defaults to 100.

        """

        iterations = 0
        num_tasks = len(task_params)
        max_num_queries *= num_tasks
        # when task_params size is zero or iteration is equal to max number of queries
        while len(task_params) > 0 and iterations < max_num_queries:
            logger.debug(
                f"{iterations}/{max_num_queries} - Wait until Sentinel pipeline task is completed: {num_tasks=}"
            )
            iterations += 1
            # i is the task_param index. Set i using the (new) size of the task_params variable
            i = len(task_params) - 1
            time.sleep(self.SLEEP_TIME)
            # for each task params, get the status and remove it if it has been processed
            while i >= 0:
                item_param = task_params[i]
                i -= 1
                tile, item_id, start_time, end_time, polygon_str = item_param
                # get status of this task from tasks table using its parameters
                status_task = self._select_status_from_tasks_table(
                    tile=tile,
                    start_time=start_time,
                    end_time=end_time,
                    polygon=polygon_str,
                )
                logger.debug(
                    f"task table: {tile=} {start_time=} {end_time=} {status_task=}"
                )
                assert isinstance(
                    status_task, str
                ), f"Error! Unable to find row that has the following values: table={self.tasks_table} {tile=} {item_id=} {start_time=} {end_time=} {polygon_str=}"
                # if task has been moved to process component
                if status_task == DatabasePipelineHandler.DONE:
                    # get the status of the task from downloads table
                    status_download = self._select_status_from_downloads_table(
                        tile=tile, item_id=item_id, polygon=polygon_str
                    )
                    logger.info(f"downloads table: status={status_download}")
                    # if task param has been processed, remove it
                    if status_download in [
                        DatabasePipelineHandler.DONE,
                    ]:
                        task_params.remove(item_param)

    def _select_status_from_tasks_table(
        self,
        tile: Optional[str],
        start_time: datetime,
        end_time: datetime,
        polygon: Optional[str],
    ) -> Optional[str]:
        try:
            # Connect to the PostgreSQL database
            db_conn = self.create_database_conn()
            conditions = {
                "tile": tile,
                "start_time": start_time,
                "end_time": end_time,
                "polygon": polygon,
            }
            rows = db_conn.select(
                table=self.tasks_table,
                columns=["status"],
                conditions=conditions,
                like_conditions={},
            )
            if rows is not None:
                status = rows[0]
                assert isinstance(status, str), f"Error! Invalid type: {status=}"
                assert status in DatabasePipelineHandler.STATUSES
                return status
            else:
                return None

        except Exception as error:
            logger.debug(f"Error adding task: {error}")
            raise error

    @staticmethod
    def _get_pattern(safe_name: str) -> str:
        fields = safe_name.split("_")
        assert len(fields) >= 5, f"Error! Unexpected STAC id format: {safe_name=}"
        pattern = f"{fields[0]}_%_{fields[2]}_{fields[3]}_{fields[4]}%"
        return pattern

    def _select_status_from_downloads_table(
        self, tile: Optional[str], item_id: Optional[str], polygon: Optional[str]
    ) -> Optional[str]:
        raise NotImplementedError()

    def _insert_into_tasks_table(
        self,
        tile: Optional[str],
        polygon: str,
        start_time: datetime,
        end_time: datetime,
        status: str,
        task_type: str,
        process_date: str,
    ):
        try:
            # Connect to the PostgreSQL database
            conn = self.create_database_conn()
            values = {
                "tile": tile,
                "polygon": polygon,
                "start_time": start_time,
                "end_time": end_time,
                "status": status,
                "task_type": task_type,
                "process_date": process_date,
            }
            conn.insert(table=self.tasks_table, values=values)

        except Exception as error:
            logger.debug(f"Error adding task: {error}")
