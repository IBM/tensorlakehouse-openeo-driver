from typing import Dict, List, Optional, Tuple
from tensorlakehouse_openeo_driver.model.item import Item, make_item
from datetime import datetime
from tensorlakehouse_openeo_driver.constants import (
    STAC_URL,
    logger,
)
from tensorlakehouse_openeo_driver.stac.stac import STAC


class PipelineHandler:

    def __init__(self) -> None:
        self.collection_id: str = ""
        self._pipeline_name: str = ""
        self._version_name: str = ""
        self.external_collection: str = ""
        self.datasource_stac_url: str = ""
        self.headers = {"Content-type": "application/json"}
        self.stac_fields = {
            "includes": [
                "id",
                "bbox",
                "geometry",
                "properties.datetime",
                "properties.start_datetime",
                "properties.end_datetime",
            ],
            "excludes": ["links"],
        }
        self._internal_items: List[Item] = list()
        # filter item ids by pattern (e.g., exclude L1C items)
        self._filter_cql: Optional[Dict] = None
        self._temporal_buffer: int = 1

    @property
    def filter_cql(self) -> Optional[Dict]:
        return self._filter_cql

    @property
    def temporal_buffer(self) -> int:
        return self._temporal_buffer

    @property
    def internal_items(self) -> List[Item]:
        return self._internal_items

    @property
    def version_name(self) -> str:
        return self._version_name

    @property
    def pipeline_name(self) -> str:
        assert len(self._pipeline_name) > 0
        return self._pipeline_name

    def set_internal_items(self, item_as_dicts: List[Dict]):
        """_summary_

        Args:
            item_as_dicts (List[Dict]): _description_

        Returns:
            _type_: _description_
        """
        for item_as_dict in item_as_dicts:
            item = make_item(item_dict=item_as_dict)
            self._internal_items.append(item)

    def compare_items(
        self, external_items: List[Item], internal_items: List[Item]
    ) -> List[Item]:
        """compare the items returned by STAC-source against the items returned by STAC-mirror
        and return a list of items that have not been registered in STAC-mirror


        Args:
            external_items (List[Item]): _description_
            internal_items (List[Item]): _description_

        Raises:
            NotImplementedError: _description_

        Returns:
            List[Item]: _description_
        """

        # find items that have not been registered in our instance of STAC
        unregistered_items = list()
        # for each external item
        for ext_item in external_items:
            found = False
            j = 0
            # find equivalent internal item
            while j < len(internal_items) and not found:
                int_item = internal_items[j]
                j += 1
                if self.are_equivalent_items(
                    external_item=ext_item, internal_item=int_item
                ):

                    found = True
                    print(ext_item, int_item)
            if not found:
                unregistered_items.append(ext_item)
        return unregistered_items

    def are_equivalent_items(self, external_item: Item, internal_item: Item) -> bool:
        """compare item from external STAC and item from internal STAC and return True
        if they are equivalent. Otherwise, return False

        Args:
            datasource_item (Item): external item
            internal_item (Item): internal item

        Returns:
            bool: True, if items are equivalent, otherwise False
        """

        raise NotImplementedError()

    def run_pipeline(self, unregistered_items: List[Item]) -> List[str]:
        raise NotImplementedError()

    def trigger_pipeline(
        self,
        bbox: Tuple[float, float, float, float],
        temporal_extent: Tuple[datetime, datetime | None],
    ) -> List[Item]:
        """this is the main function that triggers the data pipeline. It should be generic to
        support all data pipelines

        Args:
            bbox (Tuple[float, float, float, float]): _description_
            temporal_extent (Tuple[datetime, datetime  |  None]): _description_

        Returns:
            List[str]: a list of item ids from external STAC instance that have been registered
        """
        limit = 1000
        logger.debug(
            f"trigger_pipeline {bbox=} {temporal_extent=} {self.collection_id}"
        )
        # get list of items of the original data source
        datasource_stac = STAC(url=self.datasource_stac_url)
        # search external STAC instance
        external_items = datasource_stac.search_items(
            collections=[self.external_collection],
            bbox=bbox,
            temporal_extent=temporal_extent,
            limit=limit,
            filter_cql=self.filter_cql,
        )
        # if there is no external item, we assume that there is no data and return empty list
        if len(external_items) == 0:
            logger.debug("No external item has been found")
            return list()
        else:
            if len(self.internal_items) == 0:
                # search IBM's instance of STAC
                stac_local = STAC(url=STAC_URL)
                internal_items = stac_local.search_items(
                    collections=[self.collection_id],
                    bbox=bbox,
                    temporal_extent=temporal_extent,
                    limit=limit,
                    temporal_buffer=self.temporal_buffer,
                    filter_cql=None,
                )

            # get list of items that have not been registered in our STAC
            unregistered_items = self.compare_items(
                internal_items=internal_items, external_items=external_items
            )
            # trigger pipeline and register items
            self.run_pipeline(unregistered_items=unregistered_items)

            return unregistered_items
