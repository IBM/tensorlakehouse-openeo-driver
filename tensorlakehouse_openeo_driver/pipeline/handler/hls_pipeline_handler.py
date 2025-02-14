from typing import List
from tensorlakehouse_openeo_driver.constants import (
    HLS,
    KUBEFLOW_PIPELINE_API,
    NASA_STAC,
    logger,
)
from tensorlakehouse_openeo_driver.model.item import Item
from tensorlakehouse_openeo_driver.pipeline.handler.pipeline_handler import (
    PipelineHandler,
)
from tensorlakehouse_openeo_driver.pipeline.kfpclient import KFPClientWrapper


class HLSPipelineHandler(PipelineHandler):
    def __init__(self) -> None:
        super().__init__()
        self.collection_id = HLS
        self._pipeline_name = "HLS ingestion"
        # self._version_name = "v1.0 (10 parallel)"
        self._version_name = "v1.0 (2 parallel)"
        self.external_collection = "HLSS30_2.0"
        self.datasource_stac_url = NASA_STAC
        self._temporal_buffer = 60 * 15

    def are_equivalent_items(self, external_item: Item, internal_item: Item) -> bool:
        """compare item from external STAC and item from internal STAC and return True
        if they are equivalent. Otherwise, return False

        Args:
            datasource_item (Item): external item
            internal_item (Item): internal item

        Returns:
            bool: True, if items are equivalent, otherwise False
        """
        if external_item.item_id == internal_item.item_id:
            return True
        else:
            return False

    def run_pipeline(self, unregistered_items: List[Item]) -> List[str]:
        logger.debug(
            f"Running pipeline: collection_id={self.collection_id} external collection={self.external_collection}"
        )
        assert isinstance(
            KUBEFLOW_PIPELINE_API, str
        ), f"Error! Unexpected value: {KUBEFLOW_PIPELINE_API=}"
        kfp_client = KFPClientWrapper(
            host=KUBEFLOW_PIPELINE_API, pipeline_name=self.pipeline_name
        )
        # get pipeline id by name
        pipeline_id = kfp_client.get_pipeline_id()
        assert isinstance(
            self.version_name, str
        ), f"Error! Unexpected version name: {self.version_name}"
        version_id = kfp_client.get_version_id(
            version_name=self.version_name, pipeline_id=pipeline_id
        )

        # for each non-registered item, trigger the corresponding data pipeline
        runs: List[str] = list()
        for item in unregistered_items:

            # generate parameters that are input for data pipelines
            params = item.to_params()

            job_name = KFPClientWrapper.create_job_name()
            run = kfp_client.run_pipeline(
                pipeline_id=pipeline_id,
                job_name=job_name,
                params=params,
                version_id=version_id,
            )
            runs.append(run)
        # wait until all runs finish
        kfp_client.wait_until_complete(runs=runs)
        return [i.item_id for i in unregistered_items]
