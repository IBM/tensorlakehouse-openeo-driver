from typing import List

from tensorlakehouse_openeo_driver.constants import (
    COPERNICUS_STAC,
    KUBEFLOW_PIPELINE_API,
    SENTINEL_2_L1C,
    logger,
)

from tensorlakehouse_openeo_driver.model.item import Item

from tensorlakehouse_openeo_driver.pipeline.handler.pipeline_handler import (
    PipelineHandler,
)
from tensorlakehouse_openeo_driver.pipeline.kfpclient import KFPClientWrapper


class Sentinel2PipelineHandlerL1C(PipelineHandler):

    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"
    PENDING = "pending"
    DONE = "done"
    FAIL = "fail"

    SAFE = ".SAFE"

    def __init__(self) -> None:
        super().__init__()
        self.collection_id = SENTINEL_2_L1C
        self._pipeline_name = "sentinel2-l1c-download-pipeline"
        self._version_name = (
            "sentinel2-l1c-download-pipeline_version_at_2024-11-20T14:04:13.527Z"
        )
        self.external_collection = "SENTINEL-2"
        self.datasource_stac_url = COPERNICUS_STAC
        self._filter_cql = {"op": "=", "args": [{"property": "productType"}, "S2MSI1C"]}

    def are_equivalent_items(self, external_item: Item, internal_item: Item) -> bool:

        ext_item_id = external_item.item_id[:-5]
        if ext_item_id == internal_item.item_id:
            return True
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
