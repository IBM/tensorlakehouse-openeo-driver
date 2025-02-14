from typing import Optional
from tensorlakehouse_openeo_driver.constants import (
    HLS,
    PIPELINE_DISABLED,
    SENTINEL_1_GRD,
    SENTINEL_2_L1C,
    SENTINEL_2_L2A,
    logger,
)
from tensorlakehouse_openeo_driver.pipeline.handler.hls_pipeline_handler import (
    HLSPipelineHandler,
)
from tensorlakehouse_openeo_driver.pipeline.handler.pipeline_handler import (
    PipelineHandler,
)
from tensorlakehouse_openeo_driver.pipeline.handler.sentinel_1_pipeline_handler import (
    Sentinel1PipelineHandler,
)
from tensorlakehouse_openeo_driver.pipeline.handler.sentinel_2_pipeline_handler_l1c import (
    Sentinel2PipelineHandlerL1C,
)
from tensorlakehouse_openeo_driver.pipeline.handler.sentinel_2_pipeline_handler_l2a import (
    Sentinel2PipelineHandlerL2A,
)


def make_handler(collection_id: str) -> Optional[PipelineHandler]:
    logger.debug(f"handler_factory: make_hander = {collection_id=} {PIPELINE_DISABLED=}")
    if PIPELINE_DISABLED:
        return None
    elif collection_id == HLS:
        return HLSPipelineHandler()
    elif collection_id == SENTINEL_1_GRD:
        return Sentinel1PipelineHandler()
    elif collection_id == SENTINEL_2_L2A:
        return Sentinel2PipelineHandlerL2A()
    elif collection_id == SENTINEL_2_L1C:
        return Sentinel2PipelineHandlerL1C()
    else:
        return None
