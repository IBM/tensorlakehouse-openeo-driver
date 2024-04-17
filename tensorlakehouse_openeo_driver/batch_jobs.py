from typing import Any, Dict, Iterable, List, Optional, Tuple
from openeo_driver.backend import BatchJobMetadata, BatchJobResultMetadata, BatchJobs
from openeo_driver.errors import JobNotFinishedException, JobNotFoundException
from openeo_driver.users.user import User
from openeo_driver.utils import generate_unique_id
from datetime import datetime
from openeo_driver.jobregistry import JOB_STATUS
from celery.states import STARTED, SUCCESS, FAILURE, PENDING, RECEIVED
from openeo_geodn_driver import tasks
from celery import states
from openeo_geodn_driver.constants import GTIFF, logger


class GeodnBatchJobs(BatchJobs):
    _job_registry: Dict[Tuple[str, str], BatchJobMetadata] = {}
    _custom_job_logs: Dict[str, List[str]] = {}

    def generate_job_id(self):
        return generate_unique_id(prefix="j")

    def get_result_metadata(self, job_id: str, user_id: str) -> BatchJobResultMetadata:
        """
        Get job result metadata

        https://openeo.org/documentation/1.0/developers/api/reference.html#tag/Batch-Jobs/operation/list-results
        """
        # Default implementation, based on existing components
        return BatchJobResultMetadata(
            assets=self.get_result_assets(job_id=job_id, user_id=user_id),
            links=[],
            providers=self._get_providers(job_id=job_id, user_id=user_id),
        )

    def get_result_assets(self, job_id: str, user_id: str) -> Dict[str, dict]:
        """
        Return result assets as (filename, metadata) mapping: `filename` is the part that
        the user can see (in download url), `metadata` contains internal (root) dir where
        output is stored.

        related:
        https://openeo.org/documentation/1.0/developers/api/reference.html#tag/Batch-Jobs/operation/list-results
        """
        # Default implementation, based on legacy API
        task = tasks.app.AsyncResult(job_id)
        logger.debug(
            f"batch_jobs::get_result_assets - task id={task.id} state={task.state} task_info={task.info}"
        )
        metadata = task.info
        assert metadata is not None
        assert isinstance(metadata, dict), f"Error! Unexpected type: {metadata}"

        # views.py::_list_job_results requires geotiff instead of gtiff
        if metadata["media_type"].upper() == GTIFF:
            metadata["media_type"] = "geotiff"
        asset_metadata = {
            metadata["filename"]: {
                "title": metadata.get("title", None),
                "href": metadata["href"],
                BatchJobs.ASSET_PUBLIC_HREF: metadata[
                    "href"
                ],  # required by views.py::_asset_object
                "type": metadata["media_type"],
                "roles": ["data"],
                "raster:bands": "bands",
                "file:size": "size",
            }
        }
        return asset_metadata

    def create_job(
        self,
        user_id: str,
        process: dict,
        api_version: str,
        metadata: dict,
        job_options: dict = {},
    ) -> BatchJobMetadata:
        logger.debug(f"batch_jobs::create_job - process={process}")
        # set start time of this task
        created = datetime.now()
        # create task
        task_info = tasks.create_batch_jobs.delay(
            job_id="job-id",
            status=JOB_STATUS.CREATED,
            process=process,
            created=created,
            job_options=job_options,
            title=metadata.get("title"),
            description=metadata.get("description"),
        )
        job_id = task_info.id
        logger.debug(f"batch_jobs::create_job - task_info={task_info}")
        job_info = BatchJobMetadata(
            id=job_id,
            status=JOB_STATUS.CREATED,
            process=process,
            created=created,
            job_options=job_options,
            title=metadata.get("title"),
            description=metadata.get("description"),
        )
        # self._job_registry[(user_id, job_id)] = job_info
        return job_info

    def get_job_info(self, job_id: str, user_id: str) -> BatchJobMetadata:
        """provides metadata about the job id that will compose the response of the endpoint
        /jobs/<id>/results

        Args:
            job_id (str): unique job id (aka task id)
            user_id (str):

        Returns:
            BatchJobMetadata: metadata about job

        """
        # mapping celery states to openeo states
        mapping_states = {
            STARTED: JOB_STATUS.CREATED,
            SUCCESS: JOB_STATUS.FINISHED,
            FAILURE: JOB_STATUS.ERROR,
            PENDING: JOB_STATUS.QUEUED,
            RECEIVED: JOB_STATUS.CREATED,
        }
        try:
            logger.debug(f"batch_jobs::get_job_info - job_id={job_id}")
            # get task state and info
            task = tasks.app.AsyncResult(job_id)
            celery_state = task.state
            metadata: Optional[Dict[str, Any]] = task.info
            # if task.info is none create a default metadata dict
            if metadata is None:
                metadata = {
                    "created": None,
                    "status": JOB_STATUS.CREATED,
                    "geometry": None,
                    "bbox": None,
                    "start_datetime": None,
                    "end_datetime": None,
                    "description": None,
                    "epsg": None,
                }
            # convert celery state to openeo state
            openeo_state = mapping_states[celery_state]
            logger.debug(
                f"get_job_info task id={task.id} job_id={job_id} user_id={user_id} \
                      celery_state={celery_state} openeo_state={openeo_state} \
                        task_info={metadata}"
            )
            if celery_state == states.FAILURE:
                msg = metadata
                logger.error(msg)
                raise Exception(msg)
            # instantiate object that will be returned
            job_metadata = BatchJobMetadata(
                id=job_id,
                status=openeo_state,
                created=metadata["created"],
                geometry=metadata["geometry"],
                links=None,
                bbox=metadata["bbox"],
                start_datetime=metadata["start_datetime"],
                end_datetime=metadata["end_datetime"],
                description=metadata["description"],
                epsg=metadata["epsg"],
            )
            return job_metadata
            # return self._job_registry[(user_id, job_id)]
        except KeyError:
            raise JobNotFoundException(job_id)

    def get_user_jobs(self, user_id: str) -> List[BatchJobMetadata]:
        return [v for (k, v) in self._job_registry.items() if k[0] == user_id]

    @classmethod
    def _update_status(cls, job_id: str, user_id: str, status: str):
        logger.debug(
            f"batch_jobs.py::_update_status jobs_id={job_id} user_id={user_id} status={status}"
        )
        try:
            result = tasks.app.AsyncResult(job_id)
            metadata = result.info
            logger.debug(
                f"batch_jobs.py::_update_status state={result.state} metadata={metadata}"
            )
            if metadata is None:
                metadata = dict()
            # mapping openeo states to celery states
            mapping_states = {
                JOB_STATUS.CREATED: STARTED,
                JOB_STATUS.FINISHED: SUCCESS,
                JOB_STATUS.ERROR: FAILURE,
                JOB_STATUS.QUEUED: PENDING,
                JOB_STATUS.CREATED: RECEIVED,
            }
            new_state = mapping_states.get(status, STARTED)
            # update task metadata by setting state to created and also its metadata
            logger.debug(
                f"batch_jobs.py::_update_status new_state={new_state} metadata={metadata}"
            )
            # allow user to update only tasks that are in progress because it does not
            # make sense to set the state of a task to 'running' if it has finished
            if result.state not in states.READY_STATES:
                result.backend.store_result(job_id, result.result, new_state)

        except KeyError as e:
            logger.error(f"Error! job_id={job_id} error={e}")
            raise JobNotFoundException(job_id)

    def start_job(self, job_id: str, user: User):
        self._update_status(
            job_id=job_id, user_id=user.user_id, status=JOB_STATUS.RUNNING
        )

    def _output_root(self) -> str:
        return "/data/jobs"

    def get_results(self, job_id: str, user_id: str) -> Dict[str, Any]:
        if (
            self._get_job_info(job_id=job_id, user_id=user_id).status
            != JOB_STATUS.FINISHED
        ):
            raise JobNotFinishedException

        return {
            "stac_version": "1.0.0",
            "stac_extensions": [
                "https://openeo.example/stac/custom-extemsion/v1.0.0/schema.json"
            ],
            "id": job_id,
            "type": "Feature",
            "bbox": [-180, -90, 180, 90],
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]
                ],
            },
            "properties": {
                "datetime": "2019-08-24T14:15:22Z",
                "start_datetime": "2019-08-24T14:15:22Z",
                "end_datetime": "2019-08-24T14:15:22Z",
                "title": "NDVI based on Sentinel 2",
                "description": "Deriving minimum NDVI measurements over pixel time series of Sentinel 2",
                "license": "Apache-2.0",
                "providers": [
                    {
                        "name": "Example Cloud Corp.",
                        "description": "No further processing applied.",
                        "roles": ["producer", "licensor", "host"],
                        "url": "https://cloud.example",
                    }
                ],
                "created": "2017-01-01T09:32:12Z",
                "updated": "2017-01-01T09:36:18Z",
                "expires": "2020-11-01T00:00:00Z",
                "openeo:status": "running",
            },
            "assets": {
                "preview.png": {
                    "href": "https://openeo.example/api/v1/download/583fba8b2ce583fba8b2ce/preview.png",
                    "type": "image/png",
                    "title": "Thumbnail",
                    "roles": ["thumbnail"],
                }
            },
            "links": [
                {
                    "rel": "canonical",
                    "type": "application/geo+json",
                    "href": "https://openeo.example/api/v1/download/583fba8b2ce583fba8b2ce/item.json",
                }
            ],
        }

    def get_log_entries(
        self, job_id: str, user_id: str, offset: Optional[str] = None
    ) -> Iterable[dict]:
        self._get_job_info(job_id=job_id, user_id=user_id)
        default_logs = [{"id": "1", "level": "info", "message": "hello world"}]
        for log in self._custom_job_logs.get(job_id, default_logs):
            if isinstance(log, dict):
                yield log
            elif isinstance(log, Exception):
                raise log
            else:
                raise ValueError(log)

    def cancel_job(self, job_id: str, user_id: str):
        self._get_job_info(job_id=job_id, user_id=user_id)

    def delete_job(self, job_id: str, user_id: str):
        self.cancel_job(job_id, user_id)
