from datetime import datetime
from typing import Any, Dict, List, Optional, Union
import time
import uuid

import requests
from tensorlakehouse_openeo_driver.constants import logger


class KFPClientWrapper:

    READY_STATUS = [
        "Completed",
        "Failed",
        "Error",
        "Skipped",
        "Succeeded",
        "PipelineRunStopping",
    ]
    # https://www.kubeflow.org/docs/components/pipelines/legacy-v1/reference/api/kubeflow-pipeline-api-spec/
    RELATIONSHIP_CREATOR = "CREATOR"
    RELATIONSHIP_UNKNOWN_RELATIONSHIP = "UNKNOWN_RELATIONSHIP"
    RELATIONSHIP_OWNER = "OWNER"

    def __init__(self, host: str, pipeline_name: str) -> None:
        """_summary_

        Args:
            host (str): url to KFP API, exclude api version

        """
        self.host = host
        # self.client = kfp.Client(host=host)

        assert isinstance(pipeline_name, str)
        self.pipeline_name = pipeline_name
        self.experiment_name = f"openeo-{pipeline_name}"
        self._experiment_id: Optional[str] = None

    # def list_pipelines(self, page_size: int = 10) -> Set[str]:
    #     """list existing pipelines

    #     Args:
    #         page_size (int, optional): number of pipelines per page. Defaults to 10.

    #     Returns:
    #         set: list of pipeline names
    #     """
    #     page_token = None
    #     pipelines = set()
    #     while True:
    #         response = self.client.list_pipelines(
    #             page_size=page_size, page_token=page_token
    #         )
    #         for p in response.pipelines:
    #             pipelines.add(p.name)

    #         # Check if there are more pages to retrieve
    #         if response.next_page_token:
    #             page_token = response.next_page_token
    #         else:
    #             break
    #     return pipelines

    def list_pipelines(self, page_size: int = 10):

        url = f"{self.host}/apis/v1beta1/pipelines"
        pipelines = list()
        page_token = None
        while True:
            params = {"page_size": page_size, "page_token": page_token}
            resp = requests.get(url=url, params=params)
            resp.raise_for_status()
            pipeline_dict = resp.json()
            assert isinstance(pipeline_dict, dict)
            pipelines.extend(pipeline_dict["pipelines"])

            # Check if there are more pages to retrieve
            if "next_page_token" in pipeline_dict.keys():
                page_token = pipeline_dict["next_page_token"]
            else:
                break
        return pipelines

    def get_pipeline_id(self) -> str:
        pipelines = self.list_pipelines()
        pipeline_id = None
        for pipeline in pipelines:
            name = pipeline["name"]
            if name == self.pipeline_name:
                pipeline_id = pipeline["id"]
                break

        # requests.get(${SVC}/apis/v1beta1/pipelines/${PIPELINE_ID})
        assert isinstance(
            pipeline_id, str
        ), f"Error! pipeline name not found: {self.pipeline_name}"
        return pipeline_id

    def get_version_id(self, version_name: str, pipeline_id: Optional[str]) -> str:
        if pipeline_id is None:
            pipeline_id = self.get_pipeline_id()
        url = f"{self.host}/apis/v1beta1/pipeline_versions"
        params = {"resource_key.type": "PIPELINE", "resource_key.id": pipeline_id}
        resp = requests.get(url=url, params=params)
        resp.raise_for_status()
        response = resp.json()
        assert isinstance(response, dict), f"Error! {response=} is not a dict"
        versions = response["versions"]
        i = 0
        found = False
        version_id = None

        while i < len(versions) and not found:
            version = versions[i]
            i += 1
            if version["name"] == version_name:
                found = True
                version_id = version["id"]
        assert found, f"Error! Unable to find {version_name=} of pipeline {pipeline_id}"
        assert isinstance(version_id, str)
        return version_id

    def list_experiments(self, page_size: int = 10) -> List[Dict]:
        experiments = list()
        url = f"{self.host}/apis/v1beta1/experiments"
        # initialize page_token to enter the while loop
        page_token: Optional[str] = ""
        params: Dict[str, Union[int, Optional[str]]] = {
            "page_size": page_size,
            "page_token": None,
        }
        while page_token is not None:

            resp = requests.get(url=url, params=params)
            resp.raise_for_status()
            response = resp.json()
            assert isinstance(response, dict), f"Error! {response=} is not a dict"
            experiments.extend(response["experiments"])
            page_token = response.get("next_page_token", None)
            params = {"page_size": page_size, "page_token": page_token}
        return experiments

    def create_experiment(self, experiment_name: str) -> str:
        """create new experiment

        Args:
            experiment_name (str): _description_

        Raises:
            TimeoutError: _description_

        Returns:
            str: experiment id
        """
        url = f"{self.host}/apis/v1beta1/experiments"

        payload = {
            "name": experiment_name,
            "description": experiment_name,
            "created_at": KFPClientWrapper.get_now(),
        }
        resp = requests.post(url=url, json=payload)
        resp.raise_for_status()
        exp = resp.json()
        assert isinstance(exp, dict)
        exp_id = exp["id"]
        assert isinstance(exp_id, str)
        return exp_id

    @staticmethod
    def get_now() -> str:
        now = datetime.now()
        dt_format = "%Y-%m-%dT%H:%M:%SZ"
        return now.strftime(dt_format)

    def find_experiment_id(
        self, experiment_name: str, experiments: List[Dict]
    ) -> Optional[str]:
        for exp in experiments:
            if exp["name"] == experiment_name:
                exp_id = exp["id"]
                assert isinstance(exp_id, str)
                return exp_id
        return None

    @staticmethod
    def create_job_name() -> str:
        random_str = uuid.uuid4().hex
        # job_name = f"{pipeline_id}-{random_str}"
        job_name = f"tensorlakehouse-{random_str}"
        return job_name

    @staticmethod
    def _convert_parameters(params: Optional[Dict[str, str]]) -> List[Dict]:
        """convert parameters from {"param-name": "param-value"} to
        [{"name": "param-name", "value": "param-value"}], which is the accepted format by KFP API

        Args:
            params (Optional[Dict[str, str]]): parameters to be converted

        Returns:
            List[Dict]: converted parameters
        """
        parameters = list()
        if params is not None:
            for k, v in params.items():
                parameters.append({"name": k, "value": v})
        return parameters

    def create_pipeline_run(
        self,
        pipeline_id: str,
        params: Optional[Dict[str, str]],
        job_name: str,
        experiment_id: str,
        version_id: str,
    ) -> Dict:
        url = f"{self.host}/apis/v1beta1/runs"
        payload: Dict[str, Any] = {
            "name": job_name,
            "pipeline_spec": {
                "pipeline_id": pipeline_id,
            },
            "resource_references": [
                {
                    "key": {
                        "type": "EXPERIMENT",
                        "id": experiment_id,
                    },
                    "name": "Default",
                    "relationship": KFPClientWrapper.RELATIONSHIP_OWNER,
                },
                {
                    "key": {
                        "type": "PIPELINE_VERSION",
                        "id": version_id,
                    },
                    "name": "Default",
                    "relationship": KFPClientWrapper.RELATIONSHIP_CREATOR,
                },
            ],
            "created_at": KFPClientWrapper.get_now(),
        }
        parameters = KFPClientWrapper._convert_parameters(params=params)
        if len(parameters) > 0:
            payload["pipeline_spec"]["parameters"] = parameters
        resp = requests.post(url=url, json=payload)
        resp.raise_for_status()
        response = resp.json()

        assert isinstance(response, dict)
        return response

    def run_pipeline(
        self,
        pipeline_id: str,
        version_id: str,
        job_name: Optional[str] = None,
        params: Optional[Dict[str, str]] = None,
    ) -> str:
        experiments = self.list_experiments()
        experiment_id = self.find_experiment_id(
            experiment_name=self.experiment_name, experiments=experiments
        )
        if experiment_id is None:
            experiment_id = self.create_experiment(experiment_name=self.experiment_name)
        logger.debug(f"Experiment has been created: {experiment_id=} {pipeline_id=}")
        # OR get an existing experiment by name
        if job_name is None:
            job_name = KFPClientWrapper.create_job_name()
        run = self.create_pipeline_run(
            pipeline_id=pipeline_id,
            params=params,
            job_name=job_name,
            experiment_id=experiment_id,
            version_id=version_id,
        )
        run_id = run["run"]["id"]
        assert isinstance(run_id, str), f"Error! {run_id=} is not a str"
        logger.debug(f"Pipeline has been started: {run_id}")
        return run_id

    def get_pipeline_run(self, pipeline_run_id: str):
        url = f"{self.host}/apis/v1beta1/runs/{pipeline_run_id}"
        resp = requests.get(url=url)
        resp.raise_for_status()
        response = resp.json()
        assert isinstance(response, dict)
        try:
            status = response["run"]["status"]
            return status
        except KeyError as e:
            msg = f"Error! Missing 'run' or 'status' keys -  {response=} - {e}"
            raise KeyError(msg)

    def wait_until_complete(self, runs: List[str], max_requests: int = 200) -> None:
        """track the status of the run until it finishes

        Args:
            run (ApiRun): run metadata
            max_requests (int, optional): max number of requests before timeout

        Raises:
            TimeoutError: _description_

        Returns:
            _type_: _description_
        """
        # list of status when the run is ready
        counter = 0
        num_runs = len(runs)

        while len(runs) > 0 and counter < max_requests:
            # counter += 1
            for index, run_id in enumerate(runs):
                time.sleep(1)
                # initialize counter and sleep time variables
                try:
                    # Get the run status
                    run_status = self.get_pipeline_run(pipeline_run_id=run_id)
                except Exception as e:
                    logger.debug(e)
                    time.sleep(60)
                    run_status = self.get_pipeline_run(pipeline_run_id=run_id)

                logger.debug(f"Pipeline Run id={run_id} status={run_status}")

                if run_status in KFPClientWrapper.READY_STATUS:
                    runs.pop(index)

            num_runs = len(runs)
            sleep_time = 2**counter
            sleep_time = min(sleep_time, 60)
            counter += 1
            logger.debug(
                f"{counter} - the number of pipeline runs in progress is {num_runs} - sleep for {sleep_time} seconds..."
            )
            time.sleep(sleep_time)

        if counter >= max_requests:
            msg = (
                f"Timeout Error! Failed to run all {num_runs} runs because it exceeded the number of max requests: {counter} >= {max_requests}",
            )

            raise TimeoutError(msg)
        logger.debug(f"wait_until_complete:: {counter=} {max_requests=} {num_runs=}")

    def get_pipeline(self, pipeline_id: str):
        pass
