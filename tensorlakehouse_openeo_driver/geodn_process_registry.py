from openeo_pg_parser_networkx import ProcessRegistry
from typing import Iterable, List, Optional
from openeo_pg_parser_networkx.process_registry import Process
from openeo_driver.processes import ProcessRegistry, ProcessesListing


class TensorLakeHouseProcessesListing(ProcessesListing):
    def to_response_dict(self) -> dict:
        resp = super().to_response_dict()
        resp["links"].append({"rel": "docs", "href": "http://processing.test/dummy"})
        resp["flavor"] = "salt and pepper"
        resp["version"] = "dummy-v2"
        return resp


class TensorLakehouseProcessRegistry(ProcessRegistry):
    def __init__(self, wrap_funcs: Optional[list] = None, *args, **kwargs):
        super().__init__(wrap_funcs, *args, **kwargs)

    def get_specs(self, exclusion_list: Optional[Iterable[str]] = None):
        assert isinstance(self.store, dict), f"Error! Not a dict: {self.store}"
        store = self.store
        process_list = []
        # iterate over the list of registered processes
        for processes in store.values():

            for proc in processes.values():
                assert isinstance(proc, Process)
                process_list.append(proc.spec)
        return process_list

    def get_processes_listing(
        self, *, exclusion_list: Optional[Iterable[str]] = None
    ) -> ProcessesListing:
        return TensorLakeHouseProcessesListing(
            processes=self.get_specs(exclusion_list=exclusion_list),
            target_version=self.target_version,
        )
