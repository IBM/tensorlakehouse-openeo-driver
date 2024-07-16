from openeo_pg_parser_networkx import ProcessRegistry
from typing import Optional
from openeo_pg_parser_networkx.process_registry import Process


class TensorLakehouseProcessRegistry(ProcessRegistry):
    def __init__(self, wrap_funcs: Optional[list] = None, *args, **kwargs):
        super().__init__(wrap_funcs, *args, **kwargs)

    def get_specs(self):
        assert isinstance(self.store, dict), f"Error! Not a dict: {self.store}"
        store = self.store
        process_list = []
        # iterate over the list of registered processes
        for processes in store.values():

            for proc in processes.values():
                assert isinstance(proc, Process)
                process_list.append(proc.spec)
        return process_list
