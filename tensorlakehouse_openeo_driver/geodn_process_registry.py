from openeo_pg_parser_networkx import ProcessRegistry
from typing import Optional


class GeodnProcessRegistry(ProcessRegistry):
    def __init__(self, wrap_funcs: Optional[list] = None, *args, **kwargs):
        super().__init__(wrap_funcs, *args, **kwargs)

    def get_specs(self):
        store = self.store
        process_list = []
        for processes in store.values():
            for proc in processes.values():
                process_list.append(proc.spec)
        return process_list
