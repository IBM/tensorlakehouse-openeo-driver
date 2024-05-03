from pathlib import Path
from typing import Iterable, List, Union
from openeo_driver.utils import read_json
from openeo_driver.ProcessGraphDeserializer import ConcreteProcessing
from openeo_driver.dry_run import SourceConstraint
from tensorlakehouse_openeo_driver.save_result import GeoDNImageCollectionResult
from openeo_driver.utils import EvalEnv
from openeo_pg_parser_networkx import OpenEOProcessGraph
from openeo_pg_parser_networkx import ProcessRegistry, Process
from tensorlakehouse_openeo_driver.geodn_process_registry import GeodnProcessRegistry
import os
import logging
from openeo.capabilities import ComparableVersion

from tensorlakehouse_openeo_driver.get_specs import get_process_names
from tensorlakehouse_openeo_driver.get_openeo_process_implementations import get_openeo_impls
from tensorlakehouse_openeo_driver.get_process_implementations import get_impls
from openeo_processes_dask.process_implementations import _max, _min
from openeo_processes_dask.specs import _max as max_spec, _min as min_spec
from openeo_processes_dask.process_implementations.core import process


assert os.path.isfile("logging.conf")
logging.config.fileConfig(fname="logging.conf", disable_existing_loggers=False)
logger = logging.getLogger("geodnLogger")


class GeoDNProcessing(ConcreteProcessing):
    def __init__(self) -> None:
        super().__init__()
        # `process` is wrapped around each registered implementation
        self.process_registry = GeodnProcessRegistry(wrap_funcs=[process])

        process_names = get_process_names()
        # explicit reading rename dimension and rename labels processes specification
        # because they're not part of openeo-process-dask lib
        openeo_process_specs = Path() / "tensorlakehouse_openeo_driver" / "process_specifications"
        for proc_name in ["rename_dimension", "rename_labels"]:
            proc_path = openeo_process_specs / f"{proc_name}.json"

            assert proc_path.exists()
            proc_spec = read_json(proc_path)
            process_names.append(proc_spec["id"])

        openeo_impls = get_openeo_impls()
        geodn_impls = get_impls()
        # get a list of processes implemented by openeo-process-dask
        processes_by_openeo = list(
            set(process_names).intersection([t for t in openeo_impls.keys()])
        )
        # get a list of processes implemented by openeo-geodn-driver
        processes_by_geodn = list(set(process_names).intersection(geodn_impls))

        # remove openeo implementations that are implemented by geodn
        for item in processes_by_geodn:
            if item in processes_by_openeo:
                processes_by_openeo.remove(item)

        proc_data = []
        # add openeo_process_dask name, spec and impl
        for item in processes_by_openeo:
            # import its spec
            specsmod = __import__("openeo_processes_dask.specs", fromlist=[item])
            itemspec = getattr(specsmod, item)

            # import its implementation
            if item in openeo_impls.keys():
                module_dir = openeo_impls[item]
                implmod = __import__(module_dir, fromlist=[item])
                itemimpl = getattr(implmod, item)

                proc_data.append({"name": item, "spec": itemspec, "impl": itemimpl})
        # add geodn processes name, spec and impl
        for item in processes_by_geodn:
            # import its spec
            try:
                specsmod = __import__("openeo_processes_dask.specs", fromlist=[item])
                itemspec = getattr(specsmod, item)
            except AttributeError:
                proc_path = openeo_process_specs / f"{item}.json"

                assert proc_path.exists()
                itemspec = read_json(proc_path)

            # import its implementation
            implmod = __import__("tensorlakehouse_openeo_driver.processes", fromlist=[item])
            itemimpl = getattr(implmod, item)

            proc_data.append({"name": item, "spec": itemspec, "impl": itemimpl})

        proc_data.append({"name": "max", "spec": max_spec, "impl": _max})
        proc_data.append({"name": "min", "spec": min_spec, "impl": _min})

        for p in proc_data:
            self.process_registry[p["name"]] = Process(spec=p["spec"], implementation=p["impl"])

    def get_process_registry(self, api_version: Union[str, ComparableVersion]) -> ProcessRegistry:
        return self.process_registry

    def evaluate(self, process_graph: dict, env: EvalEnv = None):
        parsed_graph = OpenEOProcessGraph(pg_data=process_graph)

        # get process graph
        pg_callable = parsed_graph.to_callable(process_registry=self.process_registry)
        result = pg_callable()

        if isinstance(result, GeoDNImageCollectionResult):
            return result
        else:
            return result

    def extra_validation(
        self,
        process_graph: dict,
        env: EvalEnv,
        result,
        source_constraints: List[SourceConstraint],
    ) -> Iterable[dict]:
        # Fake missing tiles
        for source_id, constraints in source_constraints:
            if source_id[0] == "load_collection" and source_id[1][0] == "S2_FOOBAR":
                dates = constraints.get("temporal_extent")
                bbox = constraints.get("spatial_extent")
                if dates and dates[0] <= "2021-02-10" and bbox and bbox["west"] <= 1.4:
                    yield {
                        "code": "MissingProduct",
                        "message": "Tile 4322 not available",
                    }
