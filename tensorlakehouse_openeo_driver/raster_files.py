from datetime import datetime
from typing import List, Union
import uuid
from zipfile import ZipFile
import rioxarray
import xarray as xr
from pathlib import Path
import filetype

DEFAULT_TIME_DIMENSION = "time"


def open_tif_files(path: Path) -> xr.Dataset:
    assert path is not None
    if isinstance(path, str):
        path = Path(path)
    assert isinstance(path, Path), f"Error! path is not a pathlib.Path: {type(path)}"
    # if path does not exist, replace tif by zip
    if not path.exists():
        zip_path = path.rename(path.with_suffix(".zip"))

    assert path.exists()
    assert path.is_file()
    kind = filetype.guess(path)
    # rename tif to zip
    if "zip" in kind.mime.lower() and "zip" not in path.suffix:
        print(path.suffix)
        zip_path = path.rename(path.with_suffix(".zip"))
        print(f"Extracting file: {zip_path}")
        # set new directory name
        target_dir = path.parent / uuid.uuid4().hex
        # extract all
        zipfile = ZipFile(zip_path)
        zipfile.extractall(path=target_dir)
        ds = _open_multiple_tif(target_dir=target_dir)

    else:
        print(f"Open file: {path}")
        ds = rioxarray.open_rasterio(path)  # type: ignore
    return ds


def _open_multiple_tif(target_dir: Path) -> xr.Dataset:
    raster_files = sorted(list(target_dir.rglob("openeo*.tif")))
    print("Extracted files:")
    timestamps: List[Union[int, datetime]] = list()
    for index, f in enumerate(raster_files):
        if len(f.name) > 35:
            try:
                t: Union[int, datetime] = datetime.strptime(
                    f.name[15:35], "%Y-%m-%dT%H-%M-%SZ"
                )
            except ValueError:
                t = index
        else:
            t = index
        timestamps.append(t)
    # open all raster files and concatenate
    ds = xr.open_mfdataset(
        list(sorted(raster_files)),
        concat_dim="time",
        combine="nested",
    )
    ds = ds.assign_coords({DEFAULT_TIME_DIMENSION: timestamps})
    return ds
