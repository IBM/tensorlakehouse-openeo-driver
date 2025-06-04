from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from pystac import Item
import pytest
import xarray as xr

from tensorlakehouse_openeo_driver.constants import (
    DEFAULT_BANDS_DIMENSION,
    TEST_DATA_ROOT,
)
from tensorlakehouse_openeo_driver.file_reader.grib2_file_reader import Grib2FileReader
from datetime import datetime
from rasterio.crs import CRS
from openeo_pg_parser_networkx.pg_schema import ParameterReference

GRIB2_ITEM = {
    "type": "Feature",
    "stac_version": "1.0.0",
    "id": "hrrr-conus-sfc-2024-05-10T12-FH2",
    "assets": {
        "data": {
            "href": "https://noaahrrr.blob.core.windows.net/hrrr/hrrr.20240510/conus/hrrr.t12z.wrfsfcf02.grib2",
            "type": "application/wmo-GRIB2",
            "title": "2D Surface Levels",
            "description": "2D Surface Level forecast data as a grib2 file. Subsets of the data can be loaded using the provided byte range.",
            "grib:layers": {
                "REFC entire atmosphere 2 hour fcst": {
                    "start_byte": 0,
                    "byte_size": 345203,
                },
                "RETOP cloud top 2 hour fcst": {
                    "start_byte": 345203,
                    "byte_size": 103748,
                },
                "var discipline=0 center=7 local_table=1 parmcat=16 parm=201 entire atmosphere 2 hour fcst": {
                    "start_byte": 448951,
                    "byte_size": 298772,
                },
                "VIL entire atmosphere 2 hour fcst": {
                    "start_byte": 747723,
                    "byte_size": 206066,
                },
                "VIS surface 2 hour fcst": {"start_byte": 953789, "byte_size": 1442151},
                "REFD 1000 m above ground 2 hour fcst": {
                    "start_byte": 2395940,
                    "byte_size": 121854,
                },
                "REFD 4000 m above ground 2 hour fcst": {
                    "start_byte": 2517794,
                    "byte_size": 75988,
                },
                "REFD 263 K level 2 hour fcst": {
                    "start_byte": 2593782,
                    "byte_size": 93833,
                },
                "GUST surface 2 hour fcst": {
                    "start_byte": 2687615,
                    "byte_size": 1169844,
                },
                "UGRD 250 mb 2 hour fcst": {"start_byte": 3857459, "byte_size": 733876},
                "VGRD 250 mb 2 hour fcst": {"start_byte": 4591335, "byte_size": 709074},
                "UGRD 300 mb 2 hour fcst": {"start_byte": 5300409, "byte_size": 730408},
                "VGRD 300 mb 2 hour fcst": {"start_byte": 6030817, "byte_size": 695545},
                "HGT 500 mb 2 hour fcst": {"start_byte": 6726362, "byte_size": 692648},
                "TMP 500 mb 2 hour fcst": {"start_byte": 7419010, "byte_size": 547193},
                "DPT 500 mb 2 hour fcst": {"start_byte": 7966203, "byte_size": 889875},
                "UGRD 500 mb 2 hour fcst": {"start_byte": 8856078, "byte_size": 592239},
                "VGRD 500 mb 2 hour fcst": {"start_byte": 9448317, "byte_size": 585955},
                "HGT 700 mb 2 hour fcst": {"start_byte": 10034272, "byte_size": 690596},
                "TMP 700 mb 2 hour fcst": {"start_byte": 10724868, "byte_size": 555202},
                "DPT 700 mb 2 hour fcst": {"start_byte": 11280070, "byte_size": 988215},
                "DZDT 700 mb 2 hour fcst": {
                    "start_byte": 12268285,
                    "byte_size": 891351,
                },
                "UGRD 700 mb 2 hour fcst": {
                    "start_byte": 13159636,
                    "byte_size": 595194,
                },
                "VGRD 700 mb 2 hour fcst": {
                    "start_byte": 13754830,
                    "byte_size": 585048,
                },
                "HGT 850 mb 2 hour fcst": {"start_byte": 14339878, "byte_size": 723302},
                "TMP 850 mb 2 hour fcst": {"start_byte": 15063180, "byte_size": 581924},
                "DPT 850 mb 2 hour fcst": {
                    "start_byte": 15645104,
                    "byte_size": 1102354,
                },
                "UGRD 850 mb 2 hour fcst": {
                    "start_byte": 16747458,
                    "byte_size": 619672,
                },
                "VGRD 850 mb 2 hour fcst": {
                    "start_byte": 17367130,
                    "byte_size": 611669,
                },
                "TMP 925 mb 2 hour fcst": {"start_byte": 17978799, "byte_size": 612363},
                "DPT 925 mb 2 hour fcst": {
                    "start_byte": 18591162,
                    "byte_size": 1138445,
                },
                "UGRD 925 mb 2 hour fcst": {
                    "start_byte": 19729607,
                    "byte_size": 617257,
                },
                "VGRD 925 mb 2 hour fcst": {
                    "start_byte": 20346864,
                    "byte_size": 625829,
                },
                "TMP 1000 mb 2 hour fcst": {
                    "start_byte": 20972693,
                    "byte_size": 634175,
                },
                "DPT 1000 mb 2 hour fcst": {
                    "start_byte": 21606868,
                    "byte_size": 1130618,
                },
                "UGRD 1000 mb 2 hour fcst": {
                    "start_byte": 22737486,
                    "byte_size": 617380,
                },
                "VGRD 1000 mb 2 hour fcst": {
                    "start_byte": 23354866,
                    "byte_size": 616183,
                },
                "MAXUVV 100-1000 mb above ground 1-2 hour max fcst": {
                    "start_byte": 23971049,
                    "byte_size": 654497,
                },
                "MAXDVV 100-1000 mb above ground 1-2 hour max fcst": {
                    "start_byte": 24625546,
                    "byte_size": 646388,
                },
                "DZDT 0.5-0.8 sigma layer 1-2 hour ave fcst": {
                    "start_byte": 25271934,
                    "byte_size": 610500,
                },
                "MSLMA mean sea level 2 hour fcst": {
                    "start_byte": 25882434,
                    "byte_size": 610436,
                },
                "HGT 1000 mb 2 hour fcst": {
                    "start_byte": 26492870,
                    "byte_size": 702419,
                },
                "MAXREF 1000 m above ground 1-2 hour max fcst": {
                    "start_byte": 27195289,
                    "byte_size": 209653,
                },
                "REFD 263 K level 1-2 hour max fcst": {
                    "start_byte": 27404942,
                    "byte_size": 101256,
                },
                "MXUPHL 5000-2000 m above ground 1-2 hour max fcst": {
                    "start_byte": 27506198,
                    "byte_size": 8946,
                },
                "MNUPHL 5000-2000 m above ground 1-2 hour min fcst": {
                    "start_byte": 27515144,
                    "byte_size": 44804,
                },
                "MXUPHL 2000-0 m above ground 1-2 hour max fcst": {
                    "start_byte": 27559948,
                    "byte_size": 37429,
                },
                "MNUPHL 2000-0 m above ground 1-2 hour min fcst": {
                    "start_byte": 27597377,
                    "byte_size": 158855,
                },
                "MXUPHL 3000-0 m above ground 1-2 hour max fcst": {
                    "start_byte": 27756232,
                    "byte_size": 39301,
                },
                "MNUPHL 3000-0 m above ground 1-2 hour min fcst": {
                    "start_byte": 27795533,
                    "byte_size": 31453,
                },
                "RELV 2000-0 m above ground 1-2 hour max fcst": {
                    "start_byte": 27826986,
                    "byte_size": 2129165,
                },
                "RELV 1000-0 m above ground 1-2 hour max fcst": {
                    "start_byte": 29956151,
                    "byte_size": 2442756,
                },
                "HAIL entire atmosphere 1-2 hour max fcst": {
                    "start_byte": 32398907,
                    "byte_size": 117252,
                },
                "HAIL 0.1 sigma level 1-2 hour max fcst": {
                    "start_byte": 32516159,
                    "byte_size": 11774,
                },
                "HAIL surface 1-2 hour max fcst": {
                    "start_byte": 32527933,
                    "byte_size": 6102,
                },
                "TCOLG entire atmosphere (considered as a single layer) 1-2 hour max fcst": {
                    "start_byte": 32534035,
                    "byte_size": 24275,
                },
                "LTNGSD 1 m above ground 2 hour fcst": {
                    "start_byte": 32558310,
                    "byte_size": 2555,
                },
                "LTNGSD 2 m above ground 2 hour fcst": {
                    "start_byte": 32560865,
                    "byte_size": 30378,
                },
                "LTNG entire atmosphere 2 hour fcst": {
                    "start_byte": 32591243,
                    "byte_size": 17495,
                },
                "UGRD 80 m above ground 2 hour fcst": {
                    "start_byte": 32608738,
                    "byte_size": 1124667,
                },
                "VGRD 80 m above ground 2 hour fcst": {
                    "start_byte": 33733405,
                    "byte_size": 1111253,
                },
                "PRES surface 2 hour fcst": {
                    "start_byte": 34844658,
                    "byte_size": 1506358,
                },
                "HGT surface 2 hour fcst": {
                    "start_byte": 36351016,
                    "byte_size": 2153695,
                },
                "TMP surface 2 hour fcst": {
                    "start_byte": 38504711,
                    "byte_size": 1319785,
                },
                "ASNOW surface 0-2 hour acc fcst": {
                    "start_byte": 39824496,
                    "byte_size": 18411,
                },
                "MSTAV 0 m underground 2 hour fcst": {
                    "start_byte": 39842907,
                    "byte_size": 1483819,
                },
                "CNWAT surface 2 hour fcst": {
                    "start_byte": 41326726,
                    "byte_size": 79522,
                },
                "WEASD surface 2 hour fcst": {
                    "start_byte": 41406248,
                    "byte_size": 77438,
                },
                "SNOWC surface 2 hour fcst": {
                    "start_byte": 41483686,
                    "byte_size": 48644,
                },
                "SNOD surface 2 hour fcst": {
                    "start_byte": 41532330,
                    "byte_size": 63662,
                },
                "TMP 2 m above ground 2 hour fcst": {
                    "start_byte": 41595992,
                    "byte_size": 1183072,
                },
                "POT 2 m above ground 2 hour fcst": {
                    "start_byte": 42779064,
                    "byte_size": 1120758,
                },
                "SPFH 2 m above ground 2 hour fcst": {
                    "start_byte": 43899822,
                    "byte_size": 1501512,
                },
                "DPT 2 m above ground 2 hour fcst": {
                    "start_byte": 45401334,
                    "byte_size": 1207290,
                },
                "RH 2 m above ground 2 hour fcst": {
                    "start_byte": 46608624,
                    "byte_size": 1589727,
                },
                "MASSDEN 8 m above ground 2 hour fcst": {
                    "start_byte": 48198351,
                    "byte_size": 759771,
                },
                "UGRD 10 m above ground 2 hour fcst": {
                    "start_byte": 48958122,
                    "byte_size": 2381615,
                },
                "VGRD 10 m above ground 2 hour fcst": {
                    "start_byte": 51339737,
                    "byte_size": 2143472,
                },
                "WIND 10 m above ground 1-2 hour max fcst": {
                    "start_byte": 53483209,
                    "byte_size": 1180915,
                },
                "MAXUW 10 m above ground 1-2 hour max fcst": {
                    "start_byte": 54664124,
                    "byte_size": 1320277,
                },
                "MAXVW 10 m above ground 1-2 hour max fcst": {
                    "start_byte": 55984401,
                    "byte_size": 1280441,
                },
                "CPOFP surface 2 hour fcst": {
                    "start_byte": 57264842,
                    "byte_size": 66707,
                },
                "PRATE surface 2 hour fcst": {
                    "start_byte": 57331549,
                    "byte_size": 37139,
                },
                "APCP surface 0-2 hour acc fcst": {
                    "start_byte": 57368688,
                    "byte_size": 174138,
                },
                "WEASD surface 0-2 hour acc fcst": {
                    "start_byte": 57542826,
                    "byte_size": 21769,
                },
                "FROZR surface 0-2 hour acc fcst": {
                    "start_byte": 57564595,
                    "byte_size": 17432,
                },
                "FRZR surface 0-2 hour acc fcst": {
                    "start_byte": 57582027,
                    "byte_size": 18196,
                },
                "SSRUN surface 1-2 hour acc fcst": {
                    "start_byte": 57600223,
                    "byte_size": 7331,
                },
                "BGRUN surface 1-2 hour acc fcst": {
                    "start_byte": 57607554,
                    "byte_size": 5046,
                },
                "APCP surface 1-2 hour acc fcst": {
                    "start_byte": 57612600,
                    "byte_size": 268683,
                },
                "WEASD surface 1-2 hour acc fcst": {
                    "start_byte": 57881283,
                    "byte_size": 18815,
                },
                "FROZR surface 1-2 hour acc fcst": {
                    "start_byte": 57900098,
                    "byte_size": 13062,
                },
                "CSNOW surface 2 hour fcst": {
                    "start_byte": 57913160,
                    "byte_size": 2816,
                },
                "CICEP surface 2 hour fcst": {"start_byte": 57915976, "byte_size": 286},
                "CFRZR surface 2 hour fcst": {
                    "start_byte": 57916262,
                    "byte_size": 1051,
                },
                "CRAIN surface 2 hour fcst": {
                    "start_byte": 57917313,
                    "byte_size": 50618,
                },
                "SFCR surface 2 hour fcst": {
                    "start_byte": 57967931,
                    "byte_size": 1890747,
                },
                "FRICV surface 2 hour fcst": {
                    "start_byte": 59858678,
                    "byte_size": 1128687,
                },
                "SHTFL surface 2 hour fcst": {
                    "start_byte": 60987365,
                    "byte_size": 1402342,
                },
                "LHTFL surface 2 hour fcst": {
                    "start_byte": 62389707,
                    "byte_size": 1426731,
                },
                "VEG surface 2 hour fcst": {
                    "start_byte": 63816438,
                    "byte_size": 1471458,
                },
                "VEGMIN surface 2 hour fcst": {
                    "start_byte": 65287896,
                    "byte_size": 1116137,
                },
                "VEGMAX surface 2 hour fcst": {
                    "start_byte": 66404033,
                    "byte_size": 876934,
                },
                "LAI surface 2 hour fcst": {
                    "start_byte": 67280967,
                    "byte_size": 835225,
                },
                "GFLUX surface 2 hour fcst": {
                    "start_byte": 68116192,
                    "byte_size": 1086976,
                },
                "VGTYP surface 2 hour fcst": {
                    "start_byte": 69203168,
                    "byte_size": 781172,
                },
                "LFTX 500-1000 mb 2 hour fcst": {
                    "start_byte": 69984340,
                    "byte_size": 925972,
                },
                "CAPE surface 2 hour fcst": {
                    "start_byte": 70910312,
                    "byte_size": 435868,
                },
                "CIN surface 2 hour fcst": {
                    "start_byte": 71346180,
                    "byte_size": 263137,
                },
                "PWAT entire atmosphere (considered as a single layer) 2 hour fcst": {
                    "start_byte": 71609317,
                    "byte_size": 902960,
                },
                "AOTK entire atmosphere (considered as a single layer) 2 hour fcst": {
                    "start_byte": 72512277,
                    "byte_size": 1363000,
                },
                "COLMD entire atmosphere (considered as a single layer) 2 hour fcst": {
                    "start_byte": 73875277,
                    "byte_size": 969520,
                },
                "TCOLW entire atmosphere 2 hour fcst": {
                    "start_byte": 74844797,
                    "byte_size": 847844,
                },
                "TCOLI entire atmosphere 2 hour fcst": {
                    "start_byte": 75692641,
                    "byte_size": 834565,
                },
                "TCDC boundary layer cloud layer 2 hour fcst": {
                    "start_byte": 76527206,
                    "byte_size": 633076,
                },
                "LCDC low cloud layer 2 hour fcst": {
                    "start_byte": 77160282,
                    "byte_size": 738311,
                },
                "MCDC middle cloud layer 2 hour fcst": {
                    "start_byte": 77898593,
                    "byte_size": 229169,
                },
                "HCDC high cloud layer 2 hour fcst": {
                    "start_byte": 78127762,
                    "byte_size": 284768,
                },
                "TCDC entire atmosphere 2 hour fcst": {
                    "start_byte": 78412530,
                    "byte_size": 756186,
                },
                "HGT cloud ceiling 2 hour fcst": {
                    "start_byte": 79168716,
                    "byte_size": 1232296,
                },
                "HGT cloud base 2 hour fcst": {
                    "start_byte": 80401012,
                    "byte_size": 2061811,
                },
                "PRES cloud base 2 hour fcst": {
                    "start_byte": 82462823,
                    "byte_size": 937840,
                },
                "PRES cloud top 2 hour fcst": {
                    "start_byte": 83400663,
                    "byte_size": 595454,
                },
                "HGT cloud top 2 hour fcst": {
                    "start_byte": 83996117,
                    "byte_size": 1042625,
                },
                "ULWRF top of atmosphere 2 hour fcst": {
                    "start_byte": 85038742,
                    "byte_size": 1759535,
                },
                "DSWRF surface 2 hour fcst": {
                    "start_byte": 86798277,
                    "byte_size": 2330713,
                },
                "DLWRF surface 2 hour fcst": {
                    "start_byte": 89128990,
                    "byte_size": 1992864,
                },
                "USWRF surface 2 hour fcst": {
                    "start_byte": 91121854,
                    "byte_size": 1907173,
                },
                "ULWRF surface 2 hour fcst": {
                    "start_byte": 93029027,
                    "byte_size": 1655644,
                },
                "CFNSF surface 2 hour fcst": {
                    "start_byte": 94684671,
                    "byte_size": 5630,
                },
                "VBDSF surface 2 hour fcst": {
                    "start_byte": 94690301,
                    "byte_size": 2430525,
                },
                "VDDSF surface 2 hour fcst": {
                    "start_byte": 97120826,
                    "byte_size": 2423418,
                },
                "USWRF top of atmosphere 2 hour fcst": {
                    "start_byte": 99544244,
                    "byte_size": 2314835,
                },
                "HLCY 3000-0 m above ground 2 hour fcst": {
                    "start_byte": 101859079,
                    "byte_size": 1119254,
                },
                "HLCY 1000-0 m above ground 2 hour fcst": {
                    "start_byte": 102978333,
                    "byte_size": 1866160,
                },
                "USTM 0-6000 m above ground 2 hour fcst": {
                    "start_byte": 104844493,
                    "byte_size": 992875,
                },
                "VSTM 0-6000 m above ground 2 hour fcst": {
                    "start_byte": 105837368,
                    "byte_size": 941737,
                },
                "VUCSH 0-1000 m above ground 2 hour fcst": {
                    "start_byte": 106779105,
                    "byte_size": 2381615,
                },
                "VVCSH 0-1000 m above ground 2 hour fcst": {
                    "start_byte": 109160720,
                    "byte_size": 2381615,
                },
                "VUCSH 0-6000 m above ground 2 hour fcst": {
                    "start_byte": 111542335,
                    "byte_size": 2619757,
                },
                "VVCSH 0-6000 m above ground 2 hour fcst": {
                    "start_byte": 114162092,
                    "byte_size": 2619757,
                },
                "HGT 0C isotherm 2 hour fcst": {
                    "start_byte": 116781849,
                    "byte_size": 1841094,
                },
                "RH 0C isotherm 2 hour fcst": {
                    "start_byte": 118622943,
                    "byte_size": 684284,
                },
                "PRES 0C isotherm 2 hour fcst": {
                    "start_byte": 119307227,
                    "byte_size": 702926,
                },
                "HGT highest tropospheric freezing level 2 hour fcst": {
                    "start_byte": 120010153,
                    "byte_size": 708390,
                },
                "RH highest tropospheric freezing level 2 hour fcst": {
                    "start_byte": 120718543,
                    "byte_size": 682418,
                },
                "PRES highest tropospheric freezing level 2 hour fcst": {
                    "start_byte": 121400961,
                    "byte_size": 687721,
                },
                "HGT 263 K level 2 hour fcst": {
                    "start_byte": 122088682,
                    "byte_size": 662175,
                },
                "HGT 253 K level 2 hour fcst": {
                    "start_byte": 122750857,
                    "byte_size": 612556,
                },
                "4LFTX 180-0 mb above ground 2 hour fcst": {
                    "start_byte": 123363413,
                    "byte_size": 926833,
                },
                "CAPE 180-0 mb above ground 2 hour fcst": {
                    "start_byte": 124290246,
                    "byte_size": 461225,
                },
                "CIN 180-0 mb above ground 2 hour fcst": {
                    "start_byte": 124751471,
                    "byte_size": 555966,
                },
                "HPBL surface 2 hour fcst": {
                    "start_byte": 125307437,
                    "byte_size": 2893729,
                },
                "HGT level of adiabatic condensation from sfc 2 hour fcst": {
                    "start_byte": 128201166,
                    "byte_size": 2893195,
                },
                "CAPE 90-0 mb above ground 2 hour fcst": {
                    "start_byte": 131094361,
                    "byte_size": 358176,
                },
                "CIN 90-0 mb above ground 2 hour fcst": {
                    "start_byte": 131452537,
                    "byte_size": 482779,
                },
                "CAPE 255-0 mb above ground 2 hour fcst": {
                    "start_byte": 131935316,
                    "byte_size": 460558,
                },
                "CIN 255-0 mb above ground 2 hour fcst": {
                    "start_byte": 132395874,
                    "byte_size": 284924,
                },
                "HGT equilibrium level 2 hour fcst": {
                    "start_byte": 132680798,
                    "byte_size": 2140255,
                },
                "PLPL 255-0 mb above ground 2 hour fcst": {
                    "start_byte": 134821053,
                    "byte_size": 1188119,
                },
                "CAPE 0-3000 m above ground 2 hour fcst": {
                    "start_byte": 136009172,
                    "byte_size": 648499,
                },
                "HGT level of free convection 2 hour fcst": {
                    "start_byte": 136657671,
                    "byte_size": 2656787,
                },
                "EFHL surface 2 hour fcst": {
                    "start_byte": 139314458,
                    "byte_size": 874680,
                },
                "CANGLE 0-500 m above ground 2 hour fcst": {
                    "start_byte": 140189138,
                    "byte_size": 2091581,
                },
                "LAYTH 261 K level - 256 K level 2 hour fcst": {
                    "start_byte": 142280719,
                    "byte_size": 1285691,
                },
                "ESP 0-3000 m above ground 2 hour fcst": {
                    "start_byte": 143566410,
                    "byte_size": 549774,
                },
                "RHPW entire atmosphere 2 hour fcst": {
                    "start_byte": 144116184,
                    "byte_size": 1134322,
                },
                "LAND surface 2 hour fcst": {
                    "start_byte": 145250506,
                    "byte_size": 50465,
                },
                "ICEC surface 2 hour fcst": {"start_byte": 145300971, "byte_size": 418},
                "SBT123 top of atmosphere 2 hour fcst": {
                    "start_byte": 145301389,
                    "byte_size": 1383808,
                },
                "SBT124 top of atmosphere 2 hour fcst": {
                    "start_byte": 146685197,
                    "byte_size": 2346602,
                },
                "SBT113 top of atmosphere 2 hour fcst": {
                    "start_byte": 149031799,
                    "byte_size": 1273550,
                },
            },
        },
    },
    "properties": {
        "datetime": "2024-05-10T00:00:00Z",
        "cube:dimensions": {
            "latitude": {
                "axis": "y",
                "step": 0.025234760254957505,
                "type": "spatial",
                "extent": [21.12222222, 47.84583333],
                "reference_system": 4326,
                "unit": "degrees",
            },
            "longitude": {
                "axis": "x",
                "step": 0.006332530416898273,
                "type": "spatial",
                "extent": [-134.0, -80.0],
                "reference_system": 4326,
                "unit": "degrees",
            },
            "isobaricInhPa": {
                "type": "spatial",
                "extent": [1, 2, 3, 4, 5, 6, 7, 8, 9],
            },
            "time": {
                "type": "temporal",
                "extent": [
                    "2000-01-02T00:00:00Z",
                    "2000-01-02T00:00:00Z",
                ],
            },
        },
        "cube:variables": {
            "refc": {
                "type": "data",
                "unit": "",
                "description": "some description",
                "values": [],
                "dimensions": ["x", "y", "time"],
            }
        },
    },
}


@pytest.mark.parametrize(
    "items, spatial_extent, temporal_extent, properties, bands, crs, expected_dim_size",
    [
        (
            [GRIB2_ITEM],
            (-130.0, 22.0, -120.0, 30.0),
            (datetime(2024, 5, 10), datetime(2024, 5, 11)),
            None,
            ["SBT113 top of atmosphere 2 hour fcst"],
            4326,
            {
                "longitude": 177,
                "latitude": 344,
                DEFAULT_BANDS_DIMENSION: 1,
            },
        ),
        (
            [GRIB2_ITEM],
            (-130.0, 22.0, -120.0, 30.0),
            (datetime(2024, 5, 10), datetime(2024, 5, 11)),
            None,
            ["SBT113 top of atmosphere 2 hour fcst", "TMP 500 mb 2 hour fcst"],
            4326,
            {
                "longitude": 177,
                "latitude": 344,
                DEFAULT_BANDS_DIMENSION: 2,
            },
        ),
    ],
)
def test_load_items(
    items: List[Dict],
    spatial_extent: Tuple[float, float, float, float],
    temporal_extent: Tuple[datetime, datetime],
    properties: Optional[Dict[str, Any]],
    bands: List[str],
    crs: str,
    expected_dim_size: Dict[str, int],
):
    items = [Item.from_dict(i) for i in items]
    reader = Grib2FileReader(
        items=items,
        bbox=spatial_extent,
        temporal_extent=temporal_extent,
        bands=bands,
        properties=properties,
    )
    array = reader.load_items()
    assert isinstance(array, xr.DataArray)
    assert dict(array.sizes) == expected_dim_size
    assert array.rio.crs == CRS.from_epsg(crs)
    ds = array.to_dataset(dim=DEFAULT_BANDS_DIMENSION)
    path = TEST_DATA_ROOT / "test_convert_grib2_to_netcdf.nc"
    if Path(path).exists():
        Path(path).unlink()
    ds.to_netcdf(path=path, engine="netcdf4")  # type: ignore[call-overload]
    if Path(path).exists():
        Path(path).unlink()
