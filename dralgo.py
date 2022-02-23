from dataclasses import dataclass
from typing import Optional


@dataclass
class DRAlgo:
    proj_id: Optional[str] = ""
    proj_name: Optional[str] = ""
    model_id: Optional[str] = ""
    proj_descr: Optional[str] = ""
    proj_file_name: Optional[str] = ""
    model_type: Optional[str] = ""
    model_pkg_id: Optional[str] = ""
    model_pkg_name: Optional[str] = ""
    depl_id: Optional[str] = ""
    prediction_threshold: Optional[str] = ""
    algo_name: Optional[str] = ""
    algo_id: Optional[str] = ""
    model_upload_path: Optional[str] = ""
    model_download_path: Optional[str] = ""
    published_endpoint: Optional[str] = ""
    num_of_res: Optional[int] = 1
