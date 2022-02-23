import datarobot as dr
from dralgo import DRAlgo
import requests
import json
import logging
import yaml
from pathlib import Path
import os.path
from concurrent.futures import ThreadPoolExecutor, as_completed


class DataRobotOps:
    def __init__(self, config_file="./config/datarobot.conf.yaml"):
        with open(config_file) as conf_file:
            config = yaml.safe_load(conf_file)
        self.api_token = config["apiToken"]
        self.api_address = config["apiAddress"]
        self.proj_name = config["projectName"]
        self.training_data_path = config["trainingDataPath"]
        self.target_feature = config["target"]
        self.dr_client = dr.Client(token=self.api_token, endpoint=self.api_address)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_token}",
                "content-type": "application/json",
            }
        )
        self.logger = logging.getLogger(__name__)

    def start_automl_scoring_code_only(self):
        proj = dr.Project.start(
            sourcedata=self.training_data_path,
            project_name=self.proj_name,
            target=self.target_feature,
            scoring_code_only=True,
        )
        proj.wait_for_autopilot()

    def get_top_models(self, n_top):
        project = dr.Project.list(search_params={"project_name": self.proj_name})[0]
        recommended_model = project.recommended_model()
        top_models = [recommended_model]
        self.logger.info(f"Top {n_top} models on the leaderboard: {top_models}")
        dralgos = []
        for model in top_models:
            dralgo = DRAlgo(
                proj_id=project.id,
                proj_name=self.proj_name,
                proj_file_name=project.file_name,
                proj_descr=project.project_description,
                model_id=model.id,
                model_type=model.model_type,
                prediction_threshold=model.prediction_threshold,
            )
            dralgos.append(dralgo)
        return dralgos

    def deploy_on_algorithmia_pred_env(self, dralgos):
        self._create_modelpkgs(dralgos)
        env_id = self._get_algorithmia_pred_env()

        get_depls = f"{self.api_address}/deployments"
        create_depl = f"{get_depls}/fromModelPackage"
        existing_depls = self.session.get(get_depls).json()["data"]

        for dralgo in dralgos:
            depl = next(
                (
                    x
                    for x in existing_depls
                    if x["modelPackage"]["id"] == dralgo.model_pkg_id
                    and x["predictionEnvironment"]["id"] == env_id
                ),
                None,
            )
            if depl:
                self.logger.info(
                    f"Found existing deployment for the model package id on env: {depl['modelPackage']['name']}"
                )
            else:
                depl_label = f"{dralgo.model_type} on Algorithmia"
                depl_body = {
                    "modelPackageId": dralgo.model_pkg_id,
                    "predictionEnvironmentId": env_id,
                    "label": depl_label,
                    "importance": "LOW",
                }
                depl = self.session.post(create_depl, data=json.dumps(depl_body)).json()
                self.logger.info(f"Created a new model deployment: {depl_label}")
            dralgo.depl_id = depl["id"]

    def download_depl_codegens_concurrently(self, dralgos, include_agent=True):
        with ThreadPoolExecutor(max_workers=5) as executor:
            wait_for = {
                executor.submit(self._download_depl_codegen, dra, include_agent): dra
                for dra in dralgos
            }
            for f in as_completed(wait_for):
                dra = wait_for[f]
                try:
                    _ = f.result()
                except Exception as e:
                    self.logger.error(
                        f"Got exception {e} when downloading codegen for: {dra.depl_id}"
                    )
                else:
                    self.logger.info(
                        f"Codegen package for {dra.depl_id} is at {dra.model_download_path}"
                    )

    def _download_depl_codegen(self, dralgo, include_agent):
        depl_id = dralgo.depl_id
        self.logger.info(f"Checking codegen for {depl_id}")
        download_path = os.path.join("/tmp", f"{depl_id}.jar")
        existing_codegen = Path(download_path)
        if not existing_codegen.is_file():
            self.logger.info(f"Downloading codegen for {depl_id}")
            deployment = dr.Deployment.get(deployment_id=depl_id)
            deployment.download_scoring_code(download_path, include_agent=include_agent)
        dralgo.model_download_path = download_path

    def get_depls_on_algorithmia_predenv(self):
        env_id = self._get_algorithmia_pred_env()
        get_depls = f"{self.api_address}/deployments"
        existing_depls = self.session.get(get_depls).json()["data"]
        algo_depls = list(
            filter(
                lambda x: (
                    x["predictionEnvironment"]["id"] == env_id
                    and x["status"] == "active"
                ),
                existing_depls,
            )
        )
        for depl in algo_depls:
            self.logger.info(f"Found {depl['label']} on Algorithmia PE")
        return [d["id"] for d in algo_depls]

    def _create_modelpkgs(self, dralgos):
        get_model_pkgs = f"{self.api_address}/modelPackages"
        create_model_pkg = f"{get_model_pkgs}/fromLearningModel"

        existing_pkgs = self.session.get(get_model_pkgs).json()["data"]
        for dralgo in dralgos:
            pkg = next(
                (x for x in existing_pkgs if x["modelId"] == dralgo.model_id), None
            )
            if pkg:
                self.logger.info(f"Found existing model package: {pkg['name']}")
            else:
                model_pkg_body = {"modelId": dralgo.model_id}
                pkg = self.session.post(
                    create_model_pkg, data=json.dumps(model_pkg_body)
                ).json()
                self.logger.info(f"Created a new model package: {pkg['name']}")
            dralgo.model_pkg_id = pkg["id"]
            dralgo.model_pkg_name = pkg["name"]

    def _get_algorithmia_pred_env(self):
        algorithmia_env_name = "AlgorithmiaPredEnv"
        pred_envs = f"{self.api_address}/predictionEnvironments"
        params = {"search": algorithmia_env_name}
        found_envs = self.session.get(pred_envs, params=params).json()["data"]
        if len(found_envs):
            algorithmia_env = found_envs[0]
        else:
            env_body = {
                "name": algorithmia_env_name,
                "description": "Algorithmia Prediction Environment",
                "platform": "other",
            }
            algorithmia_env = self.session.post(
                pred_envs, data=json.dumps(env_body)
            ).json()
        return algorithmia_env["id"]
