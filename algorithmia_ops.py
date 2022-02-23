import Algorithmia
from retry import retry
import os, shutil
import json
import hashlib
import requests
from time import time
import logging
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
import repo_ops


class AlgorithmiaOps:
    def __init__(self, config_file="./config/algorithmia.conf.yaml"):
        self.logger = logging.getLogger(__name__)
        with open(config_file) as conf_file:
            config = yaml.safe_load(conf_file)
        self.api_key = config["apiKey"]
        self.admin_api_key = config["adminApiKey"]
        self.username = config["username"]
        self.datasource = config["dataSource"]
        self.artifact_path = config["artifactBasePath"]
        self.algo_api_addr = config["apiAddress"]
        self.algo_ca_cert = config.get("caCert", None)
        self.test_batch = config['testBatchPayload']
        self.test_rt = config['testRealtimePayload']
        self.algo_client = Algorithmia.client(self.api_key, self.algo_api_addr, ca_cert=self.algo_ca_cert)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Simple {self.api_key}",
                "content-type": "application/json",
            }
        )

        self.admin_session = requests.Session()
        self.admin_session.headers.update(
            {
                "Authorization": f"Simple {self.admin_api_key}",
                "content-type": "application/json",
            }
        )
        self.algo_prefix = "DR_"

    def delete_algo_and_res(self, dralgo):
        if dralgo.algo_name == "":
            dralgo.algo_name = f"{self.algo_prefix}{dralgo.depl_id}"

        self.logger.info(f"Deleting algorithm: {dralgo.algo_name}...")
        try:
            algo_url = (
                f"{self.algo_api_addr}/v1/algorithms/{self.username}/{dralgo.algo_name}"
            )
            self.session.delete(algo_url)

            self._manage_algo_res(dralgo, delete_on_slots_mismatch=False, create=False)
            self.logger.info(
                f"Deleted {dralgo.algo_name} and its reservations successfully."
            )
        except Exception as e:
            self.logger.error(
                f"Got exception: {e} while deleting the algorithm or the reservation."
            )

    def prep_algos_concurrently(self, dralgos, mlops_api_token, test_inputs=None):
        user_algos = self._get_user_algos()
        with ThreadPoolExecutor(max_workers=5) as executor:
            wait_for = {
                executor.submit(
                    self._prep_algo_for_depl,
                    user_algos,
                    dra,
                    mlops_api_token,
                    test_inputs,
                ): dra
                for dra in dralgos
            }
            for f in as_completed(wait_for):
                dra = wait_for[f]
                try:
                    _ = f.result()
                except Exception as e:
                    self.logger.error(f"Got exception {e} when creating algo: {dra}.")
                else:
                    self.logger.info(
                        f"Completed create/update of algo {dra.algo_name}."
                    )

    def _prep_algo_for_depl(
        self, user_algos, dralgo, mlops_api_token=None, test_inputs=None
    ):
        algo_for_depl = f"{self.algo_prefix}{dralgo.depl_id}"
        matching_algo = next((x for x in user_algos if x[1] == algo_for_depl), None)
        if matching_algo:
            self.logger.info(f"Found an existing algo: {algo_for_depl}")
            self._clone_or_pull_algorithm_repo(algo_for_depl)
            dralgo.algo_id = matching_algo[0]
            dralgo.algo_name = matching_algo[1]
        else:
            self.logger.info(f"Creating an algo from scratch: {algo_for_depl}...")
            algo_path = os.path.join(self.artifact_path, algo_for_depl)
            if os.path.exists(algo_path) and os.path.isdir(algo_path):
                self.logger.info(
                    "Found existing dir for algo repo. Removing and clean pulling..."
                )
                shutil.rmtree(algo_path)
            tagline = (
                f"{dralgo.model_pkg_name}"
            )
            algo_id = self._create_algorithm(
                algo_for_depl, label=algo_for_depl, tagline=tagline
            )
            dralgo.algo_id = algo_id
            dralgo.algo_name = algo_for_depl
        self._add_algo_secret(dralgo.algo_id, "DATAROBOT_API_TOKEN", mlops_api_token)
        self._update_manifest_and_src(dralgo)
        self._test_and_publish_algo(dralgo, test_inputs)
        self._manage_algo_res(dralgo, delete_on_slots_mismatch=True, create=True)

    @retry(Exception, tries=10, delay=1)
    def _test_and_publish_algo(self, dralgo, test_inputs=None):
        try:
            namespace = f"{self.username}/{dralgo.algo_name}"
            algo = self.algo_client.algo(namespace)
            latest_algo_hash = algo.info().version_info.git_hash
            latest_build = self.algo_client.algo(
                "{}/{}".format(namespace, latest_algo_hash)
            )
            latest_build.set_options(timeout=60, stdout=False)

            sample_input = None
            if test_inputs:
                for test_input in test_inputs:
                    latest_build.pipe(test_input)
                sample_input = json.dumps(test_inputs[-1])

            publish_params = {
                "version_info": {
                    "sample_input": sample_input,
                    "version_type": "minor",
                    "release_notes": "Released programmatically from the Algorithmia Plugin",
                },
                "details": {"label": dralgo.algo_name},
                "settings": {"algorithm_callability": "private"},
            }

            response = algo.publish(
                settings=publish_params["settings"],
                version_info=publish_params["version_info"],
                details=publish_params["details"],
            )
            latest_version = response["version_info"]["semantic_version"]
            dralgo.published_endpoint = (
                f"{self.algo_api_addr}/v1/algo/{namespace}/{latest_version}"
            )
            self.logger.info(
                f"Published {latest_version} version of {dralgo.algo_name}. Enjoy!"
            )
        except Exception as e:
            self.logger.error(
                f"Got exception: {e} while testing and publishing {dralgo}"
            )

    def upload_codegens_concurrently(self, dralgos):
        with ThreadPoolExecutor(max_workers=5) as executor:
            wait_for = {
                executor.submit(self._upload_codegen, dra): dra for dra in dralgos
            }
            for f in as_completed(wait_for):
                dra = wait_for[f]
                try:
                    _ = f.result()
                except Exception as e:
                    self.logger.error(
                        f"Got exception {e} when uploading codegen for: {dra}."
                    )
                else:
                    self.logger.info(
                        f"Uploaded codegen for {dra.depl_id} is as {dra.model_upload_path}."
                    )

    def _upload_codegen(self, dralgo):
        download_path = dralgo.model_download_path
        filename = os.path.basename(download_path)
        abs_path = os.path.abspath(download_path)
        remote_path = f"data://{self.username}/{self.datasource}/{filename}"
        self.logger.info(f"Checking for model {filename} in Algorithmia Data Source...")
        if not self.algo_client.file(remote_path).exists():
            self.algo_client.file(remote_path).putFile(abs_path)
        dralgo.model_upload_path = remote_path

    @retry(Exception, tries=10, delay=1)
    def test_and_publish_algo(self, dralgo, test_inputs=None):
        try:
            namespace = f"{self.username}/{dralgo.algo_name}"
            algo = self.algo_client.algo(namespace)
            latest_algo_hash = algo.info().version_info.git_hash
            latest_build = self.algo_client.algo(
                "{}/{}".format(namespace, latest_algo_hash)
            )
            latest_build.set_options(timeout=60, stdout=False)

            sample_input = None
            if test_inputs:
                for test_input in test_inputs:
                    latest_build.pipe(test_input)
                sample_input = json.dumps(test_inputs[-1])

            publish_params = {
                "version_info": {
                    "sample_input": sample_input,
                    "version_type": "minor",
                    "release_notes": "Released programmatically from the Algorithmia Plugin",
                },
                "details": {"label": dralgo.algo_name},
                "settings": {"algorithm_callability": "private"},
            }

            response = algo.publish(
                settings=publish_params["settings"],
                version_info=publish_params["version_info"],
                details=publish_params["details"],
            )
            latest_version = response["version_info"]["semantic_version"]
            dralgo.published_endpoint = (
                f"{self.algo_api_addr}/v1/algo/{namespace}/{latest_version}"
            )
            self.logger.info(
                f"Published {latest_version} version of {dralgo.algo_name}. Enjoy!"
            )
        except Exception as e:
            self.logger.error(
                f"Got exception: {e} while testing and publishing {dralgo}"
            )

    def _create_algorithm(
        self, algo_name, label, tagline, env_display_name="Python 3.7 + H2O - beta"
    ):
        algo_env_id = self._get_algo_env_id_by_name(env_display_name)
        details = {"label": label, "tagline": tagline}
        settings = {
            "algorithm_environment": algo_env_id,
            "source_visibility": "closed",
            "license": "apl",
            "network_access": "full",
            "pipeline_enabled": True,
        }

        algo_namespace = f"{self.username}/{algo_name}"
        algo = self.algo_client.algo(algo_namespace)
        algo_id = algo.create(details=details, settings=settings).id
        self.logger.info(f"{algo_name} algorithm created")
        self._clone_or_pull_algorithm_repo(algo_name)
        return algo_id

    def _get_user_algos(self):
        get_algos = f"{self.algo_api_addr}/v1/users/{self.username}/algorithms"
        response = self.session.get(get_algos).json()
        return [
            (algo["id"], algo["name"])
            for algo in response
            if algo["name"].startswith(self.algo_prefix)
        ]

    def algo_for_depl(self, depl_id):
        algo_name = f"{self.algo_prefix}{depl_id}"
        namespace = f"{self.username}/{algo_name}"
        algo = f"{self.algo_api_addr}/v1/algorithms/{namespace}"
        response = self.session.get(algo).json()
        if "error" not in response:
            latest_version = response["version_info"]["semantic_version"]
            pred_url = f"{self.algo_api_addr}/v1/algo/{namespace}/{latest_version}"
            return pred_url
        return None

    def _manage_algo_res(self, dralgo, delete_on_slots_mismatch=False, create=False):
        try:
            self.logger.info(
                f"Checking existing algo reservations for {dralgo.algo_name}..."
            )
            get_res = f"{self.algo_api_addr}/v1/admin/reservations"
            reservations = self.admin_session.get(get_res).json()
            for res in reservations:
                if res["algoname"] == dralgo.algo_name:
                    self.logger.info(f"Found a reservation for {dralgo.algo_name}.")
                    should_delete = True
                    # If we want to delete only on a mismatch of slots, and the slots DO match, then we should not delete
                    if (
                        delete_on_slots_mismatch
                        and res["num_slots"] == dralgo.num_of_res
                    ):
                        should_delete = False
                        self.logger.info(
                            f"Existing reservation slots are the same as the newly requested slots. Will not change algo reservations for {dralgo.algo_name}"
                        )

                    if should_delete:
                        delete_res = f"{get_res}/{res['reservation_id']}"
                        response = self.admin_session.delete(delete_res)
                        if response.status_code == 200:
                            self.logger.info(
                                f"Deleted existing reservation for {dralgo.num_of_res}."
                            )
                        else:
                            self.logger.error(
                                f"Could not delete existing reservation for {dralgo.algo_name}. Still resuming operations..."
                            )
                    break

            if create:
                self.logger.info(f"Creating algo reservation for {dralgo.algo_name}...")
                res_body = {
                    "username": self.username,
                    "algoname": dralgo.algo_name,
                    "calling_user": self.username,
                    "version_type": "LATEST_PRIVATE",
                    "num_slots": int(dralgo.num_of_res),
                }
                response = self.admin_session.post(get_res, data=json.dumps(res_body))
                if response.status_code == 200:
                    self.logger.info(
                        f"Created {dralgo.num_of_res} reservations to algo: {dralgo.algo_name}"
                    )
                else:
                    self.logger.error(
                        f"Received non-success from create algo res: {response.json()}"
                    )
        except Exception as e:
            self.logger.error(f"Got exception: {e} while creating algo reservation.")

    def delete_codegen(self, filename):
        self.logger.info(f"Deleting remote model file {filename} from Algorithmia...")
        try:
            self.algo_client.file(
                f"data://{self.username}/{self.datasource}/{filename}"
            ).delete()
            self.logger.info(f"Deleted {filename} from Algorithmia.")
        except Exception as e:
            self.logger.error(
                f"Got exception: {e} while deleting codegen file {filename} from Algorithmia."
            )

    def _add_algo_secret(self, algo_id, secret_name, secret_value):
        self.logger.info("Adding DataRobot MLOps token as an algorithm secret...")
        try:
            create_secret = f"{self.algo_api_addr}/v1/algorithms/{algo_id}/secrets"
            secret_body = {
                "secret_key": secret_name,
                "description": f"{secret_name} added programmatically",
                "short_name": secret_name,
                "secret_value": secret_value,
                "owner_type": "algorithm",
                "owner_id": algo_id,
            }
            self.session.post(create_secret, data=json.dumps(secret_body)).json()
            self.logger.info(f"Added {secret_name} as an algorithm secret")
        except Exception as e:
            self.logger.error(f"Got exception: {e} while adding the algorithm secret.")

    def _clone_or_pull_algorithm_repo(self, algo_name):
        repo, repo_path = repo_ops.clone_or_pull_repo(
            self.api_key,
            self.username,
            algo_name,
            self.algo_api_addr,
            self.artifact_path,
        )
        return repo, repo_path

    def _update_manifest_and_src(self, dralgo):
        self.logger.info(
            "Updating model manifest with DR metadata, and freezing the manifest..."
        )
        required_files = dict(dralgo.__dict__)
        required_files[
            "name"
        ] = f"proj-{required_files['proj_id']}_model{required_files['model_id']}"
        required_files["source_uri"] = required_files.pop("model_upload_path")
        required_files["fail_on_tamper"] = True
        remove_fields = ["model_download_path", "algo_name", "published_endpoint"]
        [required_files.pop(x) for x in remove_fields]

        manifest = {"required_files": [required_files], "optional_files": []}

        repo_path = f"{self.artifact_path}/{dralgo.algo_name}"
        manifest_path = f"{repo_path}/model_manifest.json"
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=4)

        manifest_freeze = manifest
        md5_checksum = AlgorithmiaOps._md5_for_file(dralgo.model_download_path)

        manifest_freeze["required_files"][0]["md5_checksum"] = md5_checksum
        manifest_freeze["timestamp"] = str(time())
        lock_md5_checksum = AlgorithmiaOps._md5_for_str(str(manifest_freeze))
        manifest_freeze["lock_checksum"] = lock_md5_checksum

        manifest_freeze_path = (
            f"{self.artifact_path}/{dralgo.algo_name}/model_manifest.json.freeze"
        )
        with open(manifest_freeze_path, "w") as f:
            json.dump(manifest_freeze, f, indent=4)

        AlgorithmiaOps._write_algo_code(repo_path, dralgo.algo_name)
        repo_ops.push_repo(repo_path)

    def _get_algo_env_id_by_name(self, env_display_name):
        envs = self._get_algo_environments()
        for env in envs:
            if str.lower(env_display_name) == str.lower(env["display_name"]):
                return env["id"]
        return None

    def _get_algo_environments(self):
        response = self.session.get(
            f"{self.algo_api_addr}/webapi/v1/algorithm-environments/languages/python3/environments"
        )
        return response.json()["environments"]

    @staticmethod
    def _write_algo_code(repo_path, algo_name):
        algo_src = f"{repo_path}/src/{algo_name}.py"
        with open(algo_src, "w") as f:
            f.write(
                """import Algorithmia
import csv
import subprocess
import os
import time

def load(modelData):
    modelData["model_path"] = (list(modelData.models.values())[0]).file_path
    modelData["dr_token"] = os.environ["DATAROBOT_API_TOKEN"]
    return modelData

def apply(input, modelData):
    model_path = modelData["model_path"]
    dr_token = modelData["dr_token"]

    if input.startswith("data://"):
        input_csv = modelData.client.file(input).getFile().name
    else:
        input_csv = "/tmp/input.csv"
        reader = csv.reader(input.split("\\n"), delimiter=",")
        with open(input_csv, "w") as f:
            writer = csv.writer(f, delimiter=",")
            for row in reader:
                writer.writerow(row)

    start = time.time()
    cmd = f"java -jar {model_path} csv --input={input_csv} --output=-"
    p = subprocess.check_output(cmd, shell=True)
    pred_duration = round(time.time() - start)
    return {
        "predictions": p.decode("ascii"), 
        "duration": pred_duration, 
        "model_metadata": modelData.manifest_data["required_files"][0]
    }

client = Algorithmia.client()
algorithm = Algorithmia.ADK(apply, load, client)
algorithm.init()
"""
            )

    @staticmethod
    def _md5_for_file(fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return str(hash_md5.hexdigest())

    @staticmethod
    def _md5_for_str(content):
        hash_md5 = hashlib.md5()
        hash_md5.update(content.encode())
        return str(hash_md5.hexdigest())
