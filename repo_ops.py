import urllib.parse
from git import Git, Repo, remote
import os
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Progress(remote.RemoteProgress):
    def line_dropped(self, line):
        print(line)

    def update(self, *args):
        print(self._cur_line)


def _git_host_from_api(algo_api_addr):
    url = urllib.parse.urlparse(algo_api_addr).netloc
    url = url[4:] if url.startswith("www.") else url
    url = url[4:] if url.startswith("api.") else url
    git_host = f"git.{url}"
    logger.info(f"Using {git_host} as the algorithm repo host")
    return git_host


def clone_or_pull_repo(api_key, username, algo_name, algo_api_addr, workspace):
    repo_path = "{}/{}".format(workspace, algo_name)
    p = Progress()
    if not os.path.exists(repo_path):
        encoded_api_key = urllib.parse.quote_plus(api_key)
        git_host = _git_host_from_api(algo_api_addr)
        algo_repo = f"https://{username}:{encoded_api_key}@{git_host}/git/{username}/{algo_name}.git"
        repo = Repo.clone_from(algo_repo, repo_path, progress=p)
    else:
        repo = Repo(repo_path)
        origin = repo.remote(name="origin")
        repo.git.reset("--hard")
        origin.pull(progress=p)
    return repo, repo_path


def push_repo(repo_path):
    repo = Repo(repo_path)
    repo.git.add(".")
    repo.index.commit("Updated algorithm files")
    p = Progress()
    repo.remote(name="origin").push(progress=p)
