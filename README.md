# Deploying Models From AutoML to Algorithmia
API-centric way of deploying Scoring Code enabled AutoML models to Algorithmia

## Flow of operations:
    0. Kick off an AutoML project with a CSV dataset
    1. Get top N_TOP_MODELS from AutoML Leaderboard
    2. Create Model Packages from top learning models
    3. Get (or create) a Prediction Environment for Algorithmia
    4. Deploy all model packages on Algorithmia Prediction Environment
    5. Download scoring code+monitoring agent JAR from deployments
    6. Upload JARs to Algorithmia's Hosted Data Collection
    7. Create respective algorithms, with DR metadata in model manifests
    8. Test algorithms with sample inputs and publish
    9. Create 1 reserved slot for the published algorithm version


## Use Instructions
    - update the `config/datarobot.conf.yaml` file to reflect your datarobot project and workflow you wish to manage on Algorithmia.
    - update the `config/algorithmia.conf.yaml` file to reflect how your algorithmia configuration is setup, and to ensure you can deploy to the right user account, etc
    - create a virtual environment, or use one for this project in python (minimum python 3.7)
    - execute your deployment workflow by calling `python main_deploy.py`
    - if all goes well, the output of the workflow should indicate an algorithmia API endpoint
    - you can now make curl requests against this endpoint, and incorporate it into your business logic / production environments.