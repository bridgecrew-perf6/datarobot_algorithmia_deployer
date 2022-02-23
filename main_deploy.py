import algorithmia_ops
import datarobot_ops

N_TOP_MODELS = 1

""" Flow of operations:
    #0. Kick off an AutoML project with a CSV dataset
    #1. Get top N_TOP_MODELS from AutoML Leaderboard
    #2. Create Model Packages from top learning models
    #3. Get (or create) a Prediction Environment for Algorithmia
    #4. Deploy all model packages on Algorithmia Prediction Environment
    #5. Download scoring code+monitoring agent JAR from deployments
    #6. Upload JARs to Algorithmia's Hosted Data Collection
    #7. Create respective algorithms, with DR metadata in model manifests
    #8. Test algorithms with sample inputs and publish
    #9. Create 1 reserved slot for the published algorithm version
"""
if __name__ == "__main__":
    algo_ops = algorithmia_ops.AlgorithmiaOps()
    dr_ops = datarobot_ops.DataRobotOps()

    #dr_ops.start_automl_scoring_code_only()
    top_models = dr_ops.get_top_models(N_TOP_MODELS)
    dr_ops.deploy_on_algorithmia_pred_env(top_models)
    dr_ops.download_depl_codegens_concurrently(top_models, include_agent=False)
    algo_ops.upload_codegens_concurrently(top_models)
    algo_ops.prep_algos_concurrently(
        top_models,
        mlops_api_token=dr_ops.api_token,
        test_inputs=[algo_ops.test_batch, algo_ops.test_rt],
    )

    for model in top_models:
        print(f"DataRobot model with deployment ID:{model.depl_id} is deployed at {model.published_endpoint} on Algorithmia")
