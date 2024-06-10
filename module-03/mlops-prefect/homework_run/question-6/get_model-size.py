import mlflow

# Get the model URI
model_uri = mlflow.get_run(mlflow.search_runs().iloc[0].run_id).info.artifact_uri + "/model"

# Get the model size in bytes
model_size_bytes = mlflow.get_model_size(model_uri)

print("Size of the model (in bytes):", model_size_bytes)
