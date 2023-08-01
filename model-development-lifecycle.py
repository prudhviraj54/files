# Databricks notebook source
# MAGIC %md
# MAGIC # Model Development
# MAGIC
# MAGIC This notebook will go over model development lifecycle and create a project that will help showcase the following:
# MAGIC
# MAGIC 1. Create modular and individually testable code.
# MAGIC 2. MLFlow for experiment tracking and iteration & performance tuning.
# MAGIC 3. Databricks Widgets + Parameters for testing out different combinations of code quickly.
# MAGIC 4. Databricks Experiments (powered by MLFlow)
# MAGIC 5. Databricks Dashboards (optional)
# MAGIC
# MAGIC
# MAGIC This notebook will be broken down into the neccesary steps either by individual files or by functions. In this case, I will be creating just functions in the notebook. Steps are as follows: ingest -> transform -> split -> train -> evaluate -> orchestration
# MAGIC
# MAGIC
# MAGIC Note: I will just use iris dataset to showcase but any other datasets can be used.
# MAGIC
# MAGIC
# MAGIC ## Challenges
# MAGIC 1. ADLS Artifact Storage
# MAGIC 2. mlflow local connect to databricks backend

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest
# MAGIC
# MAGIC Data Ingestion process varies project to project and whether the data is available to you curated or you will need to get the data out of a system using REST call or connecting to a database. The connector may change however the rest of the process will remain same. 

# COMMAND ----------

import pandas as pd
import sklearn as sk

def ingest_iris_data() -> pd.DataFrame:
    # Note: Since I'll just be using iris dataset, I will just pull the data from sklearn. 
    # Other sources can be used as well and parameterized to fetch portions of data as needed. 
    frame = sk.datasets.load_iris(as_frame=True)
    df = pd.DataFrame(data = frame.data, columns=frame.feature_names)
    df['target'] = frame.target
    return df

# COMMAND ----------

# datapath = ""
# credentials = {}

# def ingest_xyz(datapath: str, credentials: dict) -> pd.DataFrame:
#     spark.read.csv(a)

# COMMAND ----------

# MAGIC %md 
# MAGIC Functions can easily be individually tested and are predictable hence it is good to have seperations in difference stages of model development and can easily be expanded on if needed.
# MAGIC
# MAGIC Below is likely a test scenario that could be implemented. 

# COMMAND ----------

iris_df = ingest_iris_data()

# Expecting 4 features and 1 target column
assert len(iris_df.columns) == 5

# Expecting certain features to be in the dataset
assert "sepal length (cm)" in iris_df.columns and "sepal width (cm)" in iris_df.columns

# COMMAND ----------

# MAGIC %md 
# MAGIC It is also important to store data at every step to keep track of data lineage and data drift if any. One way to keep track of it is by keeping the artifact for each run or by using formats such as delta table which can be updated and histories are maintained. 

# COMMAND ----------

# import os
# import os.path as path

# # Path can be abfss path as well however since pandas doesn't support abfss path, use spark.
# project_path = "/dbfs/FileStore/model-development-lifecycle"
# iris_path = f"{project_path}/injest/iris.csv"

# if not path.exists(path.dirname(iris_path)): os.makedirs(path.dirname(iris_path))
# iris_df.to_csv(iris_path, index=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Transformation 
# MAGIC
# MAGIC Given our dataset(s) can be unclean or not properly organized. It is going to be vital to apply certain transformation(s) to whip the data in good shape. It is a good idea to think of transformations as functions and be generic. These operations can be as simple as renaming a column or as complex as doing some compute based on other columns or exploding the data.

# COMMAND ----------

def column_name_case(columns: list) -> dict:
    return { c: to_kebab_case(c) for c in columns}

def to_kebab_case(val: str) -> str:
    delimeter = '_'
    space = ' '

    special_chars = ['/', '\\', '[', ']', '(', ')', '.']
    for char in special_chars:
        val = val.replace(char, '')
    
    words = val.split(space)

    kebab_case_val = delimeter.join(word.lower() for word in words)
    return kebab_case_val


def apply_transformations(df: pd.DataFrame) -> pd.DataFrame:
    columns = column_name_case(df.columns)
    df.rename(columns=columns, inplace=True)
    return df

# COMMAND ----------

# MAGIC %md 
# MAGIC Again for testing, Individually testable code will be consistent and can be reused. TDD can be used to iterate on each piece individually which will improve the overall code.

# COMMAND ----------

# func: to_kebab_case
assert to_kebab_case("This is a test.") == "this_is_a_test"
assert to_kebab_case(iris_df.columns[0]) == "sepal_length_cm"

# func: column_name_case
assert column_name_case(iris_df) == {'sepal length (cm)': 'sepal_length_cm',
 'sepal width (cm)': 'sepal_width_cm',
 'petal length (cm)': 'petal_length_cm',
 'petal width (cm)': 'petal_width_cm',
 'target': 'target'
}

transformed_df = apply_transformations(iris_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Storing the clean and sorted data as a dataset can help us start from where we left off last time.

# COMMAND ----------

# transformed_path = f"{project_path}/transformed/iris.csv"

# if not path.exists(path.dirname(transformed_path)): os.makedirs(path.dirname(transformed_path))
# transformed_df.to_csv(transformed_path, index=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Split
# MAGIC
# MAGIC The way data is split and the sizing of it can also impact the model results. These splits are critical to reproducing the results of a model training. Having the training data change from run to run can cause the accuracy of model to be falsely inflated or deflated.

# COMMAND ----------

from random import randint
from sklearn.model_selection import train_test_split

def split(df: pd.DataFrame, test_size: float = 0.3, seed: int = randint(1, 10000)):
    # Split the DataFrame into training and testing sets
    train_df, test_df = train_test_split(df, test_size=test_size, random_state=seed)
    return train_df, test_df

# COMMAND ----------

# Since the amount of data being sent to create the model can impact training results, It can be parameterized so we can tune it 
test_size = 0.3

train, test = split(transformed_df, test_size=test_size)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Data can be profiled for records, if needed. 

# COMMAND ----------

print("Training Data Shape", train.shape)
print("Testing Data Shape", test.shape)

# COMMAND ----------

# MAGIC %md 
# MAGIC Store the training and testing dataset to track experiments with their training and testing data

# COMMAND ----------

# train_path = f"{project_path}/split/train.csv"
# test_path = f"{project_path}/split/test.csv"

# if not path.exists(path.dirname(train_path)): os.makedirs(path.dirname(train_path))
# train.to_csv(train_path, index=False)

# if not path.exists(path.dirname(test_path)): os.makedirs(path.dirname(test_path))
# test.to_csv(test_path, index=False)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Train 
# MAGIC
# MAGIC This step is where we can try out different things and have vastly different results. There are numerous models out there that can be used to augment and improve our model. It is important to keep the params from this step. 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 1. Automatic Model Selection

# COMMAND ----------

import lazypredict.Supervised as lsu

features = ['sepal_length_cm', 'sepal_width_cm', 'petal_length_cm', 'petal_width_cm']
target = ['target']

d_inputs = {
    "X_train": train[features],
    "X_test": test[features],
    "y_train": train[target],
    "y_test": test[target]
}

clf = lsu.LazyClassifier()
models, predictions = clf.fit(**d_inputs)
print(models)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Training using the best model

# COMMAND ----------

from sklearn.svm import SVC

def train(X_train, y_train):
    model = SVC(kernel='rbf')
    model.fit(X_train, y_train)
    return model

# COMMAND ----------

svc = train(d_inputs["X_train"], d_inputs["y_train"])

# COMMAND ----------

# MAGIC %md 
# MAGIC Again storing the model as a pickle 

# COMMAND ----------

# import joblib

# model_path = f"{project_path}/model/iris_svc.joblib"
# if not path.exists(path.dirname(model_path)): os.makedirs(path.dirname(model_path))

# joblib.dump(svc, model_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Evaluation 
# MAGIC
# MAGIC Given that the model is built, We can get the neccesary scores and metrics and track them for future improvements and enhancements. Model can be loaded from filesystem like below:
# MAGIC
# MAGIC ```python3
# MAGIC import joblib
# MAGIC
# MAGIC model_path = f"{project_path}/model/iris_svc.joblib"
# MAGIC model = joblib.load(model_path)
# MAGIC
# MAGIC predictions = svc_loaded.predict(...)
# MAGIC ```
# MAGIC
# MAGIC However I will just be using it directly here.

# COMMAND ----------

from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, classification_report, confusion_matrix

def evaluate(model, X_test, y_test):
    y_pred = model.predict(X_test)

    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, average='weighted')
    recall = recall_score(y_test, y_pred, average='weighted')
    f1 = f1_score(y_test, y_pred, average='weighted')
    # More or different checks can be added here.

    return accuracy, precision, recall, f1

# COMMAND ----------

X_test = d_inputs["X_test"]
y_test = d_inputs["y_test"]

accuracy, precision, recall, f1 = evaluate(svc, X_test, y_test)

# Reporting Accuracy
print("Accuracy:", accuracy)
print("Precision:", precision)
print("Recall:", recall)
print("F1 Score:", f1)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Orchestration
# MAGIC
# MAGIC Given the above model deployment steps, We can create pipeline to run anywhere all the necessary base packages are installed. The following code usually resides either in the same package rest of the code or can be completely independent and brings in all the neccesary modular code from a library or codebase.

# COMMAND ----------

# MAGIC %md
# MAGIC ### MLFLow Automation
# MAGIC
# MAGIC MLFlow allows tracking and managing experiments and their parameters, metrics, artifacts and executions. Your MLFlow Project could just be a single script or could be a multi-file and complex project.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Complex Projects

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC There are couple of files neccesary for getting a complex project working and configured. This approach can also be used for simpler projects.
# MAGIC
# MAGIC **NOTE:** This is only needed if you will be running the project using the mlflow CLI or via a MLFlow pipeline. 

# COMMAND ----------

# DBTITLE 1,MLProject File
# MAGIC %md
# MAGIC
# MAGIC This file usually resides at `{PROJECT_ROOT}/MLProject`. It acts as an orchestrator file that tells mlflow CLI on what to do when we use the `mlflow run` command. 
# MAGIC
# MAGIC <!-- %writefile ${project_path}/MLProject  -->
# MAGIC ```yaml
# MAGIC name: 'model-development-lifecycle'
# MAGIC conda_env: envrionment.yaml
# MAGIC entry_points:
# MAGIC   ingest:
# MAGIC     parameters:
# MAGIC       output_dir: path
# MAGIC     command: 'python ingest.py {output_dir}'
# MAGIC   transform:
# MAGIC     parameters:
# MAGIC       dataset_file: path
# MAGIC     command: 'python transform.py {dataset_file}'
# MAGIC   split:
# MAGIC     parameters: 
# MAGIC       dataset_file: path
# MAGIC       output_dir: path
# MAGIC       test_size: {type: float, default: 0.3}
# MAGIC     command: 'python split.py {dataset_file} {output_dir} --test-size={test_size}'
# MAGIC   train:
# MAGIC     parameters:
# MAGIC       data_dir: path
# MAGIC       output_dir: path
# MAGIC     command: 'python train.py {data_dir} {output_dir}'
# MAGIC   # Main can be used to orchestrate the entire project. main.py file is like the glue file that puts all the individual pieces together and can run the whole project steps.
# MAGIC   main:
# MAGIC     parameters:
# MAGIC       project_dir: path
# MAGIC       test_size: {type: float, default: 0.3}
# MAGIC     command: 'python main.py {project_dir} {test_size}'
# MAGIC ```
# MAGIC
# MAGIC **NOTE:** When creating an entrypoint per script, you are expected to have main block on each of those entrypoint supporing that operations so they can be run individually if needed

# COMMAND ----------

# DBTITLE 1,ingest.py
# MAGIC %md
# MAGIC
# MAGIC This file will reside at `{PROJECT_ROOT}/ingest.py`. It defines our ingestion process and can be executed individually. 
# MAGIC
# MAGIC
# MAGIC ```py
# MAGIC import os
# MAGIC import mlflow
# MAGIC import logging
# MAGIC import pandas as pd
# MAGIC import sklearn as sk
# MAGIC import os.path as path
# MAGIC
# MAGIC def ingest_iris_data() -> pd.DataFrame:
# MAGIC   # Note: Since I'll just be using iris dataset, I will just pull the data from sklearn. 
# MAGIC   # Other sources can be used as well and parameterized to fetch portions of data as needed. 
# MAGIC   frame = sk.datasets.load_iris(as_frame=True)
# MAGIC   df = pd.DataFrame(data = frame.data, columns=frame.feature_names)
# MAGIC   df['target'] = frame.target
# MAGIC   return df
# MAGIC
# MAGIC if __name__ == "__main__":
# MAGIC   # Setup Logger
# MAGIC   logging.basicConfig(level=logging.INFO)
# MAGIC   log = logging.getLogger(__name__)
# MAGIC   
# MAGIC   with mlflow.start_run() as run:
# MAGIC     # MLFlow tracks experiment with runs! each run has a unique id and is used to fetch data for the run and can also be used to compare runs. 
# MAGIC     log.info("Active run_id: {}".format(run.info.run_id))
# MAGIC
# MAGIC     # Getting the user entered arguments
# MAGIC     try:
# MAGIC       [_, project_dir] = sys.argv
# MAGIC     except ValueError as v:
# MAGIC       log.error("Invalid or missing arguments. Please provide valid <project_dir> arguments")
# MAGIC
# MAGIC     iris_df = ingest_iris_data()
# MAGIC     iris_csv = f"{project_dir}/ingest/iris.csv"
# MAGIC     os.makedirs(path.dirname(iris_csv), exist_ok=True)
# MAGIC     iris_df.to_csv(iris_csv, index = False)
# MAGIC ```
# MAGIC

# COMMAND ----------

# DBTITLE 1,main.py
# MAGIC %md
# MAGIC
# MAGIC This file is the main workflow file `{PROJECT_ROOT}/main.py` 
# MAGIC
# MAGIC ```py
# MAGIC # import MLFlow for mlflow tracking
# MAGIC import mlflow 
# MAGIC import logging
# MAGIC import sys
# MAGIC import os
# MAGIC import os.path as path
# MAGIC import joblib
# MAGIC
# MAGIC # importing my custom modules
# MAGIC import ingest
# MAGIC import transform
# MAGIC import split
# MAGIC import train
# MAGIC import evaluate
# MAGIC
# MAGIC if __name__ == "__main__":
# MAGIC   # Setup Logger
# MAGIC   log = logging.getLogger(__name__)
# MAGIC   
# MAGIC   with mlflow.start_run() as run:
# MAGIC     # MLFlow tracks experiment with runs! each run has a unique id and is used to fetch data for the run and can also be used to compare runs. 
# MAGIC     log.info("Active run_id: {}".format(run.info.run_id))
# MAGIC     
# MAGIC     model_name = "iris"
# MAGIC     # Getting the user entered arguments
# MAGIC     try:
# MAGIC       [_, project_dir, test_size] = sys.argv
# MAGIC     except ValueError as v:
# MAGIC       log.error("Invalid or missing arguments. Please provide valid <project_dir> and <test_size> arguments")
# MAGIC
# MAGIC     # Logging user-entered params
# MAGIC     mlflow.log_params({'test_size': test_size, 'project_dir': project_dir})
# MAGIC
# MAGIC     # Starting the ingestion process
# MAGIC     iris_df = ingest.ingest_iris_data()
# MAGIC
# MAGIC     # Write iris to filesystem  
# MAGIC     iris_path = f"{project_dir}/injest/iris.csv"
# MAGIC     os.makedirs(path.dirname(iris_path), exist_ok=True)
# MAGIC     iris_df.to_csv(iris_path, index=False)
# MAGIC
# MAGIC     # Log Original Artifact
# MAGIC     mlflow.log_artifact(iris_path, artifact_path="injest")
# MAGIC
# MAGIC     # Transform
# MAGIC     transformed_df = transform.apply_transformations(iris_df)
# MAGIC
# MAGIC     # Write transformed to filesystem  
# MAGIC     transformed_path = f"{project_dir}/transform/iris.csv"
# MAGIC     os.makedirs(path.dirname(transformed_path), exist_ok=True)
# MAGIC     transformed_df.to_csv(transformed_path, index=False)
# MAGIC
# MAGIC     # Log Transformed Artifact
# MAGIC     mlflow.log_artifact(transformed_path, artifact_path="transformed")
# MAGIC
# MAGIC     # Split Train & Test Data
# MAGIC     train, test = split.split(transformed_df, test_size=float(test_size))
# MAGIC     
# MAGIC     # Write split data to filesystem  
# MAGIC     train_path = f"{project_dir}/split/train.csv"
# MAGIC     test_path = f"{project_dir}/split/test.csv"
# MAGIC     os.makedirs(path.dirname(train_path), exist_ok=True)
# MAGIC     os.makedirs(path.dirname(test_path), exist_ok=True)
# MAGIC     train.to_csv(train_path, index=False)
# MAGIC     test.to_csv(test_path, index=False)
# MAGIC
# MAGIC     # Log train test split
# MAGIC     mlflow.log_artifact(train_path, artifact_path="split")
# MAGIC     mlflow.log_artifact(test_path, artifact_path="split")
# MAGIC
# MAGIC     # Log Data Profile
# MAGIC     profile = {
# MAGIC       "train": { "rows": train.shape[0], "columns": train.shape[1] },
# MAGIC       "test": { "rows": test.shape[0], "columns": test.shape[1] }
# MAGIC     }
# MAGIC     mlflow.log_dict(profile, 'data_profile.json')
# MAGIC
# MAGIC     # Train Model
# MAGIC     d_inputs = {
# MAGIC         "X_train": train[features],
# MAGIC         "X_test": test[features],
# MAGIC         "y_train": train[target],
# MAGIC         "y_test": test[target]
# MAGIC     }
# MAGIC     model = train.train(d_inputs["X_train"], d_inputs["y_train"])
# MAGIC
# MAGIC     # Write modelfile to filesystem
# MAGIC     model_path = f"{project_dir}/model/iris_svc.joblib"
# MAGIC     os.makedirs(path.dirname(model_path), exist_ok=True)
# MAGIC     joblib.dump(svc, model_path)
# MAGIC
# MAGIC     # Log Model
# MAGIC     mlflow.sklearn.log_model(model, "model", registered_model_name=model_name)
# MAGIC
# MAGIC     # Evaluate Model
# MAGIC     accuracy, precision, recall, f1 = evaluate.evaluate(model, d_inputs["X_test"], d_inputs["y_test"])
# MAGIC     mlflow.log_metric("accuracy", accuracy)
# MAGIC     mlflow.log_metric("precision", precision)
# MAGIC     mlflow.log_metric("recall", recall)
# MAGIC     mlflow.log_metric("f1", f1)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Assuming all the other scripts are in place and files created like above, your repo will look something like this. 
# MAGIC
# MAGIC <pre>
# MAGIC .
# MAGIC ├── MLProject
# MAGIC ├── environment.yaml
# MAGIC ├── evaluate.py
# MAGIC ├── ingest.py
# MAGIC ├── main.py
# MAGIC ├── split.py
# MAGIC ├── train.py
# MAGIC └── transform.py
# MAGIC </pre>
# MAGIC
# MAGIC Your project can be more complex and can have a different structure as well and have a lot more files but the basics will remain. The complexity of the project can be managed by using more packages and organizing them for reusablity. 
# MAGIC
# MAGIC
# MAGIC ##### Local Run
# MAGIC
# MAGIC Locally The project can simply be run by issuing the following command in the project root folder.
# MAGIC
# MAGIC ```sh
# MAGIC # The parameter can be added if needed or the defaults configured in MLProject file will be used.
# MAGIC mlflow run -p project_dir="/path/to/dir" -p test_size=0.3 .
# MAGIC ```
# MAGIC
# MAGIC If we want to run individual step or process, we can do that too. Consider below example for running ingest, or train step. 
# MAGIC
# MAGIC ```sh
# MAGIC
# MAGIC # For Ingestion Step
# MAGIC mlflow run -e ingest -p output_dir="/path/to/dir" .
# MAGIC
# MAGIC # For Training Step
# MAGIC mlflow run -e train -p data_dir="/path/to/dir" -p output_dir="/path/to/dir" .
# MAGIC
# MAGIC
# MAGIC # Note: The above commands will do the exact same thing as the following commands
# MAGIC
# MAGIC # Ingest
# MAGIC python ingest.py "/path/to/dir"
# MAGIC
# MAGIC # Training
# MAGIC python train.py "/path/to/dir" "/path/to/dir"
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC ##### Databricks Run
# MAGIC
# MAGIC Since we want to run this on databricks, the process will be the same. We will be configuring the workflow on github_repo and calling main.py and pass the arguments as neccesary. `MLProject` file will not be used when configuring the workflow and can be skipped however it is good documentation for creating the workflow and the paramters and file to be executed is documented.
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Simple Projects (Single file like this notebook)

# COMMAND ----------

import mlflow 
import logging
import sys
import os
import os.path as path
import joblib

# COMMAND ----------

def run_workflow(project_dir: str, test_size: float, model_name: str):
    # Setup Logger
    log = logging.getLogger(__name__)
    with mlflow.start_run() as run:
        # MLFlow tracks experiment with runs! each run has a unique id and is used to fetch data for the run and can also be used to compare runs. 
        log.info("Active run_id: {}".format(run.info.run_id))

        # Logging user-entered params
        mlflow.log_params({'test_size': test_size, 'project_dir': project_dir})

        # Starting the ingestion process
        iris_df = ingest_iris_data()

        # Write iris to filesystem  
        iris_path = f"{project_dir}/injest/iris.csv"
        os.makedirs(path.dirname(iris_path), exist_ok=True)
        iris_df.to_csv(iris_path, index=False)

        # Log Original Artifact
        mlflow.log_artifact(iris_path, artifact_path="injest")

        # Transform
        transformed_df = apply_transformations(iris_df)

        # Write transformed to filesystem  
        transformed_path = f"{project_dir}/transform/iris.csv"
        os.makedirs(path.dirname(transformed_path), exist_ok=True)
        transformed_df.to_csv(transformed_path, index=False)

        # Log Transformed Artifact
        mlflow.log_artifact(transformed_path, artifact_path="transformed")

        # Split Train & Test Data
        train_df, test_df = split(transformed_df, test_size=float(test_size))
        
        # Write split data to filesystem  
        train_path = f"{project_dir}/split/train.csv"
        test_path = f"{project_dir}/split/test.csv"
        os.makedirs(path.dirname(train_path), exist_ok=True)
        os.makedirs(path.dirname(test_path), exist_ok=True)
        train_df.to_csv(train_path, index=False)
        test_df.to_csv(test_path, index=False)

        # Log train test split
        mlflow.log_artifact(train_path, artifact_path="split")
        mlflow.log_artifact(test_path, artifact_path="split")

        # Log Data Profile
        profile = {
        "train": { "rows": train_df.shape[0], "columns": train_df.shape[1] },
        "test": { "rows": test_df.shape[0], "columns": test_df.shape[1] }
        }
        mlflow.log_dict(profile, 'data_profile.json')

        # Train Model
        model = train(train_df[features], train_df[target])

        # Write modelfile to filesystem
        model_path = f"{project_dir}/model/iris_svc.joblib"
        os.makedirs(path.dirname(model_path), exist_ok=True)
        joblib.dump(svc, model_path)

        # Log Model
        mlflow.sklearn.log_model(model, "model", registered_model_name=model_name)

        # Evaluate Model
        accuracy, precision, recall, f1 = evaluate(model, test_df[features], test_df[target])
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("f1", f1)

# COMMAND ----------

# For running on local and as a script 
# Uncomment below for creating a run file for local

# if __name__ == "__main__":
#     # Getting the user entered arguments
    # try:
    #   [_, project_dir, test_size] = sys.argv
    # except ValueError as v:
    #   log.error("Invalid or missing arguments. Please provide valid <project_dir> and <test_size> arguments")
    # model_name = "iris"

#     run_workflow(project_dir=project_dir, test_size=float(test_size), model_name=model_name)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The above example can run anywhere where there are all dependencies

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Databricks Workflow Automation

# COMMAND ----------

# MAGIC %md 
# MAGIC Databricks workflow can also be used to trigger the MLFlow files and the parameters can be set using the parameters.
# MAGIC
# MAGIC The widgets can be configued so we can change a value and see immediate change here in notebook and for picking up values from workflow run. 

# COMMAND ----------

import numpy as np

# Setup required widgets with default values for this notebook. (Only needed for notebooks)
# dbutils.widgets.text(name="project_dir", defaultValue="/dbfs/FileStore/model-development-lifecycle", label="project_dir")
# dbutils.widgets.text(name="model_name", defaultValue="iris", label="model_name")
# dbutils.widgets.dropdown(name="test_size", defaultValue="0.30", choices=[str("%.2f" % i) for i in np.linspace(0,1,100)], label="test_size")

# COMMAND ----------

# For running on Databricks as script
# Uncomment below for creating a run file for databricks

if __name__ == "__main__":
    # Getting the user entered arguments
    project_dir = dbutils.widgets.get(name="project_dir")
    model_name = dbutils.widgets.get(name="model_name")
    try:
      test_size = float(dbutils.widgets.get(name="test_size"))
    except Exception as e:
      log.error("test_size entered is invalid")
      exit()

    #mlflow.set_experiment("/Users/aggaani@mfcgd.com/learning-environments/model-development-lifecycle-assets/model-development-lifecycle")
    run_workflow(project_dir=project_dir, test_size=test_size, model_name=model_name)

# COMMAND ----------


