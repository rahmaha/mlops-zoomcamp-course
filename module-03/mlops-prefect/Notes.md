## MLOps using Prefect

### Terminology 
 - *Orchestration API*: used by server to work with workflow metadata
 - *Database*: storess workflows metadata
 - *UI*: visualize workflow

#### Task, Flow, and Subflow

***Task***\
    *Definition*: A task is a single unit of work within a flow. It's an individual step in the workflow.\
    *Analogous to*: Individual operations or function calls within the main function or subfunctions.\
    *Role*: Perform specific units of work within a flow or subflow.\
    *Example*: "preprocessing data," "training the model," and "evaluating the model".\ 

***Flow***\
    *Definition*: A flow is a complete workflow or pipeline that orchestrates a series of tasks.\
    *Analogous to*: The main function in a Python script.\
    *Role*: Orchestrates the entire workflow, calling tasks and subflows as needed.\
    *Example*: Imagine you have a pipeline that preprocesses data, trains a model, and then evaluates it. This entire process is a flow.\

***Subflow***\
    *Definition*: A subflow is a flow that is called and executed within another flow. It acts as a task within the parent flow but can contain its own series of tasks.\
    *Analogous to*: A function within a Python script that can call other functions.\
    *Role*: Encapsulates a smaller part of the workflow, which can include multiple tasks and potentially other subflows.\
    *Example*: If you have a flow for preprocessing data that is complex enough to be managed separately, you can define it as a subflow. Then, you can call this subflow within the main training pipeline.\

***Code Example***

**Task**
```ruby
from prefect import task

@task
def preprocess_data():
    # Preprocessing code here
    return data

@task
def train_model(data):
    # Training code here
    return model

@task
def evaluate_model(model):
    # Evaluation code here
    return metrics
```
**Subflow and Flow**
```ruby
from prefect import flow

@flow
def data_preprocessing_flow():
    data = preprocess_data()
    return data

```

>**In this analogy**:
>
>    `main_flow()` is like the main function that orchestrates everything.
>
>    `data_preprocessing_flow()` is like a subfunction (subflow) that encapsulates part of the workflow.
>
>    `preprocess_data()`, train_model(), and evaluate_model() are like tasks that perform individual units of work.


