## MLOps using Prefect

### Terminology 
 - *Orchestration API*: used by server to work with workflow metadata
 - *Database*: storess workflows metadata
 - *UI*: visualize workflow

#### Task, Flow, and Subflow

***Task***

    **Definition**: A task is a single unit of work within a flow. It's an individual step in the workflow.
    *Analogous to*: Individual operations or function calls within the main function or subfunctions.
    *Role*: Perform specific units of work within a flow or subflow.
    *Example*: In the above pipeline, "preprocessing data," "training the model," and "evaluating the model" are tasks.

***Flow***

