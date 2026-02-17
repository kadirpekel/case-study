# 1. Current System Overview

Our system comprises several key components:

* ## PostgreSQL Database:
    The database contains a table with tasks that should be solved via a set of LLM models. Each task contains its own prompt and other attributes - e.g. status (solved/unsolved), answer for prompt if it is solved, etc. The table is filled regularly from different sources.
* ## Python/FastAPI Backend:
    The backend serves as an interface for requests to different LLM models. There is a limited number of available models (10). Backend has a “single” endpoint, for solving a single prompt and a “batch” endpoint, for solving multiple prompts at once. Under the hood, it breaks batch requests into parallel model calls. The time of solving a single prompt may vary from 1s to 2m. Batch endpoint returns the response only after all of the tasks have been completed.
* ## Airflow:
    Used to manage and orchestrate background processes that may make calls to the Models Backend.

# 2. Current Workflow

Here's how the system currently operates:

1. There is an Airflow DAG, that gets all new unsolved tasks from the database and breaks down those tasks into 20 equal batches.
2. The DAG spawns 20 mapped tasks (i.e. workers), each of which processes its own batch.
3. Each process sends a batch of 10 tasks to the Models Backend. After the response the process updates those 10 tasks in Postgres with the answers and “solved” status.
4. Then the process gets a new batch of 10 tasks.
5. Each of the workers has a timeout limit of 30 minutes. After all of the workers have solved their batch of tasks, or the timeout was reached, DAG restarts.

# 3. Desired System Outcome

We aim to achieve the following:

* ## Controlled traffic to each model:
    Ensure that we can specify a “target” traffic for each model that will be respected. The traffic value should be easily adjustable at any moment, if the requirements changed (e.g. available quota for model was increased).
* ## Solving speed improvement:
    Point out current process bottlenecks and implement a solution which not only controls the traffic, but also speeds up the whole process of task solving.

You are encouraged to propose and implement changes to the database schema or existing processes to enhance the system's functionality and efficiency. However, the basic requirement must be preserved: using an existing Models Backend to make model calls. This component is essential and cannot be omitted. But you can reorganize others as you want.

# Expected Deliverables:

We expect you to provide:

* ## Implementation Diagram:
    A detailed schema that outlines the proposed system architecture, including components and their interactions.
* ## Pseudocode for key/important parts:
    Pseudocode that demonstrates the logic and sequence of operations. It should be detailed enough to illustrate how the system will work, including algorithms and order of actions. Real code is not necessary, but the pseudocode should clearly convey the implementation strategy.