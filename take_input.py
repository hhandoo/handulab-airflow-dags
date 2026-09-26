from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from datetime import datetime


with DAG(
    dag_id="user_input_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,

    params={
        "name": Param(
            default="",
            type="string",
            title="Name",
            description="Enter your name",
        ),
        "age": Param(
            default=18,
            type="integer",
            title="Age",
            minimum=1,
            maximum=120,
        ),
        "environment": Param(
            default="dev",
            type="string",
            title="Environment",
            enum=["dev", "staging", "prod"],
        ),
        "email": Param(
            default="",
            type="string",
            title="Email",
            description="Enter your email address",
        ),
        "comments": Param(
            default="",
            type="string",
            title="Comments",
            description="Additional comments",
        ),
    },
) as dag:

    @task
    def process_input(**context):
        params = context["params"]

        name = params["name"]
        age = params["age"]
        environment = params["environment"]
        email = params["email"]
        comments = params["comments"]

        print(f"Name: {name}")
        print(f"Age: {age}")
        print(f"Environment: {environment}")
        print(f"Email: {email}")
        print(f"Comments: {comments}")

        return {
            "name": name,
            "age": age,
            "environment": environment,
            "email": email,
            "comments": comments,
        }

    process_input()