from langgraph.func import entrypoint, task
from langgraph.checkpoint.memory import InMemorySaver
import uuid
import random

# -----------------------------
# Task 1: external payment call
# -----------------------------
@task
def charge_customer(order_id: str, amount: float) -> dict:
    # Simulate a side effect: external payment provider call
    # In a real system, this would call Stripe / PSP / bank API
    payment_id = f"pay_{uuid.uuid4().hex[:8]}"
    return {
        "payment_id": payment_id,
        "order_id": order_id,
        "amount": amount,
        "status": "charged"
    }

# -----------------------------
# Task 2: database write
# -----------------------------
@task
def save_order(order_id: str, payment_id: str) -> dict:
    # Simulate a DB write
    return {
        "db_record_id": f"db_{order_id}",
        "order_id": order_id,
        "payment_id": payment_id,
        "saved": True
    }

# -----------------------------
# Task 3: side effect that may fail
# -----------------------------
@task
def send_confirmation(email: str, order_id: str) -> dict:
    # Simulate a flaky external email service
    if random.random() < 0.5:
        raise RuntimeError("Temporary email provider outage")
    return {
        "email": email,
        "order_id": order_id,
        "sent": True
    }

# -----------------------------
# Workflow entrypoint
# -----------------------------
@entrypoint(checkpointer=InMemorySaver())
def order_workflow(inputs: dict) -> dict:
    '''
        @entrypoint is equivalent to creating a node and attaching it to START node.
        Part of langgraph functional api
    '''
    order_id = inputs["order_id"]
    amount = inputs["amount"]
    email = inputs["email"]

    payment = charge_customer(order_id, amount).result()
    saved = save_order(order_id, payment["payment_id"]).result()
    email_result = send_confirmation(email, order_id).result()

    return {
        "order_id": order_id,
        "payment": payment,
        "saved": saved,
        "email": email_result,
        "status": "completed"
    }

config = {
    "configurable": {
        "thread_id": "order-1001"
    }
}

result = order_workflow.invoke(
    {
        "order_id": "1001",
        "amount": 250.0,
        "email": "customer@example.com"
    },
    config=config
)