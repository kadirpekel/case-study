import asyncio
import random
from typing import List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Models Backend")

# We have 10 available models
MODELS = [f"model_{i}" for i in range(1, 11)]

class SolveRequest(BaseModel):
    prompt: str
    model: str

class BatchRequest(BaseModel):
    items: List[SolveRequest]

async def call_model_internal(prompt: str, model: str):
    """
    Simulates a single LLM model call.
    Latency: 1s to 2m (we'll use 1-10s for the lab to keep it practical, 
    but with a rare chance of 30s to simulate the outlier).
    """
    if model not in MODELS:
        raise HTTPException(status_code=404, detail="Model not found")
    
    # Simulating the latency mentioned in the case (1s to 2m)
    # Using slightly smaller values for development efficiency
    # Weighted choice: mostly fast, but occasionally very slow
    dice = random.random()
    if dice > 0.95:
        latency = random.uniform(20.0, 40.0) # The "slow" outlier
    else:
        latency = random.uniform(1.0, 5.0)
        
    await asyncio.sleep(latency)
    return {"answer": f"Response to '{prompt[:20]}...' from {model}", "model": model}



@app.post("/solve")
async def solve(request: SolveRequest):
    """Single prompt solving endpoint"""
    return await call_model_internal(request.prompt, request.model)

@app.post("/batch")
async def solve_batch(request: BatchRequest):
    """
    Batch solving endpoint.
    Requirement: "breaks batch requests into parallel model calls"
    Requirement: "returns the response only after all of the tasks have been completed"
    """
    if not request.items:
        return []
        
    # Parallel execution of model calls
    tasks = [call_model_internal(item.prompt, item.model) for item in request.items]
    return await asyncio.gather(*tasks)

@app.get("/health")
async def health():
    return {"status": "ok", "models_available": len(MODELS)}
