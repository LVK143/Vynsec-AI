# main.py
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from datetime import datetime
import redis
import json
import asyncio
from typing import Optional, Dict, Any
import logging

# FastAPI app
app = FastAPI(title="AI SOC Backend")

# Redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
REDIS_QUEUE_KEY = "security_events"

# Data Models
class SecurityEvent(BaseModel):
    source: str
    source_ip: Optional[str] = None
    timestamp: str
    raw_message: str
    severity: Optional[str] = "Unknown"
    hostname: Optional[str] = None
    filename: Optional[str] = None

class AIAnalysisResult(BaseModel):
    event_id: str
    summary: str
    risk_score: float
    recommended_action: str
    confidence: float
    analyzed_at: str

@app.get("/")
async def root():
    return {"message": "AI SOC Backend Running"}

@app.post("/ingest")
async def ingest_event(event: SecurityEvent, background_tasks: BackgroundTasks):
    """Receive security events from agents/syslog"""
    try:
        # Add unique ID and processing timestamp
        event_dict = event.dict()
        event_dict["ingested_at"] = datetime.utcnow().isoformat() + "Z"
        event_dict["event_id"] = f"evt_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"
        
        # Queue for AI processing
        background_tasks.add_task(queue_event_for_analysis, event_dict)
        
        return {"status": "queued", "event_id": event_dict["event_id"]}
    
    except Exception as e:
        logging.error(f"Error ingesting event: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

def queue_event_for_analysis(event_dict: Dict[str, Any]):
    """Add event to Redis queue for AI processing"""
    try:
        redis_client.rpush(REDIS_QUEUE_KEY, json.dumps(event_dict))
        logging.info(f"Queued event {event_dict['event_id']} for analysis")
    except Exception as e:
        logging.error(f"Error queueing event: {e}")

@app.get("/queue/stats")
async def get_queue_stats():
    """Check queue status"""
    queue_length = redis_client.llen(REDIS_QUEUE_KEY)
    return {
        "queue_length": queue_length,
        "queue_name": REDIS_QUEUE_KEY,
        "status": "active" if queue_length < 1000 else "backlogged"
    }

# Background worker to process the queue
async def start_ai_worker():
    """Background task to process events from Redis queue"""
    while True:
        try:
            # Blocking pop from queue (wait up to 30 seconds)
            event_data = redis_client.blpop(REDIS_QUEUE_KEY, timeout=30)
            
            if event_data:
                _, event_json = event_data
                event = json.loads(event_json)
                await process_event_with_ai(event)
                
        except Exception as e:
            logging.error(f"Error in AI worker: {e}")
            await asyncio.sleep(5)  # Wait before retrying

async def process_event_with_ai(event: Dict[str, Any]):
    """Placeholder for AI processing (Week 3)"""
    print(f"🤖 [AI WOULD ANALYZE]: {event['event_id']}")
    print(f"   Message: {event['raw_message'][:100]}...")
    
    # TODO: Week 3 - Integrate LangChain + GPT-4 here
    # For now, simulate processing delay
    await asyncio.sleep(1)

# Start the background worker when the app starts
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(start_ai_worker())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
