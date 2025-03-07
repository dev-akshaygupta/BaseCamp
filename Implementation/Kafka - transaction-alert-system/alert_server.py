import json
import redis
from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse

# Initialize FastAPI app
app = FastAPI()

# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

html = """
<!DOCTYPE html>
<html>
<head>
    <title>Suspicious Transaction Alerts</title>
</head>
<body>
    <h2>Live Alerts</h2>
    <ul id="alerts"></ul>
    <script>
        const ws = new WebSocket("ws://localhost:8000/ws");
        ws.onmessage = function(event) {
            let alertBox = document.getElementById("alerts");
            alertBox.innerHTML += "<p>" + event.data + "</p>";
        };
     </script>
</body>
</html>
"""

@app.get("/")
async def get():
    return HTMLResponse(html)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    pubsub = redis_client.pubsub()
    pubsub.subscribe("suspicious_alerts")

    try:
        for message in pubsub.listen():
            if message["type"] == "message":
                alert = json.loads(message["data"])
                await websocket.send_text(json.dumps(alert))
    except Exception as e:
        print(f"WebSocket error: {e}")
    
    finally:
        pubsub.unsubscribe("suspicious_alerts")
        await websocket.close()