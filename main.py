from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio
import json

app = FastAPI()

# ✅ Global shared list of clients
connected_clients: list[WebSocket] = []

@app.get("/")
async def root():
    return {"status": "ok", "message": "WELCOME"}

# ✅ Broadcast to ALL clients
async def broadcast(message: str, sender: WebSocket):
    disconnected = []
    for client in connected_clients:
        try:
            await client.send_text(message)
        except Exception:
            disconnected.append(client)  # mark dead clients

    # Clean up dead clients
    for client in disconnected:
        if client in connected_clients:
            connected_clients.remove(client)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    print(f"[CONNECTED] {websocket.client} | Total: {len(connected_clients)}")

    try:
        await websocket.send_text("Connected!")
        while True:
            message = await websocket.receive_text()
            print(f"[RECEIVED] {message} from {websocket.client}")

            # ✅ Broadcast to everyone including ESP32
            await broadcast(message, websocket)

    except WebSocketDisconnect:
        print(f"[DISCONNECTED] {websocket.client}")
        if websocket in connected_clients:
            connected_clients.remove(websocket)