# from fastapi import FastAPI, WebSocket, WebSocketDisconnect

# app = FastAPI()


# @app.get("/")
# async def root():
#     return {"status": "ok", "websocket": "ws://localhost:8000/ws"}


# @app.websocket("/ws")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#     await websocket.send_text("Connected!")

#     try:
#         while True:
#             message = await websocket.receive_text()
#             await websocket.send_text(f"Echo: {message}")
#     except WebSocketDisconnect:
#         print("Client disconnected")


from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from asyncio import Queue
import asyncio

app = FastAPI()

connected_clients: list[WebSocket] = []

@app.get("/")
async def root():
    return {"status": "ok", "websocket": "ws://localhost:8000/ws"}

# ---------- WebSocket Endpoint ----------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    await websocket.send_text("Connected!")

    queue = Queue()

    # Run sender and receiver concurrently
    await asyncio.gather(
        receiver(websocket, queue),
        sender(websocket, queue),
    )


# ---------- Receiver ----------
async def receiver(websocket: WebSocket, queue: Queue):
    """Listens for incoming messages and puts them in a queue."""
    try:
        while True:
            message = await websocket.receive_text()
            print(f"[RECEIVED] {message}")
            await queue.put(message)
    except WebSocketDisconnect:
        print("[RECEIVER] Client disconnected")
        await queue.put(None)  # Signal sender to stop

# ---------- Sender ----------
async def sender(websocket: WebSocket, queue: Queue):
    """Reads from the queue and sends messages to the client."""
    while True:
        message = await queue.get()
        if message is None:
            break  # Stop signal received
        response = message
        print(f"[SENT] {response}")

        for client in connected_clients:
            try:
                await client.send_text(response)
            except:
                connected_clients.remove(websocket)