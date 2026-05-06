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

@app.get("/")
async def root():
    return {"status": "ok", "websocket": "ws://localhost:8000/ws"}

# ---------- WebSocket Endpoint ----------
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
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
        response = f"Echo: {message}"
        print(f"[SENT] {response}")
        await websocket.send_text(response)



# import socketio
# import uvicorn
# from fastapi import FastAPI

# sio = socketio.AsyncServer(
#     async_mode="asgi",
#     cors_allowed_origins="*",
#     transports=["polling", "websocket"],  # ✅ allow both
# )

# app = FastAPI()
# socket_app = socketio.ASGIApp(sio, socketio_path="socket.io")

# @app.get("/")
# async def root():
#     return {"status": "ok"}

# @sio.event
# async def connect(sid, environ):
#     print(f"✅ Connected: {sid}")

# @sio.event
# async def disconnect(sid):
#     print(f"❌ Disconnected: {sid}")

# @sio.event
# async def message(sid, data):
#     print(f"📨 Message: {data}")
#     await sio.emit("message", f"Echo: {data}", to=sid)

# if __name__ == "__main__":
#     uvicorn.run(socket_app, host="0.0.0.0", port=8000)