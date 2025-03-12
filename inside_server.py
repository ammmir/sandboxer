from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from jupyter_client.manager import AsyncKernelManager
import asyncio
import json
from io import BytesIO

app = FastAPI()

async def execute_code(code: str):
    km = AsyncKernelManager()
    await km.start_kernel()
    kc = km.client()
    kc.start_channels()
    await kc.wait_for_ready()

    msg_id = kc.execute(code)

    async def stream_results():
        try:
            while True:
                reply = await kc.get_iopub_msg()
                msg_type = reply["msg_type"]
                if msg_type == 'stream':
                    yield json.dumps({"text": reply['content']['text']}) + "\n"
                elif msg_type == 'display_data':
                    data = reply['content']['data']
                    if "image/png" in data:
                        yield json.dumps({"image": data["image/png"]}) + "\n"
                elif msg_type == "error":
                    traceback = "\n".join(reply['content']['traceback'])
                    yield json.dumps({"error": traceback}) + "\n"
                    break
                elif msg_type == "status" and reply["content"]["execution_state"] == "idle":
                    break
                else:
                    print(f"OTHER: {msg_type}", reply)
        except asyncio.CancelledError:
            print("CancelledError")
            pass
        finally:
            kc.stop_channels()
            await km.shutdown_kernel()

    return StreamingResponse(stream_results(), media_type="application/x-ndjson")

@app.post("/execute")
async def execute(request: dict):
    if "code" not in request:
        raise HTTPException(status_code=400, detail="Missing 'code' field")

    return await execute_code(request["code"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000) 