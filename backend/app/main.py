from fastapi import FastAPI

app = FastAPI(title="StreamSentinel")

@app.get("/health")
def health():
    return {"status": "ok"}