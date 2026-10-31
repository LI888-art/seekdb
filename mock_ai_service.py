from flask import Flask, request, jsonify

app = Flask(__name__)

# --- OpenAI 接口 ---
@app.route('/compatible-mode/v1/embeddings', methods=['POST'])
def openai_embeddings():
    req = request.json or {}
    dim = req.get("dimensions", 768) 
    inputs = req.get("input")
    
    data = []
    if isinstance(inputs, list):
        for i, text in enumerate(inputs):
            current_dim = 768 if text == "dimension-row" else dim
            data.append({"object": "embedding", "embedding": [0.1] * current_dim, "index": i})
    else:
        if inputs == "dimension-row":
            dim = 768
        data.append({"object": "embedding", "embedding": [0.1] * dim, "index": 0})
        
    print(f"[RECV] Embedding request, input count: {len(data)}, dim: {dim}", flush=True)
    return jsonify({"object": "list", "data": data, "model": "text-embedding-v1"})

@app.route('/compatible-mode/v1/chat/completions', methods=['POST'])
def openai_completions():
    print("[RECV] OpenAI Completion request", flush=True)
    res = {"choices": [{"message": {"role": "assistant", "content": "mock openai response"}}]}
    return jsonify(res)

# --- Ollama 原生接口 ---
@app.route('/api/generate', methods=['POST'])
def ollama_generate():
    print("[RECV] Ollama Generate request", flush=True)
    res = {"model": "qwen2.5", "response": "mock ollama response", "done": True}
    return jsonify(res)

@app.route('/api/embed', methods=['POST'])
def ollama_embed():
    print("[RECV] Ollama Embed request", flush=True)
    res = {"embeddings": [[0.1] * 768]}
    return jsonify(res)

# --- SiliconFlow / Rerank 接口 ---
@app.route('/v1/rerank', methods=['POST'])
def rerank():
    req = request.json or {}
    documents = req.get("documents", [])
    top_n = req.get("top_n", len(documents))
    print(f"[RECV] Rerank request, doc count: {len(documents)}, top_n: {top_n}", flush=True)
    
    results = []
    for i, doc in enumerate(documents):
        results.append({"index": i, "relevance_score": 0.9 - i * 0.1})
    if top_n is not None and top_n < len(results):
        results = results[:top_n]
        
    res = {"id": "mock-rerank-id", "results": results, "meta": {"tokens": {"input_tokens": 10, "output_tokens": 0}}}
    return jsonify(res)

if __name__ == '__main__':
    print("Mock AI Service (Flask) running on port 8000...", flush=True)
    app.run(host='0.0.0.0', port=8000, threaded=True)
