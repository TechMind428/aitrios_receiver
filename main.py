from fastapi import FastAPI, Request, status, BackgroundTasks
import logging
import traceback
import json
import os
import time
from datetime import datetime
from pathlib import Path
import aiofiles
import asyncio
from Desilialize import DeserializeUtil

# アプリケーション設定
class Config:
    # 保存先ディレクトリ
    IMAGE_DIR = Path("./image")
    META_DIR = Path("./meta")
    LOG_DIR = Path("./logs")
    
    # パフォーマンス設定
    WORKERS = 4  # Uvicornワーカー数
    
    # ログ設定
    LOG_LEVEL = logging.INFO
    LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
    
    # サーバー設定
    HOST = "0.0.0.0"
    PORT = 8080

# FastAPIアプリケーション初期化
app = FastAPI(title="AITRIOS Data Receiver")

# ディレクトリ構造を確保
def ensure_directories():
    for directory in [Config.IMAGE_DIR, Config.META_DIR, Config.LOG_DIR]:
        directory.mkdir(parents=True, exist_ok=True)

# ロギング設定
def setup_logging():
    log_file = Config.LOG_DIR / f"receiver_{datetime.now().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=Config.LOG_LEVEL,
        format=Config.LOG_FORMAT,
        handlers=[
            logging.StreamHandler(),  # コンソールへの出力
            logging.FileHandler(log_file)  # ファイルへの出力
        ]
    )
    
    # サードパーティライブラリのログレベルを調整
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("fastapi").setLevel(logging.WARNING)

# 初期化関数
@app.on_event("startup")
async def startup_event():
    ensure_directories()
    setup_logging()
    logging.info(f"AITRIOS Data Receiver starting up on {Config.HOST}:{Config.PORT} with {Config.WORKERS} workers")

# 処理中ファイルのカウンター（統計用）
processing_stats = {
    "images_received": 0,
    "meta_received": 0,
    "errors": 0,
    "start_time": time.time()
}

# 非同期ファイル保存（画像）
async def save_image_file(path: str, content: bytes) -> str:
    """画像ファイルを指定されたパスに保存する非同期関数"""
    try:
        # パスを解析して、ディレクトリとファイル名を分離
        path_parts = path.split('/')
        filename = path_parts[-1]
        subdirs = path_parts[:-1] if len(path_parts) > 1 else []
        
        # 保存先ディレクトリを構築
        file_dir = Config.IMAGE_DIR
        for subdir in subdirs:
            file_dir = file_dir / subdir
        
        # ディレクトリを作成
        file_dir.mkdir(parents=True, exist_ok=True)
        
        # ファイルパス
        file_path = file_dir / filename
        
        # 非同期ファイル書き込み
        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(content)
        
        processing_stats["images_received"] += 1
        return str(file_path)
    except Exception as e:
        processing_stats["errors"] += 1
        logging.error(f"Error saving image file {path}: {str(e)}")
        raise

# 非同期ファイル保存（メタデータ）
async def save_meta_file(path: str, content: dict) -> str:
    """メタデータをJSONファイルとして指定されたパスに保存する非同期関数"""
    try:
        # パスを解析して、ディレクトリとファイル名を分離
        path_parts = path.split('/')
        filename = path_parts[-1]
        subdirs = path_parts[:-1] if len(path_parts) > 1 else []
        
        # 保存先ディレクトリを構築
        file_dir = Config.META_DIR
        for subdir in subdirs:
            file_dir = file_dir / subdir
        
        # ディレクトリを作成
        file_dir.mkdir(parents=True, exist_ok=True)
        
        # ファイル名からtxtを取り除き、jsonに変更
        if filename.endswith('.txt'):
            filename = filename[:-4] + '.json'
        elif not filename.endswith('.json'):
            filename = filename + '.json'
            
        file_path = file_dir / filename
        
        # 非同期ファイル書き込み - JSONを文字列に変換
        json_str = json.dumps(content, ensure_ascii=False, indent=None)
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
            await f.write(json_str)
        
        processing_stats["meta_received"] += 1
        return str(file_path)
    except Exception as e:
        processing_stats["errors"] += 1
        logging.error(f"Error saving meta file {path}: {str(e)}")
        raise

# AITRIOSからの画像受信エンドポイント - 可変パス対応
@app.put("/image/{path:path}")
async def update_image(path: str, request: Request):
    start_time = time.time()
    try:
        content = await request.body()
        
        # デバイスIDの抽出（ログ用）
        device_id = "unknown"
        path_parts = path.split('/')
        if len(path_parts) > 1:
            # パスの最初の部分をデバイスIDとみなす
            device_id = path_parts[0]
        else:
            # ヘッダーやクエリパラメータから取得を試みる
            device_id = request.headers.get("X-Device-ID", device_id)
            if 'DeviceID' in request.query_params:
                device_id = request.query_params['DeviceID']
        
        # 非同期でファイル保存
        file_path = await save_image_file(path, content)
        
        process_time = time.time() - start_time
        logging.info(f"Image saved: {path} to {file_path} in {process_time:.3f}s [Device: {device_id}]")
        
        return {
            "status": status.HTTP_200_OK,
            "file_path": file_path,
            "process_time_ms": int(process_time * 1000)
        }
    except Exception as e:
        process_time = time.time() - start_time
        logging.error(f"Error handling image update for {path}: {str(e)}")
        traceback.print_exc()
        return {
            "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
            "error": str(e),
            "process_time_ms": int(process_time * 1000)
        }

# AITRIOSからのメタデータ受信エンドポイント - 可変パス対応
@app.put("/meta/{path:path}")
async def update_inference_result(path: str, request: Request):
    start_time = time.time()
    try:
        content = await request.body()
        contentj = json.loads(content)
        
        # デバイスIDの取得（ログ用）
        device_id = contentj.get("DeviceID", "unknown")
        
        # デシリアライズユーティリティを使用
        deserializeutil = DeserializeUtil()
        
        # 推論結果の処理
        inferences = contentj.get("Inferences", [])
        if inferences:
            inferenceresult = inferences[0].get("O", "")
            deserialize_data = deserializeutil.get_deserialize_data(inferenceresult)
            
            # デシリアライズ結果をcontentjに追加
            contentj["DeserializedData"] = deserialize_data
        
        # 非同期でファイル保存
        file_path = await save_meta_file(path, contentj)
        
        process_time = time.time() - start_time
        logging.info(f"Meta saved: {path} to {file_path} in {process_time:.3f}s [Device: {device_id}]")
        
        return {
            "status": status.HTTP_200_OK,
            "file_path": file_path,
            "process_time_ms": int(process_time * 1000)
        }
    except Exception as e:
        process_time = time.time() - start_time
        logging.error(f"Error handling meta update for {path}: {str(e)}")
        traceback.print_exc()
        return {
            "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
            "error": str(e),
            "process_time_ms": int(process_time * 1000)
        }

# HTTPとHTTPSの両方に対応するためのリダイレクト（オプション）
@app.get("/redirect-https")
async def redirect_https(request: Request):
    https_url = str(request.url).replace("http://", "https://")
    return {"redirect": https_url}

# サーバーステータスエンドポイント
@app.get("/status")
async def get_status():
    uptime = time.time() - processing_stats["start_time"]
    hours, remainder = divmod(uptime, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    # ディスク使用状況の計算
    image_size = sum(f.stat().st_size for f in Config.IMAGE_DIR.glob('**/*') if f.is_file())
    meta_size = sum(f.stat().st_size for f in Config.META_DIR.glob('**/*') if f.is_file())
    
    return {
        "status": "running",
        "uptime": f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}",
        "uptime_seconds": int(uptime),
        "images_received": processing_stats["images_received"],
        "meta_received": processing_stats["meta_received"],
        "errors": processing_stats["errors"],
        "image_dir": str(Config.IMAGE_DIR),
        "meta_dir": str(Config.META_DIR),
        "disk_usage_bytes": {
            "images": image_size,
            "meta": meta_size,
            "total": image_size + meta_size
        }
    }

# ヘルスチェックエンドポイント
@app.get("/health")
async def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=Config.HOST,
        port=Config.PORT,
        workers=Config.WORKERS
    )
