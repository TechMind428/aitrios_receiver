#!/bin/bash

# 必要なディレクトリを作成
mkdir -p ./image
mkdir -p ./meta
mkdir -p ./logs

# 依存パッケージがインストールされているか確認
if ! pip list | grep -q aiofiles; then
    echo "Installing missing dependencies..."
    pip install aiofiles
fi

# 環境変数の設定（オプション）
export AITRIOS_RECEIVER_PORT=8080
export AITRIOS_RECEIVER_WORKERS=4

# HTTPSサポートの確認（オプション）
if [ -f "./ssl/cert.pem" ] && [ -f "./ssl/key.pem" ]; then
    echo "SSL certificates found. Starting with HTTPS support."
    SSL_ARGS="--ssl-keyfile=./ssl/key.pem --ssl-certfile=./ssl/cert.pem"
else
    echo "SSL certificates not found. Starting with HTTP only."
    SSL_ARGS=""
    # 自己署名証明書を作成する（オプション）
    # mkdir -p ./ssl
    # openssl req -x509 -newkey rsa:4096 -nodes -keyout ./ssl/key.pem -out ./ssl/cert.pem -days 365 -subj "/CN=localhost"
fi

# サーバー起動（ワーカー数を指定）
echo "Starting AITRIOS Data Receiver..."
uvicorn main:app --host 0.0.0.0 --port ${AITRIOS_RECEIVER_PORT:-8080} --workers ${AITRIOS_RECEIVER_WORKERS:-4} $SSL_ARGS

# 起動に失敗した場合
if [ $? -ne 0 ]; then
    echo "Failed to start server. Check logs for details."
    exit 1
fi
