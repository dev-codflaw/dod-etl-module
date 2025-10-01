#!/bin/bash
set -e  # stop on first error

# --- Config ---
CURRENT_DIR=$(pwd)
GIT_REPO="https://github.com/dev-codflaw/dod-etl-module.git"

echo "🚀 Starting DoD ETL setup in $CURRENT_DIR"

# --- System update & basics ---
echo "📦 Updating system..."
sudo apt-get update -y
sudo apt-get install -y python3 python3-venv python3-pip git

# --- Create project dir (if not exist) ---
if [ ! -d "dod-etl-module" ]; then
  echo "📂 Creating project dir..."
  mkdir dod-etl-module
fi
cd dod-etl-module

# --- Clone repo ---
if [ ! -d ".git" ]; then
  echo "📥 Cloning repo into $(pwd)..."
  git clone "$GIT_REPO" .
else
  echo "🔄 Repo already exists, pulling latest..."
  git pull
fi

# --- Python virtual env ---
echo "🐍 Creating Python venv..."
python3 -m venv env
source env/bin/activate

# --- Install dependencies ---
echo "📦 Installing Python libs..."
pip install --upgrade pip
pip install requests boto3 parsel json_repair pymongo python-dotenv

# --- Create .env file if missing ---
if [ ! -f ".env" ]; then
  echo "⚙️  Creating .env file (empty, fill it with your secrets)"
  cat <<EOF > .env
MONGO_USERNAME=
MONGO_PASSWORD=
MONGO_CLUSTER_URL=cluster1.c4idkzi.mongodb.net
MONGO_DB_NAME=FacebookETL_InputData
MONGO_COLLECTION_NAME=batch_ag
DO_SPACES_KEY=
DO_SPACES_SECRET=
DO_SPACES_REGION=nyc3
DO_SPACES_BUCKET=
DO_SPACES_ENDPOINT=
STORAGE_TYPE=local   # or spaces
EOF
else
  echo "⚙️  .env already exists, skipping."
fi

echo "✅ Setup complete!"
echo "➡️  To activate your environment, run:"
echo "   cd $CURRENT_DIR/dod-etl-module && source env/bin/activate"