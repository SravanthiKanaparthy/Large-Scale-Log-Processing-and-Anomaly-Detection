import os
import json
import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from elasticsearch import Elasticsearch
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Connect to Elasticsearch
es = Elasticsearch(
    [f"http://{os.environ.get('ES_HOST', 'elasticsearch')}:9200"],
    http_auth=(os.environ.get('ES_USER', 'elastic'), 
               os.environ.get('ES_PASSWORD', 'changeme'))
)

# Kafka configuration
kafka_brokers = os.environ.get('KAFKA_BROKERS', 'kafka:9092')
consumer = KafkaConsumer(
    'ml-input-stream',
    bootstrap_servers=kafka_brokers,
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=kafka_brokers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Feature extraction function
def extract_features(log_entry):
    """Extract features from log entry for anomaly detection"""
    features = {}
    
    # Basic log properties
    features['timestamp'] = log_entry.get('timestamp', datetime.now().timestamp())
    features['processing_time'] = log_entry.get('processing_time', 0)
    features['message_length'] = log_entry.get('message_length', 0)
    
    # Log level as numeric value
    level_map = {'DEBUG': 0, 'INFO': 1, 'WARN': 2, 'ERROR': 3, 'CRITICAL': 4}
    level = log_entry.get('level', 'INFO').upper()
    features['level_value'] = level_map.get(level, 1)
    
    # Boolean flags
    features['is_error'] = 1 if level in ['ERROR', 'CRITICAL'] else 0
    features['contains_error'] = 1 if log_entry.get('contains_error', False) else 0
    features['contains_exception'] = 1 if log_entry.get('contains_exception', False) else 0
    
    return features

# Load or train the model
model_path = 'anomaly_model.pkl'
scaler_path = 'feature_scaler.pkl'

try:
    model = joblib.load(model_path)
    scaler = joblib.load(scaler_path)
    logger.info("Loaded existing anomaly detection model")
except:
    logger.info("Training new anomaly detection model")
    # Initialize with default model
    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,  # Assuming 5% of logs are anomalies
        random_state=42
    )
    scaler = StandardScaler()

# Collect initial data for model training/updating
def collect_training_data(n_samples=1000):
    """Collect logs for initial model training"""
    logger.info(f"Collecting {n_samples} logs for model training")
    
    # Query recent logs from Elasticsearch
    response = es.search(
        index="logs-*",
        body={
            "size": n_samples,
            "sort": [{"@timestamp": {"order": "desc"}}]
        }
    )
    
    logs = [hit['_source'] for hit in response['hits']['hits']]
    features_list = [extract_features(log) for log in logs]
    
    return pd.DataFrame(features_list)

# Train or update the model
def train_model(df):
    """Train the anomaly detection model"""
    feature_columns = ['message_length', 'level_value', 'is_error', 
                      'contains_error', 'contains_exception']
    
    X = df[feature_columns].values
    X_scaled = scaler.fit_transform(X)
    
    model.fit(X_scaled)
    
    # Save the model
    joblib.dump(model, model_path)
    joblib.dump(scaler, scaler_path)
    
    logger.info("Model trained and saved")
    return model, scaler

# Check if we need to train the model
try:
    # Try to predict on a dummy sample to see if model is trained
    dummy = np.zeros((1, 5))
    model.predict(dummy)
except:
    # Model needs training
    df = collect_training_data()
    model, scaler = train_model(df)

# Process logs and detect anomalies
def process_logs():
    """Main processing loop for anomaly detection"""
    logger.info("Starting anomaly detection processing")
    
    batch_size = 100
    batch = []
    
    for message in consumer:
        log_entry = message.value
        
        # Extract features
        features = extract_features(log_entry)
        
        # Convert to format for prediction
        feature_values = np.array([
            features['message_length'], 
            features['level_value'],
            features['is_error'],
            features['contains_error'],
            features['contains_exception']
        ]).reshape(1, -1)
        
        # Scale features
        scaled_features = scaler.transform(feature_values)
        
        # Predict anomaly (-1 for anomalies, 1 for normal)
        prediction = model.predict(scaled_features)[0]
        anomaly_score = model.score_samples(scaled_features)[0]
        
        # If anomaly detected
        if prediction == -1:
            # Enrich log with anomaly information
            anomaly_info = {
                "original_log": log_entry,
                "is_anomaly": True,
                "anomaly_score": float(anomaly_score),
                "detection_time": datetime.now().isoformat(),
                "features": {k: float(v) if isinstance(v, (int, float, bool)) else v 
                            for k, v in features.items()}
            }
            
            # Determine anomaly type and severity
            if features['is_error'] == 1:
                if 'memory' in log_entry.get('message', '').lower():
                    anomaly_info['type'] = 'Memory Issue'
                elif 'cpu' in log_entry.get('message', '').lower():
                    anomaly_info['type'] = 'CPU Spike'
                elif 'timeout' in log_entry.get('message', '').lower():
                    anomaly_info['type'] = 'Timeout'
                elif 'auth' in log_entry.get('message', '').lower():
                    anomaly_info['type'] = 'Authentication Issue'
                else:
                    anomaly_info['type'] = 'Error Anomaly'
            else:
                anomaly_info['type'] = 'Pattern Anomaly'
            
            # Set severity based on anomaly score
            if anomaly_score < -0.8:
                anomaly_info['severity'] = 'high'
            elif anomaly_score < -0.5:
                anomaly_info['severity'] = 'medium'
            else:
                anomaly_info['severity'] = 'low'
                
            # Send to Kafka for alerting
            producer.send('anomalies', anomaly_info)
            
            # Store in Elasticsearch
            es.index(
                index='log-anomalies',
                body=anomaly_info
            )
            
            logger.info(f"Anomaly detected: {anomaly_info['type']} with severity {anomaly_info['severity']}")
        
        # Collect logs for batch retraining
        batch.append(features)
        
        # Periodically retrain the model with new data
        if len(batch) >= batch_size:
            df_batch = pd.DataFrame(batch)
            model, scaler = train_model(df_batch)
            batch = []
            logger.info("Model retrained with new data")

if __name__ == "__main__":
    try:
        process_logs()
    except KeyboardInterrupt:
        logger.info("Shutting down anomaly detection service")
    except Exception as e:
        logger.error(f"Error in anomaly detection: {str(e)}")
