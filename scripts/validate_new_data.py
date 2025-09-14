# scripts/validate_new_data.py
"""
Script para simular adição de novos dados e validar a automação
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_test_data():
    """Gera dados de teste simulando novos registros"""
    new_data = {
        'order_id': [f'TEST_{datetime.now().strftime("%Y%m%d_%H%M%S")}_{i}' for i in range(10)],
        'customer_id': [f'CUST_TEST_{i}' for i in range(10)],
        'product_id': [f'PROD_TEST_{i}' for i in range(10)],
        'order_status': ['delivered'] * 10,
        'price': np.random.uniform(10, 500, 10),
        'freight_value': np.random.uniform(5, 50, 10),
        'review_score': np.random.randint(1, 6, 10),
        'order_purchase_timestamp': [datetime.now() - timedelta(days=i) for i in range(10)]
    }
    return pd.DataFrame(new_data)