import pandas as pd
import logging
from typing import Optional, Tuple, Dict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.required_columns = ['timestamp', 'flow', 'tds', 'ph', 'depth']

    def preprocess_data(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Preprocess raw data with proper type handling"""
        try:
            if df is None or df.empty:
                return None

            # Convert timestamp
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Convert numeric columns
            numeric_cols = ['flow', 'tds', 'ph', 'depth']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # Handle missing values
            df['tds'] = df['tds'].fillna(method='ffill').fillna(0)
            df['ph'] = df['ph'].fillna(method='ffill').fillna(7)  # Neutral pH
            df['depth'] = df['depth'].fillna(method='ffill').fillna(0)
            df['flow'] = df['flow'].fillna(method='ffill').fillna(0)

            # Validate ranges
            df.loc[df['ph'] < 0, 'ph'] = 7
            df.loc[df['ph'] > 14, 'ph'] = 7
            df.loc[df['tds'] < 0, 'tds'] = 0
            df.loc[df['depth'] < 0, 'depth'] = 0
            df.loc[df['flow'] < 0, 'flow'] = 0

            # Round values
            df['tds'] = df['tds'].round()
            df['ph'] = df['ph'].round(2)
            df['depth'] = df['depth'].round(2)
            df['flow'] = df['flow'].round(2)

            return df

        except Exception as e:
            logger.error(f"Preprocessing error: {str(e)}")
            return None

    def aggregate_data(self, df: pd.DataFrame, freq: str = 'H') -> Optional[pd.DataFrame]:
        """Aggregate data by specified frequency"""
        try:
            if df is None or df.empty:
                return None

            df = df.set_index('timestamp')
            aggregated = df.resample(freq).agg({
                'flow': 'mean',
                'depth': 'mean',
                'tds': 'mean',
                'ph': 'mean'
            }).round(2)

            return aggregated.reset_index()

        except Exception as e:
            logger.error(f"Aggregation error: {str(e)}")
            return None

    def get_statistics(self, df: pd.DataFrame) -> Dict:
        """Calculate basic statistics from the data"""
        try:
            return {
                'records': len(df),
                'latest_reading': df['timestamp'].max(),
                'avg_ph': df['ph'].mean().round(2),
                'avg_tds': df['tds'].mean().round(2),
                'avg_flow': df['flow'].mean().round(2),
                'avg_depth': df['depth'].mean().round(2)
            }
        except Exception as e:
            logger.error(f"Statistics calculation error: {str(e)}")
            return {}