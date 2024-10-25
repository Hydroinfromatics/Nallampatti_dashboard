import pandas as pd
import logging
from typing import Optional, Dict
from datetime import datetime
import numpy as np

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.required_columns = ['timestamp', 'FlowInd', 'TDS', 'pH', 'Depth']
        # List of possible date formats
        self.date_formats = [
            "%d-%b-%Y %H:%M:%S",        # 21-Aug-2024 12:11:13
            "%Y-%m-%d %H:%M:%S",        # 2024-08-21 12:11:13
            "%d/%m/%Y %H:%M",           # 21/08/2024 12:11
            "%d-%m-%Y %H:%M:%S",        # 21-08-2024 12:11:13
            "%Y/%m/%d %H:%M:%S",        # 2024/08/21 12:11:13
            "%d-%m-%Y %I:%M:%S %p"      # 21-08-2024 12:11:13 PM
        ]

    def _parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Try multiple date formats to parse timestamp"""
        for date_format in self.date_formats:
            try:
                return pd.to_datetime(timestamp_str, format=date_format)
            except (ValueError, TypeError):
                continue
        # If no format matches, try pandas flexible parser
        try:
            return pd.to_datetime(timestamp_str)
        except Exception as e:
            logger.error(f"Failed to parse timestamp '{timestamp_str}': {str(e)}")
            return None

    def preprocess_data(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Preprocess raw data with proper type handling"""
        try:
            if df is None or df.empty:
                logger.warning("Empty dataframe received for preprocessing")
                return None

            logger.info(f"Starting preprocessing of {len(df)} records")
            
            # Log sample of raw data
            logger.info(f"Sample of raw timestamps: {df['timestamp'].head().tolist()}")

            # Convert timestamps using multiple format handling
            try:
                # First try vectorized conversion with the first format
                df['timestamp'] = pd.to_datetime(df['timestamp'], format=self.date_formats[0])
            except ValueError:
                # If that fails, try each timestamp individually with multiple formats
                logger.info("Primary timestamp conversion failed, trying multiple formats")
                temp_timestamps = []
                for ts in df['timestamp']:
                    parsed_ts = self._parse_timestamp(ts)
                    if parsed_ts is None:
                        logger.error(f"Could not parse timestamp: {ts}")
                        # Use a fallback value or NaT
                        temp_timestamps.append(pd.NaT)
                    else:
                        temp_timestamps.append(parsed_ts)
                df['timestamp'] = temp_timestamps

            # Remove rows with invalid timestamps
            invalid_timestamps = df['timestamp'].isna().sum()
            if invalid_timestamps > 0:
                logger.warning(f"Removing {invalid_timestamps} rows with invalid timestamps")
                df = df.dropna(subset=['timestamp'])

            # Convert numeric columns
            numeric_cols = {'FlowInd': 'flow', 'TDS': 'tds', 'pH': 'ph', 'Depth': 'depth'}
            
            # Create new columns with standardized names
            for old_col, new_col in numeric_cols.items():
                if old_col in df.columns:
                    # First try direct conversion
                    try:
                        df[new_col] = pd.to_numeric(df[old_col], errors='coerce')
                    except Exception as e:
                        logger.error(f"Error converting {old_col}: {str(e)}")
                        # Try cleaning the data first
                        try:
                            # Remove any non-numeric characters except decimal point and minus
                            cleaned_values = df[old_col].astype(str).str.extract(r'([-]?\d*\.?\d+)')[0]
                            df[new_col] = pd.to_numeric(cleaned_values, errors='coerce')
                        except Exception as e2:
                            logger.error(f"Failed to clean and convert {old_col}: {str(e2)}")
                            df[new_col] = np.nan
                else:
                    logger.warning(f"Column {old_col} not found in dataframe")
                    df[new_col] = np.nan

            # Handle missing values with forward fill then backward fill
            for col in ['tds', 'ph', 'depth', 'flow']:
                df[col] = df[col].fillna(method='ffill').fillna(method='bfill')

            # Apply default values to any remaining NaN values
            df['tds'] = df['tds'].fillna(0)
            df['ph'] = df['ph'].fillna(7)  # Neutral pH
            df['depth'] = df['depth'].fillna(0)
            df['flow'] = df['flow'].fillna(0)

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

            # Drop the original columns
            for old_col in numeric_cols.keys():
                if old_col in df.columns:
                    df = df.drop(columns=[old_col])

            # Sort by timestamp
            df = df.sort_values('timestamp')

            # Log data quality metrics
            logger.info(f"Processed data shape: {df.shape}")
            logger.info(f"Timestamp range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            logger.info(f"Numeric ranges:")
            for col in ['flow', 'tds', 'ph', 'depth']:
                logger.info(f"{col}: {df[col].min():.2f} to {df[col].max():.2f}")

            return df

        except Exception as e:
            logger.error(f"Preprocessing error: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
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
            if df.empty:
                return {
                    'records': 0,
                    'latest_reading': None,
                    'avg_ph': 0,
                    'avg_tds': 0,
                    'avg_flow': 0,
                    'avg_depth': 0
                }

            stats = {
                'records': len(df),
                'latest_reading': df['timestamp'].max(),
                'avg_ph': df['ph'].mean().round(2),
                'avg_tds': df['tds'].mean().round(2),
                'avg_flow': df['flow'].mean().round(2),
                'avg_depth': df['depth'].mean().round(2)
            }

            return stats

        except Exception as e:
            logger.error(f"Statistics calculation error: {str(e)}")
            return {
                'records': 0,
                'latest_reading': None,
                'avg_ph': 0,
                'avg_tds': 0,
                'avg_flow': 0,
                'avg_depth': 0
            }
