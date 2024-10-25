from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from flask import Flask
import pandas as pd
from datetime import datetime, timedelta
import threading
import time
import os
from data_fetcher import DataFetcher
from data_processor import DataProcessor
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask and Dash
server = Flask(__name__)
app = Dash(__name__, server=server)

# Initialize components
data_fetcher = DataFetcher(os.getenv('API_URL', 'http://your-api-url'))
data_processor = DataProcessor()

# Global variables with thread safety
from threading import Lock
data_lock = Lock()
current_data = pd.DataFrame()
last_update = None
fetch_error = None

def update_data():
    """Background task to fetch and process new data"""
    global current_data, last_update, fetch_error
    
    while True:
        try:
            raw_data = data_fetcher.get_data()
            if raw_data is not None:
                processed_data = data_processor.preprocess_data(raw_data)
                if processed_data is not None:
                    with data_lock:
                        current_data = processed_data
                        last_update = datetime.now()
                        fetch_error = None
                        logger.info("Data updated successfully")
            else:
                fetch_error = "Failed to fetch new data"
                logger.error(fetch_error)
                
        except Exception as e:
            fetch_error = str(e)
            logger.error(f"Update cycle error: {fetch_error}")
            
        time.sleep(300)  # Update every 5 minutes

# Start background task
update_thread = threading.Thread(target=update_data, daemon=True)
update_thread.start()

# Dashboard layout
app.layout = html.Div([
    html.H1("Real-Time Water Quality Dashboard", className='text-center mb-4'),
    
    # Status section
    html.Div([
        html.Div(id='update-time', className='text-center mb-2'),
        html.Div(id='status', className='text-center mb-2'),
        html.Div(id='stats', className='text-center mb-4')
    ]),
    
    # Time range selector
    dcc.Dropdown(
        id='time-range',
        options=[
            {'label': 'Last Hour', 'value': '1H'},
            {'label': 'Last 6 Hours', 'value': '6H'},
            {'label': 'Last 24 Hours', 'value': '24H'},
            {'label': 'Last Week', 'value': '7D'}
        ],
        value='24H',
        className='mb-4'
    ),
    
    # Graphs
    dcc.Graph(id='quality-graph', className='mb-4'),
    dcc.Graph(id='flow-graph', className='mb-4'),
    
    # Update interval
    dcc.Interval(
        id='interval-component',
        interval=30*1000,  # 30 seconds
        n_intervals=0
    )
])

@app.callback(
    [Output('update-time', 'children'),
     Output('status', 'children'),
     Output('stats', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_status(n):
    with data_lock:
        if current_data.empty:
            return "No data", "Waiting for data...", ""
            
        stats = data_processor.get_statistics(current_data)
        status_color = 'red' if fetch_error else 'green'
        status_text = fetch_error if fetch_error else "Active"
        
        return (
            f"Last Updated: {last_update.strftime('%Y-%m-%d %H:%M:%S')}",
            html.Div(f"Status: {status_text}", style={'color': status_color}),
            f"Records: {stats['records']} | Avg pH: {stats['avg_ph']} | Avg TDS: {stats['avg_tds']} ppm"
        )

@app.callback(
    [Output('quality-graph', 'figure'),
     Output('flow-graph', 'figure')],
    [Input('interval-component', 'n_intervals'),
     Input('time-range', 'value')]
)
def update_graphs(n, time_range):
    with data_lock:
        if current_data.empty:
            return ({}, {})
            
        # Filter data based on time range
        end_time = datetime.now()
        if time_range == '1H':
            start_time = end_time - timedelta(hours=1)
        elif time_range == '6H':
            start_time = end_time - timedelta(hours=6)
        elif time_range == '24H':
            start_time = end_time - timedelta(days=1)
        else:
            start_time = end_time - timedelta(days=7)
            
        mask = (current_data['timestamp'] >= start_time)
        df = current_data[mask]
        
        if df.empty:
            return ({}, {})
            
        # Quality graph
        quality_fig = {
            'data': [
                go.Scatter(x=df['timestamp'], y=df['ph'], name='pH',
                          mode='lines+markers'),
                go.Scatter(x=df['timestamp'], y=df['tds'], name='TDS',
                          mode='lines+markers', yaxis='y2')
            ],
            'layout': {
                'title': 'Water Quality Metrics',
                'xaxis': {'title': 'Time'},
                'yaxis': {'title': 'pH'},
                'yaxis2': {
                    'title': 'TDS (ppm)',
                    'overlaying': 'y',
                    'side': 'right'
                }
            }
        }
        
        # Flow graph
        flow_fig = {
            'data': [
                go.Scatter(x=df['timestamp'], y=df['flow'], name='Flow',
                          mode='lines+markers'),
                go.Scatter(x=df['timestamp'], y=df['depth'], name='Depth',
                          mode='lines+markers', yaxis='y2')
            ],
            'layout': {
                'title': 'Flow and Depth Metrics',
                'xaxis': {'title': 'Time'},
                'yaxis': {'title': 'Flow Rate'},
                'yaxis2': {
                    'title': 'Depth (ft)',
                    'overlaying': 'y',
                    'side': 'right'
                }
            }
        }
        
        return quality_fig, flow_fig

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8050))
    app.run_server(host='0.0.0.0', port=port, debug=False)