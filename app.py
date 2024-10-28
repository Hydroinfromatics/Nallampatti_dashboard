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
data_fetcher = DataFetcher(os.getenv('API_URL', 'https://mongodb-api-hmeu.onrender.com'))
data_processor = DataProcessor()

# Global variables with thread safety
from threading import Lock
data_lock = Lock()
current_data = pd.DataFrame()
last_update = None
fetch_error = None
next_update_time = None

def update_data():
    """Background task to fetch and process new data"""
    global current_data, last_update, fetch_error, next_update_time
    
    while True:
        try:
            # Calculate time until next sensor update
            if last_update:
                time_since_update = datetime.now() - last_update
                remaining_time = timedelta(minutes=10) - time_since_update
                next_update_time = datetime.now() + remaining_time if remaining_time.total_seconds() > 0 else datetime.now()
            
            raw_data = data_fetcher.get_data()
            if raw_data is not None:
                processed_data = data_processor.preprocess_data(raw_data)
                if processed_data is not None:
                    with data_lock:
                        current_data = processed_data
                        last_update = datetime.now()
                        next_update_time = last_update + timedelta(minutes=10)
                        fetch_error = None
                        logger.info(f"Data updated successfully at {last_update}")
            else:
                if not fetch_error:
                    fetch_error = "Waiting for next sensor update"
                logger.info("No new data available")
                
        except Exception as e:
            fetch_error = str(e)
            logger.error(f"Update cycle error: {fetch_error}")
            
        # Wait for about 1 minute before checking again
        # This allows us to check frequently enough to catch new sensor data,
        # but not so frequently as to overload the system
        time.sleep(60)

# Start background task
update_thread = threading.Thread(target=update_data, daemon=True)
update_thread.start()

# Dashboard layout
app.layout = html.Div([
    html.H1("Real-Time Water Quality Dashboard", className='text-center mb-4'),
    
    # Status section
    html.Div([
        html.Div(id='update-time', className='text-center mb-2'),
        html.Div(id='next-update', className='text-center mb-2'),
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
     Output('next-update', 'children'),
     Output('status', 'children'),
     Output('stats', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_status(n):
    with data_lock:
        if current_data.empty:
            return "No data", "", "Waiting for initial data...", ""
            
        stats = data_processor.get_statistics(current_data)
        
        # Calculate time until next update
        next_update_str = ""
        if next_update_time:
            time_remaining = next_update_time - datetime.now()
            if time_remaining.total_seconds() > 0:
                minutes = int(time_remaining.total_seconds() // 60)
                seconds = int(time_remaining.total_seconds() % 60)
                next_update_str = f"Next sensor update in: {minutes}m {seconds}s"
            else:
                next_update_str = "Sensor update expected soon"
        
        status_color = 'red' if fetch_error else 'green'
        status_text = fetch_error if fetch_error else "Active"
        
        return (
            f"Last Updated: {last_update.strftime('%Y-%m-%d %H:%M:%S')}",
            next_update_str,
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
                },
                'hovermode': 'x unified'
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
                },
                'hovermode': 'x unified'
            }
        }
        
        return quality_fig, flow_fig

if __name__ == '__main__':
    port = int(os.getenv('PORT', 8050))
    app.run_server(host='0.0.0.0', port=port, debug=False)
