import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
import pandas as pd
from datetime import datetime, timedelta
import os
import io
import base64

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
app.title = "Euro Area HICP Nowcast Dashboard"

app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            @media (max-width: 768px) {
                .container-fluid {
                    padding-left: 10px !important;
                    padding-right: 10px !important;
                }
                h1 {
                    font-size: 1.5rem !important;
                }
                h5 {
                    font-size: 1rem !important;
                }
                .card-body {
                    padding: 0.75rem !important;
                }
                .btn-sm {
                    font-size: 0.75rem !important;
                    padding: 0.25rem 0.5rem !important;
                }
            }
            @media (max-width: 992px) {
                .row > [class*='col'] {
                    margin-bottom: 1rem;
                }
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

def get_connection():
    """Open read-only connection to DuckDB."""
    db_path = "../../data/euro_nowcast.duckdb"
    if os.path.exists(db_path):
        return duckdb.connect(db_path, read_only=True)
    else:
        return None

def get_hicp_series(start_date, end_date) -> pd.DataFrame:
    """Query gold.v_hicp_dashboard_wide for HICP data between dates."""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT *
            FROM gold.v_hicp_dashboard_wide
            WHERE month BETWEEN ? AND ?
            ORDER BY month
        """
        df = conn.execute(query, [start_date, end_date]).df()
        df['month'] = pd.to_datetime(df['month'])
        return df
    finally:
        conn.close()

def get_macro_series(start_date, end_date):
    """Query gold.v_macro_monthly_wide for FX, energy, and policy rate data."""
    conn = get_connection()
    
    if conn is None:
        return pd.DataFrame()
    
    try:
        query = """
        SELECT 
            ref_month,
            fx_usd_eur_avg_m,
            fx_ma3,
            fx_ma12,
            brent_crude_m,
            brent_crude_ma3,
            brent_crude_ma12,
            nat_gas_m,
            nat_gas_ma3,
            nat_gas_ma12,
            policy_rate_eom_m
        FROM gold.v_macro_monthly_wide
        WHERE ref_month >= ? AND ref_month <= ?
        ORDER BY ref_month
        """
        df = conn.execute(query, [start_date, end_date]).fetchdf()
        conn.close()
        return df
    except Exception as e:
        print(f"Error querying macro data: {e}")
        conn.close()
        return pd.DataFrame()

def parse_time_range(time_range_value, custom_start=None, custom_end=None):
    """Convert time range dropdown selection to start/end dates."""
    current_year = datetime.now().year
    
    if time_range_value == "custom" and custom_start and custom_end:
        return custom_start, custom_end
    elif time_range_value == "5years":
        return f"{current_year - 5}-01-01", f"{current_year}-12-31"
    elif time_range_value == "10years":
        return f"{current_year - 10}-01-01", f"{current_year}-12-31"
    elif time_range_value == "since2000":
        return "2000-01-01", f"{current_year}-12-31"
    else:
        return f"{current_year - 5}-01-01", f"{current_year}-12-31"

def calculate_statistics(hicp_df, macro_df):
    """Calculate key metrics (current HICP, YoY, nowcast) from dataframes."""
    stats = {}
    
    if len(hicp_df) > 0:
        actual_data = hicp_df[hicp_df['source'] == 'actual'].copy()
        nowcast_data = hicp_df[hicp_df['source'] == 'nowcast'].copy()
        
        if len(actual_data) > 0:
            latest_actual = actual_data.iloc[-1]
            stats['current_hicp_index'] = latest_actual['index_value']
            stats['current_hicp_yoy'] = latest_actual['hicp_yoy']
            stats['latest_actual_date'] = latest_actual['month']
            
            if len(actual_data) >= 3:
                recent_trend = actual_data.tail(3)['hicp_yoy'].mean()
                stats['recent_trend_3m'] = recent_trend
        
        if len(nowcast_data) > 0:
            latest_nowcast = nowcast_data.iloc[-1]
            nowcast_month = latest_nowcast['month']
            nowcast_index = latest_nowcast['index_value']
            
            # Calculate YoY properly: compare nowcast with actual from 12 months ago
            twelve_months_ago = pd.to_datetime(nowcast_month) - pd.DateOffset(months=12)
            
            # Find the actual value from 12 months ago
            hist_data = actual_data[actual_data['month'] == twelve_months_ago]
            
            if len(hist_data) > 0:
                hist_index = hist_data.iloc[0]['index_value']
                # Calculate YoY as (current / 12_months_ago - 1)
                nowcast_yoy = (nowcast_index / hist_index) - 1
                stats['nowcast_hicp_yoy'] = nowcast_yoy
                stats['nowcast_hicp_index'] = nowcast_index
                stats['nowcast_date'] = nowcast_month
                stats['nowcast_base_date'] = twelve_months_ago
                stats['nowcast_base_index'] = hist_index
            else:
                # Fallback: use the hicp_yoy from the view
                stats['nowcast_hicp_yoy'] = latest_nowcast['hicp_yoy']
                stats['nowcast_hicp_index'] = nowcast_index
                stats['nowcast_date'] = nowcast_month
    
        if len(macro_df) > 0:
            latest_macro = macro_df.iloc[-1]
            stats['current_fx'] = latest_macro['fx_usd_eur_avg_m']
            stats['current_brent'] = latest_macro['brent_crude_m']
            stats['current_nat_gas'] = latest_macro['nat_gas_m']
            stats['current_policy_rate'] = latest_macro['policy_rate_eom_m']
            
            return stats

def get_latest_model_metrics():
    """Fetch latest model performance from ml.metrics."""
    conn = get_connection()
    if conn is None:
        return None
    
    try:
        query = """
        SELECT 
            r.model_version,
            r.finished_at_utc,
            m.metric,
            m.value
        FROM ml.runs r
        JOIN ml.metrics m ON r.run_id = m.run_id
        WHERE r.finished_at_utc = (SELECT MAX(finished_at_utc) FROM ml.runs)
        ORDER BY m.metric
        """
        df = conn.execute(query).df()
        
        if len(df) > 0:
            metrics = {}
            for _, row in df.iterrows():
                metrics[row['metric']] = row['value']
            metrics['model_version'] = df['model_version'].iloc[0]
            metrics['trained_at'] = df['finished_at_utc'].iloc[0]
            return metrics
        return None
    except:
        return None
    finally:
        conn.close()

def create_csv_download(df, filename):
    """Create CSV download link from DataFrame."""
    csv_string = df.to_csv(index=False, encoding='utf-8')
    csv_string = "data:text/csv;charset=utf-8," + csv_string
    return csv_string

# Pre-build the left-hand control column so callbacks can reference IDs declared
# here. This keeps the layout definition tidy and mirrors how the production app
# would compose reusable components.
controls = dbc.Card([
    dbc.CardBody([
        html.H5("Controls", className="card-title mb-3"),
        
        html.Label("Time Range", className="fw-bold mb-2"),
        dcc.Dropdown(
            id='time-range-dropdown',
            options=[
                {'label': 'Last 5 years', 'value': '5years'},
                {'label': 'Last 10 years', 'value': '10years'},
                {'label': 'Since 2000', 'value': 'since2000'},
                {'label': 'Custom Range', 'value': 'custom'},
            ],
            value='5years',
            clearable=False,
            className="mb-3"
        ),
        
        html.Div(id='custom-date-range', children=[
            html.Label("Custom Start Date", className="fw-bold mb-1", style={'fontSize': '0.9em'}),
            dcc.DatePickerSingle(
                id='custom-start-date',
                date=datetime(2020, 1, 1),
                display_format='YYYY-MM-DD',
                className="mb-2"
            ),
            html.Label("Custom End Date", className="fw-bold mb-1", style={'fontSize': '0.9em'}),
            dcc.DatePickerSingle(
                id='custom-end-date',
                date=datetime.now(),
                display_format='YYYY-MM-DD',
                className="mb-3"
            ),
        ], style={'display': 'none'}),
        
        html.Label("Macro Series", className="fw-bold mb-2"),
        dcc.Dropdown(
            id='macro-series-dropdown',
            options=[
                {'label': 'FX USD/EUR', 'value': 'fx'},
                {'label': 'Brent Crude Oil', 'value': 'brent'},
                {'label': 'Natural Gas (EU)', 'value': 'natgas'},
                {'label': 'Policy Rate', 'value': 'policy'},
            ],
            value=['fx', 'brent', 'natgas'],
            multi=True,
            className="mb-3"
        ),
        
        html.Label("Display Options", className="fw-bold mb-2"),
        dbc.RadioItems(
            id='show-ma-toggle',
            options=[
                {'label': 'Show Rolling Averages', 'value': 'show'},
                {'label': 'Hide Rolling Averages', 'value': 'hide'},
            ],
            value='hide',
            className="mb-3"
        ),
        
        dbc.RadioItems(
            id='show-nowcast-toggle',
            options=[
                {'label': 'Show Nowcast', 'value': 'show'},
                {'label': 'Hide Nowcast', 'value': 'hide'},
            ],
            value='show',
            className="mb-3"
        ),
        
        html.Label("Comparison Mode", className="fw-bold mb-2"),
        dbc.Checklist(
            id='comparison-toggle',
            options=[
                {'label': 'Enable Period Comparison', 'value': 'enable'},
            ],
            value=[],
            className="mb-3"
        ),
        
        html.Div(id='comparison-controls', children=[
            html.Label("Compare with Period", className="fw-bold mb-1", style={'fontSize': '0.9em'}),
            dcc.Dropdown(
                id='comparison-period',
                options=[
                    {'label': '1 year earlier', 'value': '1year'},
                    {'label': '2 years earlier', 'value': '2years'},
                    {'label': '5 years earlier', 'value': '5years'},
                ],
                value='1year',
                clearable=False,
                className="mb-3"
            ),
        ], style={'display': 'none'}),
        
        html.Hr(),
        html.Label("Export Data", className="fw-bold mb-2"),
        dbc.ButtonGroup([
            dbc.Button("Export HICP CSV", id="export-hicp-btn", color="primary", size="sm", className="me-1"),
            dbc.Button("Export Macro CSV", id="export-macro-btn", color="primary", size="sm"),
        ], className="d-grid gap-2"),
        dcc.Download(id="download-hicp"),
        dcc.Download(id="download-macro"),
    ])
], className="mb-4")

app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("Euro Area HICP Nowcast Dashboard", className="text-center mb-4 mt-4")
        ])
    ]),
    
    dbc.Row([
        dbc.Col([controls], width=12, lg=3),
        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H5("Key Statistics", className="card-title mb-3"),
                    html.Div(id='statistics-panel')
                ])
            ], className="mb-4"),
            
            dbc.Card([
                dbc.CardBody([
                    html.H5("HICP Index and Year-over-Year Change", className="card-title"),
                    dcc.Graph(id='hicp-chart', config={'displayModeBar': True})
                ])
            ], className="mb-4"),
            
            dbc.Card([
                dbc.CardBody([
                    html.H5("Macro Indicators", className="card-title"),
                    dcc.Graph(id='macro-chart', config={'displayModeBar': True})
                ])
            ])
        ], width=12, lg=9)
    ]),
    
    dcc.Store(id='hicp-data-store'),
    dcc.Store(id='macro-data-store'),
], fluid=True)

@app.callback(
    Output('custom-date-range', 'style'),
    Input('time-range-dropdown', 'value')
)
def toggle_custom_date_range(time_range):
    """Show/hide custom date pickers."""
    if time_range == 'custom':
        return {'display': 'block'}
    return {'display': 'none'}

@app.callback(
    Output('comparison-controls', 'style'),
    Input('comparison-toggle', 'value')
)
def toggle_comparison_controls(comparison_enabled):
    """Show/hide comparison period selector."""
    if 'enable' in comparison_enabled:
        return {'display': 'block'}
    return {'display': 'none'}

@app.callback(
    [Output('hicp-data-store', 'data'),
     Output('macro-data-store', 'data')],
    [Input('time-range-dropdown', 'value'),
     Input('custom-start-date', 'date'),
     Input('custom-end-date', 'date')]
)
def update_data_stores(time_range, custom_start, custom_end):
    """Fetch and store HICP/macro data based on time range."""
    start_date, end_date = parse_time_range(time_range, custom_start, custom_end)
    
    hicp_df = get_hicp_series(start_date, end_date)
    macro_df = get_macro_series(start_date, end_date)
    
    return hicp_df.to_json(date_format='iso', orient='split'), macro_df.to_json(date_format='iso', orient='split')

@app.callback(
    [Output('hicp-chart', 'figure'),
     Output('macro-chart', 'figure'),
     Output('statistics-panel', 'children')],
    [Input('hicp-data-store', 'data'),
     Input('macro-data-store', 'data'),
     Input('macro-series-dropdown', 'value'),
     Input('show-ma-toggle', 'value'),
     Input('show-nowcast-toggle', 'value'),
     Input('comparison-toggle', 'value'),
     Input('comparison-period', 'value')]
)
def update_charts_and_stats(hicp_json, macro_json, macro_series, show_ma, show_nowcast, comparison_enabled, comparison_period):
    """Update charts and statistics panel based on user selections."""
    hicp_df = pd.read_json(io.StringIO(hicp_json), orient='split')
    macro_df = pd.read_json(io.StringIO(macro_json), orient='split')
    
    comparison_mode = 'enable' in comparison_enabled
    
    hicp_fig = create_hicp_chart(hicp_df, show_nowcast, comparison_mode, comparison_period)
    macro_fig = create_macro_chart(macro_df, macro_series, show_ma, comparison_mode, comparison_period)
    
    stats = calculate_statistics(hicp_df, macro_df)
    stats_panel = create_statistics_panel(stats)
    
    return hicp_fig, macro_fig, stats_panel

@app.callback(
    Output("download-hicp", "data"),
    Input("export-hicp-btn", "n_clicks"),
    State('hicp-data-store', 'data'),
    prevent_initial_call=True
)
def export_hicp_data(n_clicks, hicp_json):
    """Export HICP data to CSV."""
    if n_clicks:
        hicp_df = pd.read_json(io.StringIO(hicp_json), orient='split')
        return dcc.send_data_frame(hicp_df.to_csv, "hicp_data.csv", index=False)

@app.callback(
    Output("download-macro", "data"),
    Input("export-macro-btn", "n_clicks"),
    State('macro-data-store', 'data'),
    prevent_initial_call=True
)
def export_macro_data(n_clicks, macro_json):
    """Export macro data to CSV."""
    if n_clicks:
        macro_df = pd.read_json(io.StringIO(macro_json), orient='split')
        return dcc.send_data_frame(macro_df.to_csv, "macro_data.csv", index=False)

def create_statistics_panel(stats):
    """Build statistics panel with actual data, nowcast, model performance, and macro indicators."""
    if not stats:
        return html.P("No data available", className="text-muted")
    
    stats_items = []
    
    # SECTION 1: ACTUAL DATA (Latest known values)
    stats_items.append(
        dbc.Col([
            html.Div([
                html.H6("📊 Actual Data", className="text-muted mb-2 fw-bold"),
            ], className="mb-2")
        ], md=12, className="mb-2")
    )
    
    if 'current_hicp_index' in stats:
        stats_items.append(
            dbc.Col([
                html.Div([
                    html.Small("Current HICP Index", className="text-muted"),
                    html.H4(f"{stats['current_hicp_index']:.2f}", className="mb-0"),
                    html.Small(f"as of {pd.to_datetime(stats['latest_actual_date']).strftime('%b %Y')}", className="text-muted")
                ])
            ], md=3, className="mb-3")
        )
    
    if 'current_hicp_yoy' in stats:
        stats_items.append(
            dbc.Col([
                html.Div([
                    html.Small("Inflation Rate (YoY)", className="text-muted"),
                    html.H4(f"{stats['current_hicp_yoy']*100:.2f}%", className="mb-0 text-primary"),
                    html.Small(f"as of {pd.to_datetime(stats['latest_actual_date']).strftime('%b %Y')}", className="text-muted")
                ])
            ], md=3, className="mb-3")
        )
    
    if 'recent_trend_3m' in stats:
        stats_items.append(
            dbc.Col([
                html.Div([
                    html.Small("3-Month Avg Trend", className="text-muted"),
                    html.H4(f"{stats['recent_trend_3m']*100:.2f}%", className="mb-0"),
                ])
            ], md=3, className="mb-3")
        )
    
    # SECTION 2: NOWCAST / PREDICTION
    if 'nowcast_hicp_yoy' in stats:
        stats_items.append(
            dbc.Col([
                html.Div([
                    html.H6("🔮 Model Prediction", className="text-warning mb-2 fw-bold"),
                ], className="mb-2")
            ], md=12, className="mb-2 mt-3")
        )
        
        if 'nowcast_hicp_index' in stats:
            stats_items.append(
                dbc.Col([
                    html.Div([
                        html.Small("Nowcast HICP Index", className="text-muted"),
                        html.H4(f"{stats['nowcast_hicp_index']:.2f}", className="mb-0 text-warning"),
                        html.Small(f"predicted for {pd.to_datetime(stats['nowcast_date']).strftime('%b %Y')}", className="text-muted")
                    ])
                ], md=3, className="mb-3")
            )
        
        stats_items.append(
            dbc.Col([
                html.Div([
                    html.Small("Nowcast Inflation (YoY)", className="text-muted"),
                    html.H4(f"{stats['nowcast_hicp_yoy']*100:.2f}%", className="mb-0 text-warning"),
                    html.Small(f"vs {pd.to_datetime(stats.get('nowcast_base_date', stats['nowcast_date'])).strftime('%b %Y')}" if 'nowcast_base_date' in stats else "", className="text-muted")
                ])
            ], md=3, className="mb-3")
        )
    
        # SECTION 3: MODEL PERFORMANCE
    model_metrics = get_latest_model_metrics()
    if model_metrics:
        stats_items.append(
            dbc.Col([
                html.Div([
                    html.H6("📈 Model Performance", className="text-info mb-2 fw-bold"),
                ], className="mb-2")
            ], md=12, className="mb-2 mt-3")
        )
        
        if 'R2' in model_metrics:  
            stats_items.append(
                dbc.Col([
                    html.Div([
                        html.Small("R² Score", className="text-muted"),
                        html.H6(f"{model_metrics['R2']:.4f}", className="mb-0"),
                        html.Small("variance explained", className="text-muted")
                    ])
                ], md=2, className="mb-3")
            )
        
        if 'MAE' in model_metrics:  
            stats_items.append(
                dbc.Col([
                    html.Div([
                        html.Small("MAE", className="text-muted"),
                        html.H6(f"{model_metrics['MAE']:.3f}", className="mb-0"),
                        html.Small("mean error", className="text-muted")
                    ])
                ], md=2, className="mb-3")
            )
        
        if 'p90_error' in model_metrics:  
            stats_items.append(
                dbc.Col([
                    html.Div([
                        html.Small("P90 Error", className="text-muted"),
                        html.H6(f"{model_metrics['p90_error']:.3f}", className="mb-0"),
                        html.Small("90th percentile", className="text-muted")
                    ])
                ], md=2, className="mb-3")
            )
        
        # Add training date info
        if 'trained_at' in model_metrics:
            trained_date = pd.to_datetime(model_metrics['trained_at']).strftime('%Y-%m-%d')
            stats_items.append(
                dbc.Col([
                    html.Div([
                        html.Small("Last Trained", className="text-muted"),
                        html.H6(trained_date, className="mb-0"),
                    ])
                ], md=2, className="mb-3")
            )
    
    # SECTION 4: MACRO INDICATORS
    if any(k in stats for k in ['current_fx', 'current_brent', 'current_nat_gas', 'current_policy_rate']):
        stats_items.append(
            dbc.Col([
                html.Div([
                    html.H6("🌍 Macro Indicators", className="text-secondary mb-2 fw-bold"),
                ], className="mb-2")
            ], md=12, className="mb-2 mt-3")
        )
        
        if 'current_fx' in stats:
            stats_items.append(
                dbc.Col([
                    html.Div([
                        html.Small("FX USD/EUR", className="text-muted"),
                        html.H6(f"{stats['current_fx']:.3f}", className="mb-0"),
                    ])
                ], md=2, className="mb-3")
            )
        
        if 'current_brent' in stats:
            stats_items.append(
                dbc.Col([
                    html.Div([
                        html.Small("Brent Crude Oil", className="text-muted"),
                        html.H6(f"${stats['current_brent']:.2f}" if pd.notna(stats['current_brent']) else "$nan", className="mb-0"),
                    ])
                ], md=2, className="mb-3")
    )

        if 'current_nat_gas' in stats:
            stats_items.append(
                dbc.Col([
                    html.Div([
                        html.Small("Natural Gas (EU)", className="text-muted"),
                        html.H6(f"${stats['current_nat_gas']:.2f}" if pd.notna(stats['current_nat_gas']) else "$nan", className="mb-0"),
                    ])
                ], md=2, className="mb-3")
            )
        
        if 'current_policy_rate' in stats:
            stats_items.append(
                dbc.Col([
                    html.Div([
                        html.Small("Policy Rate", className="text-muted"),
                        html.H6(f"{stats['current_policy_rate']:.2f}%", className="mb-0"),
                    ])
                ], md=2, className="mb-3")
            )
    
    return dbc.Row(stats_items)

def create_hicp_chart(df, show_nowcast, comparison_mode=False, comparison_period='1year'):
    """Create HICP line chart with dual axes (index and YoY%)."""
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    if show_nowcast == 'hide':
        data_to_plot = df[df['source'] == 'actual'].copy()
    else:
        data_to_plot = df.copy()

    actual_data = df[df['source'] == 'actual']
    nowcast_data = df[df['source'] == 'nowcast']
    
    fig.add_trace(
        go.Scatter(
            x=actual_data['month'],
            y=actual_data['index_value'],
            name='HICP Index (Actual)',
            mode='lines',
            line=dict(color='#1f77b4', width=2),
        ),
        secondary_y=False
    )
    
    if len(nowcast_data) > 0 and show_nowcast == 'show':
        fig.add_trace(
            go.Scatter(
                x=nowcast_data['month'],
                y=nowcast_data['index_value'],
                name='HICP Index (Nowcast)',
                mode='lines',
                line=dict(color='#ff7f0e', width=2, dash='dash'),
            ),
            secondary_y=False
        )
    
    fig.add_trace(
        go.Scatter(
            x=actual_data['month'],
            y=actual_data['hicp_yoy'] * 100,
            name='HICP YoY %',
            mode='lines',
            line=dict(color='#2ca02c', width=2),
        ),
        secondary_y=True
    )
    
    if len(nowcast_data) > 0 and show_nowcast == 'show':
        fig.add_trace(
            go.Scatter(
                x=nowcast_data['month'],
                y=nowcast_data['hicp_yoy'] * 100,
                name='HICP YoY % (Nowcast)',
                mode='lines',
                line=dict(color='#d62728', width=2, dash='dash'),
            ),
            secondary_y=True
        )
    
    if comparison_mode and len(actual_data) > 0:
        offset_months = {'1year': 12, '2years': 24, '5years': 60}.get(comparison_period, 12)
        
        comparison_df = data_to_plot.copy()
        comparison_df['month'] = pd.to_datetime(comparison_df['month'])
        comparison_df['shifted_month'] = comparison_df['month'] + pd.DateOffset(months=offset_months)
        
        comparison_actual = comparison_df[comparison_df['source'] == 'actual']
        
        if len(comparison_actual) > 0:
            fig.add_trace(
                go.Scatter(
                    x=comparison_actual['shifted_month'],
                    y=comparison_actual['index_value'],
                    name=f'HICP Index ({comparison_period} earlier)',
                    mode='lines',
                    line=dict(color='#9467bd', width=1.5, dash='dot'),
                    opacity=0.6,
                ),
                secondary_y=False
            )
    
    fig.update_xaxes(title_text="Month")
    fig.update_yaxes(title_text="HICP Index (2015=100)", secondary_y=False)
    fig.update_yaxes(title_text="Year-over-Year %", secondary_y=True)
    
    fig.update_layout(
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400,
        margin=dict(l=50, r=50, t=50, b=50)
    )
    
    return fig

def create_macro_chart(df, selected_series, show_ma, comparison_mode=False, comparison_period='1year'):
    """Create macro indicators chart with selected series and optional rolling averages."""
    fig = go.Figure()
    
    if not selected_series:
        selected_series = []
    
    colors = {
        'fx': '#1f77b4',
        'brent': '#d62728',
        'natgas': '#ff7f0e',
        'policy': '#2ca02c'
    }
    
    ma_colors = {
        'fx': '#aec7e8',
        'brent': '#ff9999',
        'natgas': '#ffbb78',
        'policy': '#98df8a'
    }
    
    if 'fx' in selected_series:
        fig.add_trace(go.Scatter(
            x=df['ref_month'],
            y=df['fx_usd_eur_avg_m'],
            name='FX USD/EUR',
            mode='lines',
            line=dict(color=colors['fx'], width=2),
            yaxis='y1'
        ))
        
        if show_ma == 'show':
            fig.add_trace(go.Scatter(
                x=df['ref_month'],
                y=df['fx_ma3'],
                name='FX MA3',
                mode='lines',
                line=dict(color=ma_colors['fx'], width=1, dash='dot'),
                yaxis='y1'
            ))
            fig.add_trace(go.Scatter(
                x=df['ref_month'],
                y=df['fx_ma12'],
                name='FX MA12',
                mode='lines',
                line=dict(color=ma_colors['fx'], width=1, dash='dashdot'),
                yaxis='y1'
            ))
    
    # Brent Crude
    if 'brent' in selected_series:
        fig.add_trace(go.Scatter(
            x=df['ref_month'],
            y=df['brent_crude_m'],
            name='Brent Crude (USD/barrel)',
            mode='lines',
            line=dict(color=colors['brent'], width=2),
            yaxis='y2'
        ))
        
        if show_ma == 'show':
            fig.add_trace(go.Scatter(
                x=df['ref_month'],
                y=df['brent_crude_ma3'],
                name='Brent MA3',
                mode='lines',
                line=dict(color=ma_colors['brent'], width=1, dash='dash'),
                yaxis='y2'
            ))

    # Natural Gas
    if 'natgas' in selected_series:
        fig.add_trace(go.Scatter(
            x=df['ref_month'],
            y=df['nat_gas_m'],
            name='Natural Gas EU (USD/MMBtu)',
            mode='lines',
            line=dict(color=colors['natgas'], width=2),
            yaxis='y2'
        ))
        
        if show_ma == 'show':
            fig.add_trace(go.Scatter(
                x=df['ref_month'],
                y=df['nat_gas_ma3'],
                name='Nat Gas MA3',
                mode='lines',
                line=dict(color=ma_colors['natgas'], width=1, dash='dash'),
                yaxis='y2'
            ))
    
    if 'policy' in selected_series:
        fig.add_trace(go.Scatter(
            x=df['ref_month'],
            y=df['policy_rate_eom_m'],
            name='Policy Rate (%)',
            mode='lines',
            line=dict(color=colors['policy'], width=2),
            yaxis='y3'
        ))
    
    if comparison_mode and len(df) > 0:
        offset_months = {'1year': 12, '2years': 24, '5years': 60}.get(comparison_period, 12)
        
        comparison_df = df.copy()
        comparison_df['ref_month'] = pd.to_datetime(comparison_df['ref_month'])
        comparison_df['shifted_month'] = comparison_df['ref_month'] + pd.DateOffset(months=offset_months)
        
        if 'fx' in selected_series:
            fig.add_trace(go.Scatter(
                x=comparison_df['shifted_month'],
                y=comparison_df['fx_usd_eur_avg_m'],
                name=f'FX USD/EUR ({comparison_period} earlier)',
                mode='lines',
                line=dict(color=colors['fx'], width=1.5, dash='dot'),
                opacity=0.5,
                yaxis='y1'
            ))
        
        if 'brent' in selected_series:
            fig.add_trace(go.Scatter(
                x=comparison_df['shifted_month'],
                y=comparison_df['brent_crude_m'],
                name=f'Brent Crude ({comparison_period} earlier)',
                mode='lines',
                line=dict(color=colors['brent'], width=1.5, dash='dot'),
                opacity=0.5,
                yaxis='y2'
            ))

        if 'natgas' in selected_series:
            fig.add_trace(go.Scatter(
                x=comparison_df['shifted_month'],
                y=comparison_df['nat_gas_m'],
                name=f'Natural Gas ({comparison_period} earlier)',
                mode='lines',
                line=dict(color=colors['natgas'], width=1.5, dash='dot'),
                opacity=0.5,
                yaxis='y2'
            ))
        
        if 'policy' in selected_series:
            fig.add_trace(go.Scatter(
                x=comparison_df['shifted_month'],
                y=comparison_df['policy_rate_eom_m'],
                name=f'Policy Rate ({comparison_period} earlier)',
                mode='lines',
                line=dict(color=colors['policy'], width=1.5, dash='dot'),
                opacity=0.5,
                yaxis='y3'
            ))
    
    fig.update_layout(
        xaxis=dict(title="Month"),
        yaxis=dict(
            title="FX USD/EUR" if 'fx' in selected_series else "",
            side="left",
            showgrid=True
        ),
        yaxis2=dict(
            title="Energy Price (USD)" if ('brent' in selected_series or 'natgas' in selected_series) else "",
            overlaying="y",
            side="right",
            showgrid=False
        ),
        yaxis3=dict(
            title="Policy Rate (%)" if 'policy' in selected_series else "",
            overlaying="y",
            side="right",
            position=0.95,
            showgrid=False
        ),
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400,
        margin=dict(l=50, r=100, t=50, b=50)
    )
    
    return fig

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)