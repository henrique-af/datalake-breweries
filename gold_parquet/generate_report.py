import dash
from dash import dcc, html
import pandas as pd
import plotly.express as px
from dash.dependencies import Input, Output

breweries_df = pd.read_parquet('gold_parquet/original_breweries.parquet')

revenue_mapping = {
    'micro': 50000,
    'regional': 200000,
    'brewpub': 100000,
    'large': 500000,
    'planning': 30000,
    'contract': 80000,
    'proprietor': 70000,
    'nano': 25000
}

breweries_df['revenue'] = breweries_df['brewery_type'].map(revenue_mapping)
breweries_df = breweries_df.dropna(subset=['revenue', 'country', 'state'])

app = dash.Dash(__name__)

app.layout = html.Div(
    style={'backgroundColor': '#2c3e50', 'fontFamily': 'Montserrat', 'padding': '20px'},
    children=[
        html.H1(
            "Brewery Business Insights Dashboard",
            style={'color': '#ecf0f1', 'textAlign': 'center', 'marginBottom': '30px'}
        ),
        
        html.Div([
            html.Label('Select Country:', style={'color': '#ecf0f1'}),
            dcc.Dropdown(
                id='country_dropdown',
                options=[{'label': country, 'value': country} for country in breweries_df['country'].unique()],
                value='United States',
                style={'color': '#2c3e50', 'backgroundColor': '#ecf0f1'}
            ),
        ], style={'width': '50%', 'margin': 'auto', 'paddingBottom': '20px'}),

        html.Div([
            dcc.Graph(id='revenue_by_state', config={'displayModeBar': False}),
        ], style={'width': '50%', 'margin': 'auto'}),

        html.Div([
            dcc.Graph(id='revenue_by_brewery_type', config={'displayModeBar': False}),
        ], style={'width': '50%', 'margin': 'auto', 'paddingTop': '20px'})
    ]
)

@app.callback(
    [Output('revenue_by_state', 'figure'),
     Output('revenue_by_brewery_type', 'figure')],
    [Input('country_dropdown', 'value')]
)
def update_graphs(selected_country):
    filtered_df = breweries_df[breweries_df['country'] == selected_country]

    revenue_by_state = filtered_df.groupby('state')['revenue'].sum().reset_index()
    revenue_by_state_fig = px.bar(
        revenue_by_state,
        x='state',
        y='revenue',
        title=f'Revenue by State in {selected_country}',
        labels={'state': 'State', 'revenue': 'Revenue ($)'},
        color_discrete_sequence=['#1abc9c']
    )
    revenue_by_state_fig.update_layout(
        plot_bgcolor='#2c3e50',
        paper_bgcolor='#2c3e50',
        font_color='#ecf0f1'
    )

    revenue_by_brewery_type = filtered_df.groupby('brewery_type')['revenue'].sum().reset_index()
    
    revenue_by_brewery_type_fig = px.pie(
        revenue_by_brewery_type,
        names='brewery_type',
        values='revenue',
        title=f'Revenue Distribution by Brewery Type in {selected_country}',
        color_discrete_sequence=px.colors.sequential.Viridis
    )
    revenue_by_brewery_type_fig.update_layout(
        plot_bgcolor='#2c3e50',
        paper_bgcolor='#2c3e50',
        font_color='#ecf0f1'
    )

    return revenue_by_state_fig, revenue_by_brewery_type_fig

if __name__ == '__main__':
    app.run_server(debug=True)