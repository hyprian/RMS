# RMS/analytics_dashboard/charts.py
import pandas as pd
import plotly.express as px
import logging

logger = logging.getLogger(__name__)

def create_sales_trend_chart(trend_df: pd.DataFrame, 
                             y_column: str = 'Net Revenue', 
                             y_column_name: str = 'Net Revenue', # For display in chart
                             title: str = 'Sales Trend Over Time',
                             color_discrete_map: dict = None): # Optional for custom colors
    """
    Creates a line chart for sales trends using Plotly Express.

    Args:
        trend_df (pd.DataFrame): DataFrame with a 'Sale Date' (datetime) index or column,
                                 and the y_column for plotting.
        y_column (str): The name of the column in trend_df to plot on the y-axis.
        y_column_name (str): The display name for the y-axis.
        title (str): The title of the chart.
        color_discrete_map (dict, optional): A dictionary mapping values of a color dimension
                                             to specific colors if a color dimension is used.

    Returns:
        plotly.graph_objects.Figure: The Plotly figure object.
    """
    if trend_df is None or trend_df.empty:
        logger.warning("CHART_GEN: Trend DataFrame is empty. Cannot generate sales trend chart.")
        # Return an empty figure or a figure with a message
        fig = px.line(title="No data available for trend chart.")
        fig.update_layout(xaxis_title="Date", yaxis_title=y_column_name)
        return fig

    if 'Sale Date' not in trend_df.columns:
        logger.error("CHART_GEN: 'Sale Date' column missing in trend DataFrame.")
        fig = px.line(title="Error: 'Sale Date' column missing.")
        fig.update_layout(xaxis_title="Date", yaxis_title=y_column_name)
        return fig
    
    if y_column not in trend_df.columns:
        logger.error(f"CHART_GEN: Y-axis column '{y_column}' missing in trend DataFrame.")
        fig = px.line(title=f"Error: Column '{y_column}' missing.")
        fig.update_layout(xaxis_title="Date", yaxis_title=y_column_name)
        return fig

    # Ensure 'Sale Date' is datetime for proper plotting
    trend_df['Sale Date'] = pd.to_datetime(trend_df['Sale Date'])
    trend_df = trend_df.sort_values(by='Sale Date') # Ensure data is sorted by date

    try:
        fig = px.line(
            trend_df,
            x='Sale Date',
            y=y_column,
            title=title,
            labels={'Sale Date': 'Date', y_column: y_column_name},
            markers=True # Add markers to data points
        )
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title=y_column_name,
            hovermode="x unified" # Shows all data for a given x value on hover
        )
        logger.info(f"CHART_GEN: Successfully generated '{title}' chart for y-column '{y_column}'.")
    except Exception as e:
        logger.error(f"CHART_GEN: Error generating sales trend chart: {e}", exc_info=True)
        fig = px.line(title=f"Error generating chart: {e}")
        fig.update_layout(xaxis_title="Date", yaxis_title=y_column_name)

    return fig


def create_pie_chart(data_df: pd.DataFrame, 
                     names_column: str, 
                     values_column: str, 
                     title: str = 'Distribution Chart',
                     hole: float = 0.3): # For a donut chart effect
    """
    Creates a pie chart using Plotly Express.

    Args:
        data_df (pd.DataFrame): DataFrame containing the data.
        names_column (str): Column name for the pie chart segment names.
        values_column (str): Column name for the pie chart segment values.
        title (str): The title of the chart.
        hole (float): Value between 0 and 1 for donut chart hole size. 0 for standard pie.

    Returns:
        plotly.graph_objects.Figure: The Plotly figure object.
    """
    if data_df is None or data_df.empty:
        logger.warning(f"CHART_GEN: Data for pie chart '{title}' is empty.")
        fig = px.pie(title="No data available for pie chart.")
        return fig

    if names_column not in data_df.columns or values_column not in data_df.columns:
        logger.error(f"CHART_GEN: Required columns ('{names_column}', '{values_column}') missing for pie chart '{title}'.")
        fig = px.pie(title="Error: Missing required columns.")
        return fig
    
    try:
        fig = px.pie(
            data_df,
            names=names_column,
            values=values_column,
            title=title,
            hole=hole
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        logger.info(f"CHART_GEN: Successfully generated pie chart '{title}'.")
    except Exception as e:
        logger.error(f"CHART_GEN: Error generating pie chart '{title}': {e}", exc_info=True)
        fig = px.pie(title=f"Error generating chart: {e}")
    
    return fig

def create_bar_chart(data_df: pd.DataFrame,
                     x_column: str,
                     y_column: str,
                     x_column_name: str = None,
                     y_column_name: str = None,
                     title: str = 'Bar Chart',
                     orientation: str = 'v', # 'v' for vertical, 'h' for horizontal
                     color_column: str = None, # Optional column to color bars by
                     barmode: str = 'group'): # 'group', 'stack', 'relative'
    """
    Creates a bar chart using Plotly Express.
    """
    if data_df is None or data_df.empty:
        logger.warning(f"CHART_GEN: Data for bar chart '{title}' is empty.")
        fig = px.bar(title="No data available for bar chart.")
        return fig

    if x_column not in data_df.columns or y_column not in data_df.columns:
        logger.error(f"CHART_GEN: Required columns ('{x_column}', '{y_column}') missing for bar chart '{title}'.")
        fig = px.bar(title="Error: Missing required columns.")
        return fig
    if color_column and color_column not in data_df.columns:
        logger.warning(f"CHART_GEN: Color column '{color_column}' not found for bar chart '{title}'. Proceeding without color.")
        color_column = None

    try:
        fig = px.bar(
            data_df,
            x=x_column,
            y=y_column,
            title=title,
            orientation=orientation,
            color=color_column,
            barmode=barmode,
            labels={
                x_column: x_column_name if x_column_name else x_column,
                y_column: y_column_name if y_column_name else y_column
            }
        )
        fig.update_layout(hovermode="x unified" if orientation == 'v' else "y unified")
        logger.info(f"CHART_GEN: Successfully generated bar chart '{title}'.")
    except Exception as e:
        logger.error(f"CHART_GEN: Error generating bar chart '{title}': {e}", exc_info=True)
        fig = px.bar(title=f"Error generating chart: {e}")
    
    return fig