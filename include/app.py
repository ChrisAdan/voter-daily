"""
Voter Analytics Dashboard
At-a-glance partisan demographics and engagement analysis
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# Page config
st.set_page_config(
    page_title="Voter Analytics Dashboard",
    page_icon="🗳️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Color scheme - Gray for Independent
PARTY_COLORS = {
    'Democrat': '#1f77b4',
    'Republican': '#d62728', 
    'Independent': '#808080'
}

# Database connection
@st.cache_resource
def get_db_connection():
    """Connect to DuckDB database"""
    db_path = os.path.join(os.path.dirname(__file__), 'data', 'goodparty_prod.duckdb')
    return duckdb.connect(db_path)

# Data loading functions
@st.cache_data
def load_state_summary():
    """Load data from state_summary mart table"""
    conn = get_db_connection()
    query = """
    SELECT *
    FROM prod_mart.state_summary
    ORDER BY total_registered_voters DESC
    """
    return conn.execute(query).df()

@st.cache_data
def load_targeting_opportunities():
    """Load data from targeting_opportunities mart table"""
    conn = get_db_connection()
    query = """
    SELECT *
    FROM prod_mart.targeting_opportunities
    WHERE targeting_tier IN ('High Priority', 'Medium Priority')
    ORDER BY opportunity_score DESC
    LIMIT 20
    """
    return conn.execute(query).df()

@st.cache_data
def load_stage_voter_metrics():
    """Load data from stage_voter_metrics for trend analysis"""
    conn = get_db_connection()
    query = """
    SELECT *
    FROM prod_stage.stage_voter_metrics
    ORDER BY registered_date
    """
    return conn.execute(query).df()

@st.cache_data
def load_voter_snapshot():
    """Load data from voter_snapshot mart table"""
    conn = get_db_connection()
    query = """
    SELECT *
    FROM prod_mart.voter_snapshot
    ORDER BY total_voters DESC
    """
    return conn.execute(query).df()

@st.cache_data
def load_partisan_trends():
    """Load data from partisan_trends mart table"""
    conn = get_db_connection()
    query = """
    SELECT *
    FROM prod_mart.partisan_trends
    """
    return conn.execute(query).df()

# Dashboard header
st.title("🗳️ Voter Analytics Dashboard")
st.markdown("**At-a-glance partisan demographics and engagement analysis**")

# Load data
try:
    state_summary = load_state_summary()
    targeting_ops = load_targeting_opportunities()
    partisan_trends = load_partisan_trends()
    stage_voter_metrics = load_stage_voter_metrics()
    voter_snapshot = load_voter_snapshot()
    
    # === TOP STATS BLOCK ===
    col1, col2, col3 = st.columns(3)
    
    total_voters = state_summary['total_registered_voters'].sum()
    avg_engagement_all_time = state_summary['pct_current_voters'].mean()
    
    with col1:
        st.metric("Total Registered Voters", f"{total_voters:,.0f}")
    with col2:
        st.metric("Average Voter Engagement (All Time)", f"{avg_engagement_all_time:.1f}%")
    
    with col3:
        # Voter engagement trend line chart
        if not partisan_trends.empty:
            engagement_trend = partisan_trends[partisan_trends['participation_rate'] > 0].groupby(
                ['election_year', 'party']
            )['participation_rate'].mean().reset_index()
            engagement_trend.columns = ['election_year', 'party', 'avg_participation_rate']
            
            fig_engagement = px.line(
                engagement_trend,
                x='election_year',
                y='avg_participation_rate',
                color='party',
                title="Voter Engagement Trends",
                color_discrete_map=PARTY_COLORS,
                markers=True,
                height=200
            )
            fig_engagement.update_layout(
                xaxis_title="",
                yaxis_title="Participation %",
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.3,
                    xanchor="center",
                    x=0.5
                ),
                margin=dict(l=0, r=0, t=30, b=40)
            )
            st.plotly_chart(fig_engagement, use_container_width=True)
        else:
            st.metric("Voter Engagement Trends", "Data N/A")
    
    st.divider()
    
    # === TOP OPPORTUNITIES SECTION ===
    col_left, col_right = st.columns([1, 1])
    
    with col_left:
        st.subheader("🎯 Top Targeting Opportunities")
        
        if not targeting_ops.empty:
            # Fix opportunity score display if needed
            max_score = targeting_ops['opportunity_score'].max()
            if max_score > 100:
                targeting_ops['opportunity_score_display'] = (targeting_ops['opportunity_score'] / max_score * 100).round(1)
            else:
                targeting_ops['opportunity_score_display'] = targeting_ops['opportunity_score'].round(1) / 100
                
            top_ops = targeting_ops.head(10)[['state', 'age_group', 'party', 'opportunity_score_display']]
            
            st.dataframe(
                top_ops,
                column_config={
                    'state': 'State',
                    'age_group': 'Age Group', 
                    'party': 'Party',
                    'opportunity_score_display': st.column_config.ProgressColumn(
                        'Opportunity Score',
                        min_value=0,
                        max_value=1
                    )
                },
                hide_index=True,
                height=400
            )
        else:
            st.info("Targeting opportunities data not available")
    
    with col_right:
        st.subheader("📍 State Engagement Opportunities")
        
        # State engagement opportunities as heatmap
        top_states = state_summary.nlargest(15, 'engagement_opportunity_score')[
            ['state', 'engagement_opportunity_score', 'pct_recoverable_voters']
        ].round(1)
        
        # Create a grid layout (3x5 for 15 states)
        import numpy as np
        n_states = len(top_states)
        n_cols = 3
        n_rows = 5
        
        # Create grid positions
        grid_data = []
        for i, (idx, row) in enumerate(top_states.iterrows()):
            grid_row = i // n_cols
            grid_col = i % n_cols
            grid_data.append({
                'state': row['state'],
                'grid_row': grid_row,
                'grid_col': grid_col,
                'engagement_opportunity_score': row['engagement_opportunity_score'],
                'pct_recoverable_voters': row['pct_recoverable_voters'],
                'label': f"{row['state']}<br>{row['pct_recoverable_voters']:.1f}% rec<br>{row['engagement_opportunity_score']:.1f} eng"
            })
        
        grid_df = pd.DataFrame(grid_data)
        
        fig_heatmap = px.scatter(
            grid_df,
            x='grid_col',
            y='grid_row',
            color='engagement_opportunity_score',
            size='pct_recoverable_voters',
            text='label',
            title="",
            height=400,
            color_continuous_scale='Sunset',
            size_max=50
        )
        
        fig_heatmap.update_traces(
            marker=dict(
                symbol='square',
                line=dict(width=2, color='white')
            ),
            textposition="middle center",
            textfont=dict(size=9, color='white', family="Arial Black")
        )
        
        fig_heatmap.update_layout(
            xaxis=dict(
                showgrid=False,
                showticklabels=False,
                zeroline=False,
                title=""
            ),
            yaxis=dict(
                showgrid=False,
                showticklabels=False,
                zeroline=False,
                title="",
                autorange="reversed"  # Top to bottom
            ),
            plot_bgcolor='rgba(0,0,0,0)',
            showlegend=False,
            coloraxis_colorbar=dict(
                title="Opportunity<br>Score",
                tickfont=dict(size=10)
            )
        )
        
        st.plotly_chart(fig_heatmap, use_container_width=True)
    
    st.divider()
    
# === TARGETING BY ENGAGEMENT SECTION ===
    st.subheader("🎯 Targeting by Engagement & Demographics")
    
    if not voter_snapshot.empty:
        col_left_engage, col_right_engage = st.columns([1, 1])
        
        with col_left_engage:
            # Gender-engagement diverging bar chart
            gender_engagement = voter_snapshot.groupby(['gender', 'voter_engagement_segment', 'party'])['total_voters'].sum().reset_index()
            
            # Pivot to get M and F as separate columns
            engagement_pivot = gender_engagement.pivot_table(
                index=['voter_engagement_segment', 'party'], 
                columns='gender', 
                values='total_voters', 
                fill_value=0
            ).reset_index()
            
            # Make Male values negative for left side
            if 'M' in engagement_pivot.columns:
                engagement_pivot['M'] = -engagement_pivot['M']
            
            # Sort by engagement segment for logical ordering
            segment_order = ['Current Voter', 'Missed Last Election', 'Occasional Voter', 'Infrequent Voter', 'Dormant Voter', 'Never Voted']
            engagement_pivot['segment_order'] = engagement_pivot['voter_engagement_segment'].map(
                {seg: i for i, seg in enumerate(segment_order)}
            )
            engagement_pivot = engagement_pivot.sort_values('segment_order')
            
            fig_engagement_gender = go.Figure()
            
            # Add bars for each party
            for party in engagement_pivot['party'].unique():
                party_data = engagement_pivot[engagement_pivot['party'] == party]
                
                # Male bars (negative, extending left)
                if 'M' in engagement_pivot.columns:
                    fig_engagement_gender.add_trace(go.Bar(
                        name=f'{party} (M)',
                        y=party_data['voter_engagement_segment'],
                        x=party_data['M'],
                        orientation='h',
                        marker_color=PARTY_COLORS[party],
                        opacity=0.7,
                        showlegend=False,
                        hovertemplate=f'{party} Male: %{{x:,.0f}}<extra></extra>'
                    ))
                
                # Female bars (positive, extending right)  
                if 'F' in engagement_pivot.columns:
                    fig_engagement_gender.add_trace(go.Bar(
                        name=f'{party}',
                        y=party_data['voter_engagement_segment'],
                        x=party_data['F'],
                        orientation='h',
                        marker_color=PARTY_COLORS[party],
                        opacity=1.0,
                        showlegend=True,
                        legendgroup=party,
                        hovertemplate=f'{party} Female: %{{x:,.0f}}<extra></extra>'
                    ))
            
            fig_engagement_gender.update_layout(
                title="Gender & Engagement by Party",
                xaxis_title="← Male    Voters    Female →",
                yaxis_title="Engagement Segment",
                barmode='relative',
                height=400,
                xaxis=dict(
                    zeroline=True,
                    zerolinewidth=2,
                    zerolinecolor='black'
                ),
                yaxis=dict(
                    autorange="reversed" 
                ),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="center",
                    x=0.5
                )
            )
            
            st.plotly_chart(fig_engagement_gender, use_container_width=True)
        
        with col_right_engage:
            # Side-by-side M vs F pie charts for party affiliation
            gender_party_totals = voter_snapshot.groupby(['gender', 'party'])['total_voters'].sum().reset_index()
            
            # Create subplot with 1 row, 2 columns for side-by-side pie charts
            from plotly.subplots import make_subplots
            
            fig_gender_pie = make_subplots(
                rows=1, cols=2,
                specs=[[{'type':'domain'}, {'type':'domain'}]],
                subplot_titles=('Male Voters', 'Female Voters'),
                horizontal_spacing=0.1
            )
            
            # Male pie chart
            male_data = gender_party_totals[gender_party_totals['gender'] == 'M']
            if not male_data.empty:
                fig_gender_pie.add_trace(go.Pie(
                    labels=male_data['party'],
                    values=male_data['total_voters'],
                    name="Male",
                    marker_colors=[PARTY_COLORS[party] for party in male_data['party']],
                    hovertemplate='%{label}: %{value:,.0f}<br>%{percent}<extra></extra>',
                    showlegend=True,
                    legendgroup="parties"
                ), 1, 1)
            
            # Female pie chart  
            female_data = gender_party_totals[gender_party_totals['gender'] == 'F']
            if not female_data.empty:
                fig_gender_pie.add_trace(go.Pie(
                    labels=female_data['party'],
                    values=female_data['total_voters'],
                    name="Female",
                    marker_colors=[PARTY_COLORS[party] for party in female_data['party']],
                    hovertemplate='%{label}: %{value:,.0f}<br>%{percent}<extra></extra>',
                    showlegend=False  # Hide legend for second pie to avoid duplication
                ), 1, 2)
            
            fig_gender_pie.update_layout(
                title_text="Party Affiliation by Gender",
                title_x=0.5,
                height=400,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.1,
                    xanchor="center",
                    x=0.5
                )
            )
            
            st.plotly_chart(fig_gender_pie, use_container_width=True)
            
    else:
        st.info("Voter snapshot data not available for engagement analysis")

    st.divider()
    
    # === ANALYTICS SECTION ===
    st.subheader("📊 Voter Analytics")
    
    col1, col2, col3 = st.columns(3)
    
    if not stage_voter_metrics.empty:
        # Convert registered_date to datetime and extract year
        stage_voter_metrics['registered_date'] = pd.to_datetime(stage_voter_metrics['registered_date'])
        stage_voter_metrics['registration_year'] = stage_voter_metrics['registered_date'].dt.year
        
        with col1:
            # Registered voters by registration year
            voters_by_year = stage_voter_metrics.groupby('registration_year').size().reset_index(name='registered_voters')
            
            fig_trend = px.line(
                voters_by_year,
                x='registration_year',
                y='registered_voters',
                title="Voter Registrations by Year",
                markers=True
            )
            fig_trend.update_traces(line_color='#1f77b4', marker_color='#1f77b4')
            fig_trend.update_layout(
                xaxis_title="Registration Year",
                yaxis_title="New Registrations",
                height=400
            )
            st.plotly_chart(fig_trend, use_container_width=True)
        
        with col2:
            # Partisanship by gender over registration year - horizontal diverging bars
            gender_party_year = stage_voter_metrics.groupby(['registration_year', 'gender', 'party']).size().reset_index(name='voters')
            
            # Pivot to get M and F as separate columns
            gender_pivot = gender_party_year.pivot_table(
                index=['registration_year', 'party'], 
                columns='gender', 
                values='voters', 
                fill_value=0
            ).reset_index()
            
            # Make Male values negative for left side
            if 'M' in gender_pivot.columns:
                gender_pivot['M'] = -gender_pivot['M']
            
            # Sort by registration year descending (newest at top)
            gender_pivot = gender_pivot.sort_values('registration_year', ascending=False)
            
            fig_gender_trend = go.Figure()
            
            # Add bars for each party
            for party in gender_pivot['party'].unique():
                party_data = gender_pivot[gender_pivot['party'] == party]
                
                # Male bars (negative, extending left)
                if 'M' in gender_pivot.columns:
                    fig_gender_trend.add_trace(go.Bar(
                        name=f'{party} (M)',
                        y=party_data['registration_year'].astype(str),
                        x=party_data['M'],
                        orientation='h',
                        marker_color=PARTY_COLORS[party],
                        opacity=0.7,
                        showlegend=False,
                        hovertemplate=f'{party} Male: %{{x}}<extra></extra>'
                    ))
                
                # Female bars (positive, extending right)  
                if 'F' in gender_pivot.columns:
                    fig_gender_trend.add_trace(go.Bar(
                        name=f'{party}',
                        y=party_data['registration_year'].astype(str),
                        x=party_data['F'],
                        orientation='h',
                        marker_color=PARTY_COLORS[party],
                        opacity=1.0,
                        showlegend=True,
                        legendgroup=party,
                        hovertemplate=f'{party} Female: %{{x}}<extra></extra>'
                    ))
            
            fig_gender_trend.update_layout(
                title="Partisanship by Gender Over Time",
                xaxis_title="← Male    Voters    Female →",
                yaxis_title="Registration Year",
                barmode='relative',
                height=400,
                xaxis=dict(
                    zeroline=True,
                    zerolinewidth=2,
                    zerolinecolor='black'
                ),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="center",
                    x=0.5
                )
            )
            
            st.plotly_chart(fig_gender_trend, use_container_width=True)
        
        with col3:
            # Partisanship by age group - stacked bar similar to gender chart
            party_by_age = stage_voter_metrics.groupby(['age_group', 'party']).size().reset_index(name='voters')
            
            # Pivot to get parties as separate columns for stacking
            age_pivot = party_by_age.pivot_table(
                index='age_group', 
                columns='party', 
                values='voters', 
                fill_value=0
            ).reset_index()
            
            fig_age_party = go.Figure()
            
            # Add stacked bars for each party
            for party in ['Democrat', 'Republican', 'Independent']:
                if party in age_pivot.columns:
                    fig_age_party.add_trace(go.Bar(
                        name=party,
                        x=age_pivot['age_group'],
                        y=age_pivot[party],
                        marker_color=PARTY_COLORS[party],
                        hovertemplate=f'{party}: %{{y}}<extra></extra>'
                    ))
            
            fig_age_party.update_layout(
                title="Partisanship by Age Group",
                xaxis_title="Age Group",
                yaxis_title="Registered Voters",
                barmode='stack',
                height=400,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="center",
                    x=0.5
                )
            )
            
            st.plotly_chart(fig_age_party, use_container_width=True)
    
    else:
        st.info("Stage voter metrics data not available for trend analysis")

except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.info("Please ensure the DuckDB database exists with required tables")
    
    # Show expected data structure
    st.subheader("Expected Data Structure")
    st.code("""
    Expected tables in DuckDB:
    - prod_mart.state_summary
    - prod_mart.targeting_opportunities
    - prod_mart.partisan_trends
    - prod_mart.voter_snapshot
    - prod_stage.stage_voter_metrics
    """)

# Footer
st.divider()
st.caption("Generated from voter registration and participation data | Last updated: Today")