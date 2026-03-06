"""
Ginja AI Claims Intelligence Dashboard
Interactive Streamlit app for healthcare claims analytics
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

@st.cache_resource
def get_connection_string():
    """Create connection string"""
    return "postgresql://postgres:postgres@localhost:5432/ginja_claims"

@st.cache_resource
def get_engine():
    """Create SQLAlchemy engine with connection pooling"""
    try:
        engine = create_engine(
            get_connection_string(),
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600
        )
        return engine
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

def run_query(query):
    """Execute query and return results as DataFrame"""
    try:
        engine = get_engine()
        if engine is None:
            return None
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        return None

st.set_page_config(
    page_title="Ginja AI Claims Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Healthcare Claims Intelligence Dashboard")
st.markdown("Real-time analytics and insights")

with st.sidebar:
    st.header("Navigation")
    
    selected_tab = st.radio(
        "Select Dashboard:",
        ["Executive Summary", "Operations Metrics", "ML Insights"]
    )
    
    st.markdown("---")
    st.markdown("""
    ### Dashboard Features
    - Claims processing and approvals
    - Provider and insurer performance
    - Data quality metrics
    - ML-ready features and patterns
    
    **Data Source:** PostgreSQL  
    **Updated:** Real-time
    """)

if selected_tab == "Executive Summary":
    st.header("Executive Summary")
    st.markdown("High-level KPIs and business metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    query_overview = """
    SELECT 
        COUNT(*) as total_claims,
        COUNT(DISTINCT member_id) as total_members,
        ROUND(AVG(claim_amount)::numeric, 2) as avg_claim,
        ROUND(SUM(approved_amount)::numeric, 2) as total_approved
    FROM claims;
    """
    df_overview = run_query(query_overview)
    
    if df_overview is not None and len(df_overview) > 0:
        col1.metric("Total Claims", f"{int(df_overview['total_claims'].values[0]):,}")
        col2.metric("Total Members", f"{int(df_overview['total_members'].values[0]):,}")
        col3.metric("Avg Claim Amount", f"KES {float(df_overview['avg_claim'].values[0]):,.0f}")
        col4.metric("Total Approved", f"KES {float(df_overview['total_approved'].values[0]):,.0f}")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    query_approval = """
    SELECT 
        status,
        COUNT(*) as count
    FROM claims
    GROUP BY status
    ORDER BY count DESC;
    """
    df_approval = run_query(query_approval)
    
    if df_approval is not None and len(df_approval) > 0:
        with col1:
            st.subheader("Claims by Status")
            fig_approval = px.pie(
                df_approval,
                values='count',
                names='status',
                color_discrete_map={'Approved': '#2ecc71', 'Pending': '#f39c12', 'Rejected': '#e74c3c'},
                hole=0.3
            )
            st.plotly_chart(fig_approval, use_container_width=True)
    
    query_providers = """
    SELECT 
        p.provider_name,
        COUNT(c.claim_id) as total_claims,
        ROUND(AVG(c.claim_amount)::numeric, 2) as avg_claim
    FROM claims c
    LEFT JOIN providers p ON c.provider_id = p.provider_id
    GROUP BY p.provider_name
    ORDER BY total_claims DESC
    LIMIT 10;
    """
    df_providers = run_query(query_providers)
    
    if df_providers is not None and len(df_providers) > 0:
        with col2:
            st.subheader("Top 10 Providers by Volume")
            fig_providers = px.bar(
                df_providers,
                x='total_claims',
                y='provider_name',
                orientation='h',
                color='total_claims',
                color_continuous_scale='Blues'
            )
            fig_providers.update_layout(
                yaxis={'categoryorder': 'total ascending'},
                xaxis_title="Number of Claims",
                yaxis_title=""
            )
            st.plotly_chart(fig_providers, use_container_width=True)
    
    st.markdown("---")
    
    query_trend = """
    SELECT 
        DATE_TRUNC('month', date_of_service)::DATE as month,
        COUNT(*) as claims_count,
        ROUND(AVG(claim_amount)::numeric, 2) as avg_amount
    FROM claims
    GROUP BY DATE_TRUNC('month', date_of_service)
    ORDER BY month;
    """
    df_trend = run_query(query_trend)
    
    if df_trend is not None and len(df_trend) > 0:
        st.subheader("Claims Trend Over Time")
        
        fig_trend = go.Figure()
        fig_trend.add_trace(go.Scatter(
            x=df_trend['month'],
            y=df_trend['claims_count'],
            mode='lines+markers',
            name='Claims Count',
            line=dict(color='#3498db', width=3),
            marker=dict(size=8)
        ))
        fig_trend.update_layout(
            xaxis_title="Month",
            yaxis_title="Number of Claims",
            hovermode='x unified'
        )
        st.plotly_chart(fig_trend, use_container_width=True)

elif selected_tab == "Operations Metrics":
    st.header("Operations Dashboard")
    st.markdown("Process efficiency and data quality metrics")
    
    col1, col2, col3 = st.columns(3)
    
    query_quality = """
    SELECT 
        COUNT(*) as total_records,
        SUM(CASE WHEN claim_amount IS NULL THEN 1 ELSE 0 END) as missing_amounts,
        SUM(CASE WHEN date_of_service IS NULL THEN 1 ELSE 0 END) as missing_dates
    FROM claims;
    """
    df_quality = run_query(query_quality)
    
    if df_quality is not None and len(df_quality) > 0:
        total = int(df_quality['total_records'].values[0])
        missing_amount = int(df_quality['missing_amounts'].values[0])
        missing_date = int(df_quality['missing_dates'].values[0])
        completeness = ((total - missing_amount - missing_date) / total) * 100
        
        col1.metric("Total Records", f"{total:,}")
        col2.metric("Missing Values", f"{missing_amount + missing_date}")
        col3.metric("Data Completeness", f"{completeness:.1f}%")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    query_processing = """
    SELECT 
        status,
        ROUND(AVG(processing_days)::numeric, 2) as avg_days
    FROM claims
    GROUP BY status;
    """
    df_processing = run_query(query_processing)
    
    if df_processing is not None and len(df_processing) > 0:
        with col1:
            st.subheader("Processing Time by Status")
            fig_processing = px.bar(
                df_processing,
                x='status',
                y='avg_days',
                color='status',
                color_discrete_map={'Approved': '#2ecc71', 'Pending': '#f39c12', 'Rejected': '#e74c3c'}
            )
            fig_processing.update_layout(
                xaxis_title="Status",
                yaxis_title="Average Days",
                showlegend=False
            )
            st.plotly_chart(fig_processing, use_container_width=True)
    
    query_status_dist = """
    SELECT 
        status,
        COUNT(*) as count
    FROM claims
    GROUP BY status;
    """
    df_status_dist = run_query(query_status_dist)
    
    if df_status_dist is not None and len(df_status_dist) > 0:
        with col2:
            st.subheader("Claims Count by Status")
            fig_status = px.bar(
                df_status_dist,
                x='status',
                y='count',
                color='status',
                color_discrete_map={'Approved': '#2ecc71', 'Pending': '#f39c12', 'Rejected': '#e74c3c'}
            )
            fig_status.update_layout(
                xaxis_title="Status",
                yaxis_title="Count",
                showlegend=False
            )
            st.plotly_chart(fig_status, use_container_width=True)
    
    st.markdown("---")
    
    st.subheader("Claims Requiring Manual Review")
    
    query_review = """
    SELECT 
        claim_id,
        member_id,
        ROUND(claim_amount::numeric, 2) as claim_amount,
        ROUND(approved_amount::numeric, 2) as approved_amount,
        status
    FROM claims
    WHERE claim_amount > approved_amount
        AND approved_amount IS NOT NULL
    ORDER BY (claim_amount - approved_amount) DESC
    LIMIT 15;
    """
    df_review = run_query(query_review)
    
    if df_review is not None and len(df_review) > 0:
        st.dataframe(df_review, use_container_width=True)

elif selected_tab == "ML Insights":
    st.header("ML Insights Dashboard")
    st.markdown("Features and data distributions for machine learning models")
    
    col1, col2 = st.columns(2)
    
    query_dist = """
    SELECT claim_amount FROM claims WHERE claim_amount IS NOT NULL LIMIT 5000;
    """
    df_dist = run_query(query_dist)
    
    if df_dist is not None and len(df_dist) > 0:
        with col1:
            st.subheader("Claim Amount Distribution")
            fig_dist = px.histogram(
                df_dist,
                x='claim_amount',
                nbins=50,
                color_discrete_sequence=['#3498db']
            )
            fig_dist.update_layout(
                xaxis_title="Claim Amount (KES)",
                yaxis_title="Frequency"
            )
            st.plotly_chart(fig_dist, use_container_width=True)
    
    query_diagnosis = """
    SELECT 
        diagnosis_code,
        COUNT(*) as frequency
    FROM claims
    GROUP BY diagnosis_code
    ORDER BY frequency DESC
    LIMIT 10;
    """
    df_diagnosis = run_query(query_diagnosis)
    
    if df_diagnosis is not None and len(df_diagnosis) > 0:
        with col2:
            st.subheader("Top 10 Diagnosis Codes")
            fig_diagnosis = px.bar(
                df_diagnosis,
                x='frequency',
                y='diagnosis_code',
                orientation='h',
                color='frequency',
                color_continuous_scale='Viridis'
            )
            fig_diagnosis.update_layout(
                yaxis={'categoryorder': 'total ascending'},
                xaxis_title="Frequency",
                yaxis_title="Diagnosis Code"
            )
            st.plotly_chart(fig_diagnosis, use_container_width=True)
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    query_provider_approval = """
    SELECT 
        p.provider_name,
        ROUND(100.0 * SUM(CASE WHEN c.status = 'Approved' THEN 1 ELSE 0 END) / COUNT(c.claim_id)::numeric, 2) as approval_rate,
        COUNT(c.claim_id) as total_claims
    FROM claims c
    JOIN providers p ON c.provider_id = p.provider_id
    GROUP BY p.provider_id, p.provider_name
    ORDER BY approval_rate DESC
    LIMIT 10;
    """
    df_provider_approval = run_query(query_provider_approval)
    
    if df_provider_approval is not None and len(df_provider_approval) > 0:
        with col1:
            st.subheader("Provider Approval Rate (Top 10)")
            fig_provider_approval = px.bar(
                df_provider_approval,
                x='approval_rate',
                y='provider_name',
                orientation='h',
                color='approval_rate',
                color_continuous_scale='RdYlGn'
            )
            fig_provider_approval.update_layout(
                yaxis={'categoryorder': 'total ascending'},
                xaxis_title="Approval Rate (%)",
                yaxis_title=""
            )
            st.plotly_chart(fig_provider_approval, use_container_width=True)
    
    query_plan = """
    SELECT 
        insurance_plan_id,
        COUNT(*) as claim_count,
        ROUND(AVG(claim_amount)::numeric, 2) as avg_claim,
        ROUND(100.0 * SUM(CASE WHEN status = 'Approved' THEN 1 ELSE 0 END) / COUNT(*)::numeric, 2) as approval_rate
    FROM claims
    GROUP BY insurance_plan_id
    ORDER BY claim_count DESC;
    """
    df_plan = run_query(query_plan)
    
    if df_plan is not None and len(df_plan) > 0:
        with col2:
            st.subheader("Insurance Plan Analytics")
            fig_plan = px.scatter(
                df_plan,
                x='claim_count',
                y='approval_rate',
                size='avg_claim',
                color='insurance_plan_id',
                hover_name='insurance_plan_id'
            )
            fig_plan.update_layout(
                xaxis_title="Number of Claims",
                yaxis_title="Approval Rate (%)"
            )
            st.plotly_chart(fig_plan, use_container_width=True)

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #7f8c8d; font-size: 12px;'>
    Healthcare Claims Intelligence Dashboard | Powered by Streamlit & PostgreSQL
</div>
""", unsafe_allow_html=True)