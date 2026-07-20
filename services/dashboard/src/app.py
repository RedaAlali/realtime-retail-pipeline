"""
Real-time Retail Analytics Dashboard

Main layout and UI presentation file.
"""

import streamlit as st
import pandas as pd

# Robust import handling for local running and container setups
try:
    from src.utils import (
        fetch_categories,
        fetch_stores,
        fetch_metrics,
        fetch_customer_segments,
        fetch_segment_summary,
        fetch_product_associations,
        fetch_recent_transactions,
        fetch_store_performance,
        fetch_refunds_summary,
        fetch_refunds_total,
    )
    from src.components import (
        revenue_area_chart,
        category_pie_chart,
        top_products_chart,
        store_performance_chart,
        cumulative_revenue_chart,
        transaction_volume_chart,
        refunds_by_reason_chart,
        segment_pie_chart,
        cluster_scatter,
    )
except ImportError:
    from utils import (
        fetch_categories,
        fetch_stores,
        fetch_metrics,
        fetch_customer_segments,
        fetch_segment_summary,
        fetch_product_associations,
        fetch_recent_transactions,
        fetch_store_performance,
        fetch_refunds_summary,
        fetch_refunds_total,
    )
    from components import (
        revenue_area_chart,
        category_pie_chart,
        top_products_chart,
        store_performance_chart,
        cumulative_revenue_chart,
        transaction_volume_chart,
        refunds_by_reason_chart,
        segment_pie_chart,
        cluster_scatter,
    )

def inject_custom_styles():
    """Injects high-end, responsive custom styling and typography."""
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Font bindings */
    html, body, [data-testid="stSidebar"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }
    
    /* Premium Glassmorphic Metric Cards */
    .kpi-card {
        background: rgba(255, 255, 255, 0.02);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.07);
        border-radius: 16px;
        padding: 22px;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.15);
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        position: relative;
        overflow: hidden;
    }
    .kpi-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        width: 100%;
        height: 4px;
        background: linear-gradient(90deg, #6366f1, #10b981);
    }
    .kpi-card.refund-card::before {
        background: linear-gradient(90deg, #f43f5e, #f59e0b);
    }
    .kpi-card:hover {
        transform: translateY(-4px);
        background: rgba(255, 255, 255, 0.04);
        border-color: rgba(255, 255, 255, 0.14);
        box-shadow: 0 12px 40px 0 rgba(0, 0, 0, 0.25);
    }
    .kpi-title {
        font-size: 0.8125rem;
        font-weight: 600;
        color: rgba(255, 255, 255, 0.45);
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 6px;
    }
    .kpi-value {
        font-size: 1.875rem;
        font-weight: 700;
        color: #ffffff;
        letter-spacing: -0.03em;
        line-height: 1.1;
        margin-bottom: 6px;
    }
    .kpi-sub {
        font-size: 0.75rem;
        font-weight: 500;
    }
    .kpi-sub-positive {
        color: #10b981;
    }
    .kpi-sub-negative {
        color: #f43f5e;
    }
    
    /* Live Status Pulsing Dots */
    .status-badge {
        display: inline-flex;
        align-items: center;
        gap: 0.6rem;
        background: rgba(16, 185, 129, 0.08);
        border: 1px solid rgba(16, 185, 129, 0.18);
        padding: 5px 12px;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 600;
        color: #10b981;
        letter-spacing: 0.03em;
        margin-bottom: 1.25rem;
        text-transform: uppercase;
    }
    .pulse-dot {
        width: 8px;
        height: 8px;
        background-color: #10b981;
        border-radius: 50%;
        box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.6);
        animation: pulsing 1.8s infinite;
    }
    @keyframes pulsing {
        0% {
            transform: scale(0.92);
            box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7);
        }
        70% {
            transform: scale(1);
            box-shadow: 0 0 0 6px rgba(16, 185, 129, 0);
        }
        100% {
            transform: scale(0.92);
            box-shadow: 0 0 0 0 rgba(16, 185, 129, 0);
        }
    }
    </style>
    """, unsafe_allow_html=True)

def render_kpi_card(title: str, value: str, subtext: str = "", is_negative: bool = False, is_refund: bool = False):
    """Helper to render a beautiful HTML metric card using the custom CSS classes."""
    sub_class = "kpi-sub-negative" if is_negative else "kpi-sub-positive"
    card_class = "kpi-card refund-card" if is_refund else "kpi-card"
    st.markdown(f"""
    <div class="{card_class}">
        <div class="kpi-title">{title}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-sub"><span class="{sub_class}">{subtext}</span></div>
    </div>
    """, unsafe_allow_html=True)

def main():
    st.set_page_config(
        page_title="Retail Real-time Dashboard",
        page_icon="️",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Inject Inter fonts and custom glassmorphism styles
    inject_custom_styles()
    
    # Fetch option lists for sidebar dropdown selectors
    categories_list = fetch_categories()
    stores_list = fetch_stores()
    store_names = [s["name"] for s in stores_list]
    store_id_map = {s["name"]: s["id"] for s in stores_list}
    
    # =========================================================================
    # SIDEBAR CONTROLS
    # =========================================================================
    with st.sidebar:
        st.header(" Live Filters")
        
        selected_store_name = st.selectbox(
            "Store Location",
            options=store_names,
            index=0,
            help="Filter all metrics by a specific store location."
        )
        selected_store_id = store_id_map[selected_store_name]
        
        selected_category = st.selectbox(
            "Product Category",
            options=categories_list,
            index=0,
            help="Filter all metrics by product category."
        )
        
        hours_back = st.number_input(
            "Time Range (Hours)",
            min_value=1,
            max_value=168,
            value=24,
            step=1,
            help="Set how far back to look for historical records."
        )
        
        st.divider()
        
        st.markdown("###  Live Feed Status")
        st.markdown("""
        <div class="status-badge">
            <div class="pulse-dot"></div>
            <span>Kafka Connected</span>
        </div>
        """, unsafe_allow_html=True)
        st.caption("Auto-refreshes every 10 seconds.")
        
    # =========================================================================
    # HEADER SECTION
    # =========================================================================
    st.markdown("# Real-time Retail Analytics Dashboard")
    st.markdown("""
    Monitoring live streams of transactions and refunds from **Apache Kafka** processed via **Spark Streaming** into **PostgreSQL**.
    """)

    # =========================================================================
    # DATA LOADING
    # =========================================================================
    metrics_df = fetch_metrics(int(hours_back), selected_category, selected_store_id)
    segments_df = fetch_customer_segments()
    segment_summary = fetch_segment_summary()
    associations_df = fetch_product_associations()
    recent_tx_df = fetch_recent_transactions(limit=50, category=selected_category, store_id=selected_store_id)
    store_perf_df = fetch_store_performance()
    refunds_summary_df = fetch_refunds_summary(store_id=selected_store_id)
    refunds_total = fetch_refunds_total(store_id=selected_store_id)

    # Compute key stats for selected window and filters
    total_revenue = float(metrics_df["total_revenue"].sum()) if not metrics_df.empty else 0.0
    total_tx = int(metrics_df["transaction_count"].sum()) if not metrics_df.empty else 0
    total_refunds = refunds_total["amount"]
    net_revenue = total_revenue - total_refunds
    total_customers = len(segments_df) if not segments_df.empty else 0
    refund_rate = (total_refunds / total_revenue * 100) if total_revenue > 0 else 0.0

    # =========================================================================
    # RENDER KPI METRIC CARDS
    # =========================================================================
    kpi_cols = st.columns(5)
    with kpi_cols[0]:
        render_kpi_card("Gross Revenue", f"${total_revenue:,.2f}", "Selected Time & Filters")
    with kpi_cols[1]:
        render_kpi_card("Refunds Amount", f"${total_refunds:,.2f}", f"{refund_rate:.1f}% refund rate", is_negative=True, is_refund=True)
    with kpi_cols[2]:
        render_kpi_card("Net Revenue", f"${net_revenue:,.2f}", "Gross minus Refunds")
    with kpi_cols[3]:
        render_kpi_card("Transactions", f"{total_tx:,}", "Total orders count")
    with kpi_cols[4]:
        render_kpi_card("Customers (ML)", f"{total_customers:,}", "Analyzed in star schema")

    st.markdown("<br>", unsafe_allow_html=True)

    # =========================================================================
    # ORGANIZE SECTIONS VIA TABS
    # =========================================================================
    tab1, tab2, tab3 = st.tabs([
        " Live Sales Monitor", 
        " Store & Refunds Operations", 
        " Predictive ML Insights"
    ])

    # ----------------- TAB 1: LIVE SALES MONITOR -----------------
    with tab1:
        st.subheader("Sales Activity & Revenue Trajectory")
        st.markdown("*Real-time incoming transaction metrics filtered by active selections*")
        
        if metrics_df.empty:
            st.info("Waiting for real-time transaction data stream... Make sure the generator and Spark are running.")
        else:
            chart_df = metrics_df.copy()
            chart_df["product_label"] = chart_df["product_name"].fillna(chart_df["product_id"])
            
            # 1. Main Line Chart
            st.markdown("### Revenue Over Time")
            st.altair_chart(revenue_area_chart(chart_df), use_container_width=True)
            
            # 2. Side-by-side break downs
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("### Sales by Category")
                st.altair_chart(category_pie_chart(chart_df), use_container_width=True)
            with col2:
                st.markdown("### Revenue Growth Trajectory")
                st.altair_chart(cumulative_revenue_chart(chart_df), use_container_width=True)
                
            # 3. Transaction Volume expander
            with st.expander("Show Transaction Count Volumes", expanded=False):
                st.altair_chart(transaction_volume_chart(chart_df), use_container_width=True)
        
        st.divider()
        
        # Collapsible Raw logs table
        with st.expander(f"View Live Transaction Log (Last 50, filtered)", expanded=False):
            st.markdown("*Raw events captured in PostgreSQL database*")
            if recent_tx_df.empty:
                st.info("No recent transactions found matching the filters.")
            else:
                st.dataframe(
                    recent_tx_df.rename(columns={
                        "ts": "Timestamp",
                        "transaction_id": "Transaction ID",
                        "product_id": "Product ID",
                        "product_name": "Product Name",
                        "category": "Category",
                        "store_id": "Store ID",
                        "customer_id": "Customer ID",
                        "amount": "Amount ($)"
                    }),
                    use_container_width=True,
                    hide_index=True
                )

    # ----------------- TAB 2: OPERATIONS OVERVIEW -----------------
    with tab2:
        st.subheader("Regional Performance & Return Logistics")
        st.markdown("*Analysis of physical store channels and refund rates*")
        
        ops_col1, ops_col2 = st.columns(2)
        
        # Left Column: Refunds
        with ops_col1:
            st.markdown("### Refund Log Analysis")
            st.caption("Source: Kafka refunds stream")
            if refunds_summary_df.empty:
                st.info("No refunds reported in this store selection.")
            else:
                st.altair_chart(refunds_by_reason_chart(refunds_summary_df), use_container_width=True)
                
                with st.expander("View Reason Details"):
                    st.dataframe(
                        refunds_summary_df.rename(columns={
                            "reason": "Return Reason",
                            "refund_count": "Count",
                            "total_refunded": "Amount ($)"
                        }),
                        hide_index=True,
                        use_container_width=True
                    )
                    
        # Right Column: Stores
        with ops_col2:
            st.markdown("### Global Store Comparison")
            st.caption("Source: store_locations dimension metadata")
            if store_perf_df.empty:
                st.info("No store details found. Make sure PostgreSQL is initialized.")
            else:
                st.altair_chart(store_performance_chart(store_perf_df), use_container_width=True)
                
                with st.expander("View Store Standings"):
                    st.dataframe(
                        store_perf_df.rename(columns={
                            "store_id": "Store ID",
                            "store_name": "Name",
                            "city": "City",
                            "region": "Region",
                            "transaction_count": "Transactions",
                            "total_revenue": "Revenue ($)"
                        }),
                        hide_index=True,
                        use_container_width=True
                    )

    # ----------------- TAB 3: MACHINE LEARNING INSIGHTS -----------------
    with tab3:
        st.subheader("Customer Intelligence & Market Basket Recommendations")
        st.markdown("*Unsupervised clustering models and transaction association metrics*")
        
        ml_subtab1, ml_subtab2 = st.tabs(["Customer RFM Clustering", "Product Recommendation Rules"])
        
        # A. Customer RFM tab
        with ml_subtab1:
            st.markdown("""
            **RFM Clustering** leverages K-Means clustering to partition customers based on:
            * **R**ecency: Days since last order.
            * **F**requency: Total order volume.
            * **M**onetary: Total lifetime value (LTV).
            """)
            
            if segments_df.empty:
                st.info("Customer segments database is empty. The ML service needs at least 10 active customers to run.")
            else:
                seg_col1, seg_col2 = st.columns(2)
                with seg_col1:
                    st.markdown("#### Segment Distribution")
                    if not segment_summary.empty:
                        st.altair_chart(segment_pie_chart(segment_summary), use_container_width=True)
                with seg_col2:
                    st.markdown("#### Customer RFM Cluster Scatter Plot")
                    st.altair_chart(cluster_scatter(segments_df.head(100)), use_container_width=True)
                
                # Standings Table
                st.markdown("#### Segment Cohorts Statistics")
                if not segment_summary.empty:
                    st.dataframe(
                        segment_summary.rename(columns={
                            "rfm_segment": "Cohorts Segment",
                            "customer_count": "Shoppers Count",
                            "avg_monetary": "Average Spend ($)",
                            "avg_frequency": "Average Order Frequency",
                            "avg_recency": "Avg Days Since Last Order"
                        }),
                        use_container_width=True,
                        hide_index=True
                    )
                
                with st.expander("Show Top 10 Value Shoppers"):
                    top_shopper_table = segments_df.head(10)[["customer_id", "rfm_segment", "monetary", "frequency", "recency_days"]]
                    st.dataframe(
                        top_shopper_table.rename(columns={
                            "customer_id": "Customer ID",
                            "rfm_segment": "Cluster Cohort",
                            "monetary": "Spent ($)",
                            "frequency": "Orders",
                            "recency_days": "Days Since Last"
                        }),
                        use_container_width=True,
                        hide_index=True
                    )
                    
        # B. Product recommendations tab
        with ml_subtab2:
            st.markdown("""
            **Market Basket Analysis** finds correlation pairs bought together inside the same shopping transactions using the **Apriori Algorithm**.
            * **Support**: Percentage of baskets containing both products.
            * **Confidence**: Likelihood that product B is purchased when product A is purchased.
            * **Lift**: Strength of the rule (>1 indicates highly relevant correlation).
            """)
            
            if associations_df.empty:
                st.info("Product associations table is empty. The ML service requires multi-item orders to resolve correlation patterns.")
            else:
                st.dataframe(associations_df, use_container_width=True, hide_index=True)
                
                if len(associations_df) > 0:
                    top_rule = associations_df.iloc[0]
                    st.success(
                        f" **Top Association Rule Found:** Shoppers who bought **{top_rule['If Customer Buys']}** "
                        f"are **{top_rule['Lift']:.1f}x** more likely to also purchase **{top_rule['They Also Buy']}** "
                        f"(Confidence: {top_rule['Confidence %']}%)"
                    )

    st.divider()
    st.caption("Data feeds automatically poll every 10 seconds | Refactored with custom CSS glassmorphism widgets.")

if __name__ == "__main__":
    main()
