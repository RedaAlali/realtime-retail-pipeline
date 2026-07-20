import pandas as pd
import altair as alt

# ==============================================================================
# Design Theme Settings (Premium Color Palette)
# ==============================================================================
COLOR_EMERALD = "#10b981"
COLOR_INDIGO  = "#6366f1"
COLOR_ROSE    = "#f43f5e"
COLOR_VIOLET  = "#8b5cf6"
COLOR_AMBER   = "#f59e0b"
COLOR_TEAL    = "#06b6d4"

THEME_SCHEME_TABLEAU = ["#6366f1", "#10b981", "#8b5cf6", "#f59e0b", "#06b6d4", "#ec4899"]

def apply_chart_formatting(chart: alt.Chart) -> alt.Chart:
    """Applies modern styling configurations to an Altair chart."""
    return chart.configure_view(
        stroke=None,
        fill="transparent"
    ).configure_axis(
        gridColor="rgba(255, 255, 255, 0.05)",
        labelColor="rgba(255, 255, 255, 0.6)",
        titleColor="rgba(255, 255, 255, 0.8)",
        labelFont="Inter, Roboto, sans-serif",
        titleFont="Inter, Roboto, sans-serif",
        tickColor="rgba(255, 255, 255, 0.15)",
        domainColor="rgba(255, 255, 255, 0.15)"
    ).configure_legend(
        labelColor="rgba(255, 255, 255, 0.7)",
        titleColor="rgba(255, 255, 255, 0.9)",
        labelFont="Inter, Roboto, sans-serif",
        titleFont="Inter, Roboto, sans-serif"
    )

def revenue_area_chart(df: pd.DataFrame) -> alt.Chart:
    """Create a stacked area chart for revenue over time - smoother than bar chart."""
    chart = (
        alt.Chart(df)
        .mark_area(opacity=0.6, interpolate="monotone", line={"color": "white", "strokeWidth": 1})
        .encode(
            x=alt.X("window_start:T", title="Time Window", axis=alt.Axis(format="%H:%M", tickCount=10)),
            y=alt.Y("total_revenue:Q", title="Revenue ($)", stack="zero"),
            color=alt.Color(
                "product_label:N", 
                title="Product Catalog",
                scale=alt.Scale(range=THEME_SCHEME_TABLEAU)
            ),
            tooltip=[
                alt.Tooltip("window_start:T", title="Time", format="%H:%M"),
                alt.Tooltip("product_label:N", title="Product"),
                alt.Tooltip("total_revenue:Q", title="Revenue", format="$,.2f"),
                alt.Tooltip("transaction_count:Q", title="Transactions"),
            ],
        )
        .properties(height=320)
    )
    return apply_chart_formatting(chart)

def category_pie_chart(df: pd.DataFrame) -> alt.Chart:
    """Create revenue breakdown by category pie chart."""
    category_data = df.groupby("category").agg({
        "total_revenue": "sum"
    }).reset_index()
    
    chart = (
        alt.Chart(category_data)
        .mark_arc(innerRadius=50, outerRadius=90, stroke="rgba(0,0,0,0.1)", strokeWidth=1)
        .encode(
            theta=alt.Theta("total_revenue:Q", title="Revenue"),
            color=alt.Color(
                "category:N", 
                title="Category",
                scale=alt.Scale(range=THEME_SCHEME_TABLEAU)
            ),
            tooltip=[
                alt.Tooltip("category:N", title="Category"),
                alt.Tooltip("total_revenue:Q", title="Revenue", format="$,.2f"),
            ],
        )
        .properties(height=260)
    )
    return apply_chart_formatting(chart)

def top_products_chart(df: pd.DataFrame) -> alt.Chart:
    """Create horizontal bar chart of top products by revenue."""
    product_data = df.groupby(["product_id", "product_label"]).agg({
        "total_revenue": "sum",
        "transaction_count": "sum"
    }).reset_index().sort_values("total_revenue", ascending=False).head(6)
    
    chart = (
        alt.Chart(product_data)
        .mark_bar(cornerRadiusEnd=6, size=18)
        .encode(
            x=alt.X("total_revenue:Q", title="Total Revenue ($)"),
            y=alt.Y("product_label:N", title="", sort="-x"),
            color=alt.Color(
                "product_label:N",
                legend=None,
                scale=alt.Scale(range=THEME_SCHEME_TABLEAU)
            ),
            tooltip=[
                alt.Tooltip("product_label:N", title="Product"),
                alt.Tooltip("total_revenue:Q", title="Revenue", format="$,.2f"),
                alt.Tooltip("transaction_count:Q", title="Transactions"),
            ],
        )
        .properties(height=260)
    )
    return apply_chart_formatting(chart)

def store_performance_chart(df: pd.DataFrame) -> alt.Chart:
    """Create grouped bar chart comparing store performance by region."""
    if df.empty:
        return alt.Chart(pd.DataFrame()).mark_text().encode()
    
    # Create label with store name and city
    df = df.copy()
    df["store_label"] = df["store_name"] + " (" + df["city"] + ")"
    
    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusEnd=6, size=15)
        .encode(
            x=alt.X("total_revenue:Q", title="Total Revenue ($)"),
            y=alt.Y("store_label:N", title="", sort="-x"),
            color=alt.Color(
                "region:N",
                title="Region",
                scale=alt.Scale(range=[COLOR_VIOLET, COLOR_INDIGO, COLOR_TEAL])
            ),
            tooltip=[
                alt.Tooltip("store_name:N", title="Store"),
                alt.Tooltip("city:N", title="City"),
                alt.Tooltip("region:N", title="Region"),
                alt.Tooltip("total_revenue:Q", title="Revenue", format="$,.2f"),
                alt.Tooltip("transaction_count:Q", title="Transactions"),
            ],
        )
        .properties(height=220)
    )
    return apply_chart_formatting(chart)

def cumulative_revenue_chart(df: pd.DataFrame) -> alt.Chart:
    """Create cumulative revenue line chart - shows growth trajectory."""
    # Calculate cumulative revenue over time
    time_data = df.groupby("window_start").agg({
        "total_revenue": "sum"
    }).reset_index().sort_values("window_start")
    
    time_data["cumulative_revenue"] = time_data["total_revenue"].cumsum()
    
    line = (
        alt.Chart(time_data)
        .mark_line(color=COLOR_EMERALD, strokeWidth=3.5, interpolate="monotone")
        .encode(
            x=alt.X("window_start:T", title="Time Window", axis=alt.Axis(format="%H:%M")),
            y=alt.Y("cumulative_revenue:Q", title="Cumulative Revenue ($)"),
            tooltip=[
                alt.Tooltip("window_start:T", title="Time", format="%H:%M"),
                alt.Tooltip("cumulative_revenue:Q", title="Cumulative", format="$,.2f"),
            ],
        )
    )
    
    area = (
        alt.Chart(time_data)
        .mark_area(opacity=0.15, color=COLOR_EMERALD, interpolate="monotone")
        .encode(
            x=alt.X("window_start:T"),
            y=alt.Y("cumulative_revenue:Q"),
        )
    )
    
    chart = (area + line).properties(height=260)
    return apply_chart_formatting(chart)

def transaction_volume_chart(df: pd.DataFrame) -> alt.Chart:
    """Create transaction volume over time - number of transactions per window."""
    time_data = df.groupby("window_start").agg({
        "transaction_count": "sum"
    }).reset_index().sort_values("window_start")
    
    chart = (
        alt.Chart(time_data)
        .mark_bar(color=COLOR_INDIGO, opacity=0.75, cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("window_start:T", title="Time Window", axis=alt.Axis(format="%H:%M")),
            y=alt.Y("transaction_count:Q", title="Transaction Count"),
            tooltip=[
                alt.Tooltip("window_start:T", title="Time", format="%H:%M"),
                alt.Tooltip("transaction_count:Q", title="Transactions"),
            ],
        )
        .properties(height=200)
    )
    return apply_chart_formatting(chart)

def refunds_by_reason_chart(df: pd.DataFrame) -> alt.Chart:
    """Create horizontal bar chart showing refunds by reason."""
    if df.empty:
        return alt.Chart(pd.DataFrame()).mark_text().encode()
    
    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusEnd=6, color=COLOR_ROSE, size=15)
        .encode(
            x=alt.X("total_refunded:Q", title="Total Refunded ($)"),
            y=alt.Y("reason:N", title="", sort="-x"),
            tooltip=[
                alt.Tooltip("reason:N", title="Reason"),
                alt.Tooltip("total_refunded:Q", title="Amount", format="$,.2f"),
                alt.Tooltip("refund_count:Q", title="Count"),
            ],
        )
        .properties(height=220)
    )
    return apply_chart_formatting(chart)

def segment_pie_chart(df: pd.DataFrame) -> alt.Chart:
    """Create customer segment distribution pie chart."""
    chart = (
        alt.Chart(df)
        .mark_arc(innerRadius=60, outerRadius=100, stroke="rgba(0,0,0,0.1)", strokeWidth=1)
        .encode(
            theta=alt.Theta("customer_count:Q", title="Customers"),
            color=alt.Color(
                "rfm_segment:N",
                title="Segment",
                scale=alt.Scale(
                    domain=["Champions", "Loyal Customers", "Potential Loyalists", 
                            "New Customers", "At Risk", "Need Attention", "Hibernating"],
                    range=[COLOR_EMERALD, COLOR_INDIGO, COLOR_VIOLET, COLOR_TEAL, COLOR_ROSE, COLOR_AMBER, "#94a3b8"]
                )
            ),
            tooltip=[
                alt.Tooltip("rfm_segment:N", title="Segment"),
                alt.Tooltip("customer_count:Q", title="Customers"),
                alt.Tooltip("avg_monetary:Q", title="Avg Spend", format="$,.2f"),
            ],
        )
        .properties(height=300)
    )
    return apply_chart_formatting(chart)

def cluster_scatter(df: pd.DataFrame) -> alt.Chart:
    """Create K-Means cluster scatter plot."""
    chart = (
        alt.Chart(df)
        .mark_circle(size=70, opacity=0.8)
        .encode(
            x=alt.X("recency_days:Q", title="Recency (Days Since Last Order)", scale=alt.Scale(reverse=True)),
            y=alt.Y("monetary:Q", title="Monetary (Total Spend $)"),
            color=alt.Color(
                "cluster_id:N", 
                title="K-Means Cluster", 
                scale=alt.Scale(range=[COLOR_EMERALD, COLOR_INDIGO, COLOR_VIOLET, COLOR_AMBER])
            ),
            tooltip=[
                alt.Tooltip("customer_id:N", title="Customer"),
                alt.Tooltip("recency_days:Q", title="Recency (Days)"),
                alt.Tooltip("frequency:Q", title="Frequency (Orders)"),
                alt.Tooltip("monetary:Q", title="Monetary", format="$,.2f"),
                alt.Tooltip("rfm_segment:N", title="Segment"),
            ],
        )
        .properties(height=350)
    )
    return apply_chart_formatting(chart)
