from datetime import datetime
from typing import List
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Try to import mlxtend for association rules
try:
    from mlxtend.frequent_patterns import apriori, association_rules
    from mlxtend.preprocessing import TransactionEncoder
    MLXTEND_AVAILABLE = True
except ImportError:
    MLXTEND_AVAILABLE = False
    print("[ml_service] Warning: mlxtend not available, product associations disabled")

def calculate_rfm(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate RFM (Recency, Frequency, Monetary) metrics for each customer.
    """
    if df.empty:
        return pd.DataFrame()
    
    # Current timestamp for recency calculation
    now = datetime.utcnow()
    
    # Aggregate by customer
    rfm = df.groupby("customer_id").agg({
        "ts": lambda x: (now - x.max()).days,  # Recency: days since last purchase
        "amount": ["count", "sum"]              # Frequency & Monetary
    }).reset_index()
    
    # Flatten column names
    rfm.columns = ["customer_id", "recency_days", "frequency", "monetary"]
    
    # Convert monetary to float
    rfm["monetary"] = rfm["monetary"].astype(float)
    
    return rfm

def assign_rfm_scores(rfm: pd.DataFrame) -> pd.DataFrame:
    """
    Assign RFM scores (1-5) using quartile-based scoring.
    Handles edge cases where there aren't enough unique values for 5 bins.
    """
    if rfm.empty:
        return rfm
    
    def safe_qcut(series, q=5, ascending=True):
        """
        Safely assign quantile-based scores, falling back to simpler methods
        if there aren't enough unique values.
        """
        try:
            if ascending:
                labels = list(range(1, q + 1))
            else:
                labels = list(range(q, 0, -1))
            return pd.qcut(series.rank(method="first"), q=q, labels=labels, duplicates="drop").astype(int)
        except ValueError:
            # Not enough unique values for q bins, try fewer bins
            n_unique = series.nunique()
            if n_unique <= 1:
                # All same value, assign middle score
                return pd.Series([3] * len(series), index=series.index)
            
            # Try with fewer bins
            effective_q = min(q, n_unique)
            try:
                if ascending:
                    labels = [int(1 + (i * 4 / (effective_q - 1))) for i in range(effective_q)]
                else:
                    labels = [int(5 - (i * 4 / (effective_q - 1))) for i in range(effective_q)]
                return pd.qcut(series.rank(method="first"), q=effective_q, labels=labels, duplicates="drop").astype(int)
            except ValueError:
                # Last resort: use simple percentile-based scoring
                percentiles = series.rank(pct=True)
                if ascending:
                    scores = (percentiles * 4 + 1).round().astype(int).clip(1, 5)
                else:
                    scores = (5 - percentiles * 4).round().astype(int).clip(1, 5)
                return scores
    
    # For recency, lower is better (score reversed - low recency = high score)
    rfm["r_score"] = safe_qcut(rfm["recency_days"], q=5, ascending=False)
    
    # For frequency and monetary, higher is better
    rfm["f_score"] = safe_qcut(rfm["frequency"], q=5, ascending=True)
    rfm["m_score"] = safe_qcut(rfm["monetary"], q=5, ascending=True)
    
    return rfm

def assign_segments(rfm: pd.DataFrame) -> pd.DataFrame:
    """
    Assign customer segment labels based on RFM scores.
    """
    if rfm.empty:
        return rfm
    
    def get_segment(row):
        r, f, m = row["r_score"], row["f_score"], row["m_score"]
        
        # Champions: High R, F, M
        if r >= 4 and f >= 4 and m >= 4:
            return "Champions"
        # Loyal Customers: High F and M
        elif f >= 4 and m >= 4:
            return "Loyal Customers"
        # Potential Loyalists: Recent + moderate frequency
        elif r >= 4 and f >= 2:
            return "Potential Loyalists"
        # New Customers: Very recent, low frequency
        elif r >= 4 and f <= 2:
            return "New Customers"
        # At Risk: Used to be good, not recent
        elif r <= 2 and f >= 3:
            return "At Risk"
        # Hibernating: Low R, F, M
        elif r <= 2 and f <= 2:
            return "Hibernating"
        # Need Attention: Middle ground
        else:
            return "Need Attention"
    
    rfm["rfm_segment"] = rfm.apply(get_segment, axis=1)
    return rfm

def apply_kmeans_clustering(rfm: pd.DataFrame, n_clusters: int = 4) -> pd.DataFrame:
    """
    Apply K-Means clustering on RFM features for additional segmentation.
    """
    if rfm.empty or len(rfm) < n_clusters:
        rfm["cluster_id"] = 0
        return rfm
    
    # Features for clustering
    features = rfm[["recency_days", "frequency", "monetary"]].copy()
    
    # Standardize features
    scaler = StandardScaler()
    features_scaled = scaler.fit_transform(features)
    
    # Apply K-Means
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    rfm["cluster_id"] = kmeans.fit_predict(features_scaled)
    
    return rfm

def prepare_basket_data(df: pd.DataFrame) -> List[List[str]]:
    """
    Prepare transaction data for Apriori algorithm.
    Group products by customer to form "baskets".
    """
    if df.empty:
        return []
    
    # Group products by customer
    baskets = df.groupby("customer_id")["product_id"].apply(list).tolist()
    
    # Keep only unique products per basket
    baskets = [list(set(basket)) for basket in baskets]
    
    # Filter out single-item baskets (no associations possible)
    baskets = [b for b in baskets if len(b) >= 2]
    
    return baskets

def find_associations(baskets: List[List[str]], min_support: float = 0.01, min_confidence: float = 0.1) -> pd.DataFrame:
    """
    Find product associations using Apriori algorithm.
    """
    if not baskets or len(baskets) < 5:
        return pd.DataFrame()
    
    if not MLXTEND_AVAILABLE:
        return pd.DataFrame()
    
    try:
        # Encode transactions
        te = TransactionEncoder()
        te_array = te.fit(baskets).transform(baskets)
        df = pd.DataFrame(te_array, columns=te.columns_)
        
        # Find frequent itemsets
        frequent_itemsets = apriori(df, min_support=min_support, use_colnames=True)
        
        if frequent_itemsets.empty:
            return pd.DataFrame()
        
        # Generate association rules
        rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_confidence)
        
        if rules.empty:
            return pd.DataFrame()
        
        # Convert frozensets to strings
        rules["antecedent"] = rules["antecedents"].apply(lambda x: ", ".join(sorted(x)))
        rules["consequent"] = rules["consequents"].apply(lambda x: ", ".join(sorted(x)))
        
        # Select relevant columns
        result = rules[["antecedent", "consequent", "support", "confidence", "lift"]].copy()
        
        # Sort by lift (most interesting associations first)
        result = result.sort_values("lift", ascending=False).head(50)
        
        return result
        
    except Exception as e:
        print(f"[ml_service] Error in association rules: {e}")
        return pd.DataFrame()
