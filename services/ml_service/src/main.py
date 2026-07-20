"""
ML Service: Customer Segmentation & Product Recommendations

Main orchestration loop.
"""

import time

try:
    from src.database import (
        get_connection,
        ensure_tables_exist,
        fetch_transactions_for_rfm,
        upsert_customer_segments,
        fetch_transactions_for_basket,
        upsert_product_associations,
        POLL_SECONDS,
        MIN_TRANSACTIONS,
        N_CLUSTERS,
    )
    from src.algorithms import (
        calculate_rfm,
        assign_rfm_scores,
        assign_segments,
        apply_kmeans_clustering,
        prepare_basket_data,
        find_associations,
        MLXTEND_AVAILABLE,
    )
except ImportError:
    from database import (
        get_connection,
        ensure_tables_exist,
        fetch_transactions_for_rfm,
        upsert_customer_segments,
        fetch_transactions_for_basket,
        upsert_product_associations,
        POLL_SECONDS,
        MIN_TRANSACTIONS,
        N_CLUSTERS,
    )
    from algorithms import (
        calculate_rfm,
        assign_rfm_scores,
        assign_segments,
        apply_kmeans_clustering,
        prepare_basket_data,
        find_associations,
        MLXTEND_AVAILABLE,
    )

def run_customer_segmentation(conn) -> int:
    """
    Main function to run customer segmentation pipeline.
    Returns the number of customers segmented.
    """
    # Fetch data
    df = fetch_transactions_for_rfm(conn)
    
    if df.empty or len(df) < MIN_TRANSACTIONS:
        print(f"[ml_service] Not enough transactions for segmentation ({len(df)} < {MIN_TRANSACTIONS})")
        return 0
    
    # Calculate RFM
    rfm = calculate_rfm(df)
    
    if rfm.empty or len(rfm) < 5:
        print("[ml_service] Not enough unique customers for segmentation")
        return 0
    
    # Assign scores and segments
    rfm = assign_rfm_scores(rfm)
    rfm = assign_segments(rfm)
    
    # Apply K-Means clustering
    rfm = apply_kmeans_clustering(rfm, n_clusters=min(N_CLUSTERS, len(rfm)))
    
    # Save to database
    count = upsert_customer_segments(conn, rfm)
    
    return count

def run_product_associations(conn) -> int:
    """
    Main function to run product association mining.
    Returns the number of associations found.
    """
    if not MLXTEND_AVAILABLE:
        return 0
    
    # Fetch data
    df = fetch_transactions_for_basket(conn)
    
    if df.empty:
        print("[ml_service] No transactions for basket analysis")
        return 0
    
    # Prepare baskets
    baskets = prepare_basket_data(df)
    
    if len(baskets) < 5:
        print(f"[ml_service] Not enough multi-item baskets ({len(baskets)})")
        return 0
    
    # Find associations
    rules = find_associations(baskets)
    
    if rules.empty:
        print("[ml_service] No significant product associations found")
        return 0
    
    # Save to database
    count = upsert_product_associations(conn, rules)
    
    return count

def main():
    print(
        f"[ml_service] Starting ML service:
"
        f"  - Poll interval: {POLL_SECONDS}s
"
        f"  - Min transactions: {MIN_TRANSACTIONS}
"
        f"  - K-Means clusters: {N_CLUSTERS}
"
        f"  - Mlxtend available: {MLXTEND_AVAILABLE}"
    )
    
    while True:
        try:
            conn = get_connection()
            ensure_tables_exist(conn)
            
            # Run Customer Segmentation
            seg_count = run_customer_segmentation(conn)
            if seg_count > 0:
                print(f"[ml_service]  Segmented {seg_count} customers into RFM segments + {N_CLUSTERS} clusters")
            
            # Run Product Associations
            assoc_count = run_product_associations(conn)
            if assoc_count > 0:
                print(f"[ml_service]  Found {assoc_count} product associations")
            
            if seg_count == 0 and assoc_count == 0:
                print("[ml_service] Waiting for more data...")
            
            conn.close()
            
        except Exception as e:
            print(f"[ml_service] Error: {e}")
        
        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
