from __future__ import annotations

from pathlib import Path
from datetime import datetime
import os
import duckdb
import pandas as pd
import joblib
from dotenv import load_dotenv
import json

load_dotenv()

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parents[2]))
DB_PATH = Path(os.getenv("DB_PATH", PROJECT_ROOT / "data" / "euro_nowcast.duckdb"))
MODEL_DIR = PROJECT_ROOT / "models" / "arima"
OUT_DIR = PROJECT_ROOT / "analytics" / "outputs"

# Ensure output directory exists
OUT_DIR.mkdir(parents=True, exist_ok=True)

def load_sarima_model(path: Path | None = None):
    """Load trained SARIMA model."""
    if path is None:
        path = MODEL_DIR / "sarima_model.pkl"
    if not path.exists():
        raise FileNotFoundError(f"SARIMA model not found at {path}")
    return joblib.load(path)

def load_historical_hicp(geo: str = "EA20") -> pd.Series:
    """Load historical target_hicp_index for given geography."""
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        query = """
            SELECT 
                month,
                target_hicp_index
            FROM gold.gold_nowcast_input
            WHERE geo = ?
              AND target_hicp_index IS NOT NULL
            ORDER BY month
        """
        df = con.execute(query, [geo]).df()
        df["month"] = pd.to_datetime(df["month"])
        return df.set_index("month")["target_hicp_index"]
    finally:
        con.close()

def analyze_sarima_model(model) -> dict:
    """Extract mathematical insights from fitted SARIMA model."""
    
    analysis = {
        # Model specification
        "model_order": model.model_orders,
        "aic": float(model.aic),
        "bic": float(model.bic),
        "log_likelihood": float(model.llf),
        
        # AR coefficients (autoregressive - how past values influence future)
        "ar_coefficients": model.arparams.tolist() if hasattr(model, 'arparams') else [],
        
        # MA coefficients (moving average - how past errors influence future)
        "ma_coefficients": model.maparams.tolist() if hasattr(model, 'maparams') else [],
        
        # Seasonal AR coefficients
        "seasonal_ar_coefs": model.seasonalarparams.tolist() if hasattr(model, 'seasonalarparams') else [],
        
        # Seasonal MA coefficients
        "seasonal_ma_coefs": model.seasonalmaparams.tolist() if hasattr(model, 'seasonalmaparams') else [],
        
        # Residual diagnostics
        "residual_mean": float(model.resid.mean()),
        "residual_std": float(model.resid.std()),
        "residual_skewness": float(model.resid.skew()),
        "residual_kurtosis": float(model.resid.kurtosis()),
        
        # Statistical tests
        "ljung_box_p_value": None,
        "jarque_bera_p_value": None,
    }
    
    # Ljung-Box test (are residuals white noise?)
    try:
        from statsmodels.stats.diagnostic import acorr_ljungbox
        lb_test = acorr_ljungbox(model.resid, lags=[10], return_df=True)
        analysis["ljung_box_p_value"] = float(lb_test['lb_pvalue'].iloc[0])
    except:
        pass
    
    # Jarque-Bera test (are residuals normally distributed?)
    try:
        from scipy.stats import jarque_bera
        jb_stat, jb_pval = jarque_bera(model.resid)
        analysis["jarque_bera_p_value"] = float(jb_pval)
    except:
        pass
    
    return analysis

def print_model_insights(analysis: dict):
    """Print human-readable model insights."""
    
    print("\n" + "="*80)
    print("SARIMA MODEL INSIGHTS & DIAGNOSTICS")
    print("="*80)
    
    # Model specification - FIX: Handle the actual structure of model_orders
    print("\n📊 MODEL SPECIFICATION:")
    model_order = analysis['model_order']
    
    # model_orders returns a dict like: {'order': (p,d,q), 'seasonal_order': (P,D,Q,s), ...}
    # Check if it's nested or flat
    if isinstance(model_order, dict):
        # If it's a dict with keys 'order' and 'seasonal_order'
        if 'order' in model_order:
            order = model_order['order']
            seasonal_order = model_order['seasonal_order']
        else:
            # If the dict IS the orders directly
            order = (model_order.get('p', 0), model_order.get('d', 0), model_order.get('q', 0))
            seasonal_order = (
                model_order.get('P', 0), 
                model_order.get('D', 0), 
                model_order.get('Q', 0), 
                model_order.get('s', 12)
            )
    else:
        # Fallback if structure is unexpected
        order = (1, 1, 1)
        seasonal_order = (1, 1, 1, 12)
    
    print(f"   SARIMA{order}{seasonal_order}")
    print(f"   - AR order (p): {order[0]} → Uses last {order[0]} month(s)")
    print(f"   - Differencing (d): {order[1]} → {order[1]} level of differencing")
    print(f"   - MA order (q): {order[2]} → Uses last {order[2]} error(s)")
    print(f"   - Seasonal AR (P): {seasonal_order[0]}")
    print(f"   - Seasonal D: {seasonal_order[1]}")
    print(f"   - Seasonal MA (Q): {seasonal_order[2]}")
    print(f"   - Seasonality (s): {seasonal_order[3]} months")
    
    # Model fit quality
    print("\n📈 MODEL FIT QUALITY:")
    print(f"   AIC: {analysis['aic']:.2f} (lower is better)")
    print(f"   BIC: {analysis['bic']:.2f} (lower is better, penalizes complexity)")
    print(f"   Log-Likelihood: {analysis['log_likelihood']:.2f}")
    
    # Coefficient interpretation
    print("\n🔢 AUTOREGRESSIVE COEFFICIENTS (How past influences future):")
    if analysis['ar_coefficients']:
        for i, coef in enumerate(analysis['ar_coefficients'], 1):
            print(f"   AR({i}): {coef:+.4f} → Last month's value × {coef:.4f}")
    else:
        print("   None")
    
    print("\n🔢 MOVING AVERAGE COEFFICIENTS (How past errors influence future):")
    if analysis['ma_coefficients']:
        for i, coef in enumerate(analysis['ma_coefficients'], 1):
            print(f"   MA({i}): {coef:+.4f} → Last error × {coef:.4f}")
    else:
        print("   None")
    
    print("\n📅 SEASONAL COMPONENTS (12-month patterns):")
    if analysis['seasonal_ar_coefs']:
        print(f"   Seasonal AR: {analysis['seasonal_ar_coefs'][0]:+.4f}")
        print(f"   → Value from 12 months ago × {analysis['seasonal_ar_coefs'][0]:.4f}")
    if analysis['seasonal_ma_coefs']:
        print(f"   Seasonal MA: {analysis['seasonal_ma_coefs'][0]:+.4f}")
        print(f"   → Error from 12 months ago × {analysis['seasonal_ma_coefs'][0]:.4f}")
    
    # Residual diagnostics
    print("\n🔬 RESIDUAL DIAGNOSTICS:")
    print(f"   Mean: {analysis['residual_mean']:.4f} (should be ~0)")
    print(f"   Std Dev: {analysis['residual_std']:.4f}")
    print(f"   Skewness: {analysis['residual_skewness']:.4f} (should be ~0)")
    print(f"   Kurtosis: {analysis['residual_kurtosis']:.4f} (should be ~3)")
    
    # Statistical tests
    print("\n✅ STATISTICAL TESTS:")
    
    if analysis['ljung_box_p_value'] is not None:
        lb_pass = analysis['ljung_box_p_value'] > 0.05
        print(f"   Ljung-Box (residuals are white noise):")
        print(f"   → p-value: {analysis['ljung_box_p_value']:.4f} {'✓ PASS' if lb_pass else '✗ FAIL'}")
        if not lb_pass:
            print(f"   ⚠️  Residuals show autocorrelation - model may be misspecified")
    
    if analysis['jarque_bera_p_value'] is not None:
        jb_pass = analysis['jarque_bera_p_value'] > 0.05
        print(f"   Jarque-Bera (residuals are normal):")
        print(f"   → p-value: {analysis['jarque_bera_p_value']:.4f} {'✓ PASS' if jb_pass else '✗ FAIL'}")
        if not jb_pass:
            print(f"   ℹ️  Residuals are not perfectly normal (common in real data)")
    
    print("\n" + "="*80)

def main():
    """Analyze the trained SARIMA model."""
    
    print("="*80)
    print("SARIMA MODEL ANALYSIS")
    print("="*80)
    
    try:
        # Load the trained model
        print("\nLoading SARIMA model...")
        model = load_sarima_model()
        
        # DEBUG: Print the actual structure
        print("\n🔍 DEBUG: model_orders structure:")
        print(f"Type: {type(model.model_orders)}")
        print(f"Content: {model.model_orders}")
        print(f"Keys: {model.model_orders.keys() if isinstance(model.model_orders, dict) else 'Not a dict'}")
        
        # Analyze the model
        print("\nAnalyzing model structure and fit...")
        analysis = analyze_sarima_model(model)
        
        # DEBUG: Print analysis structure
        print("\n🔍 DEBUG: analysis['model_order'] structure:")
        print(f"Type: {type(analysis['model_order'])}")
        print(f"Content: {analysis['model_order']}")
        
        # Print insights
        print_model_insights(analysis)
        
        # Save analysis to JSON
        analysis_path = OUT_DIR / "sarima_model_analysis.json"
        with open(analysis_path, 'w') as f:
            json.dump(analysis, f, indent=2)
        print(f"\n💾 Saved model analysis to {analysis_path}")
        
        print("\n✅ Analysis complete!")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()