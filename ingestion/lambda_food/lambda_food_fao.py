    """
    Lambda Food FAO - Fetches food commodity prices from FAO FPMA Tool and World Bank Pink Sheet
    """

    import requests
    import pandas as pd
    from datetime import datetime
    from pathlib import Path
    from typing import Optional, List
    from io import StringIO, BytesIO
    from ingestion.common.bronze_io import write_bronze_by_dt
    import glob



    FAO_PP_BULK_URL = "https://fenixservices.fao.org/faostat/api/v1/en/bulkdownloads/PP_E_All_Data_(Normalized).zip"

    # Example: primary food commodity FAOSTAT item codes (add more as needed)
    FAO_COMMODITY_CODES = {
        "wheat": 1717,
        "maize": 56,
        "rice": 27,
        "barley": 1718,
        "sugar_cane": 156,
        "soybeans": 236,
        "potatoes": 116,
        "milk": 882,
        "beef": 867,
        "poultry": 1057,
        "eggs": 1062,
        "fish": 1017,
        "coffee": 656,
        "cocoa": 661,
        "tea": 667,
    }


    class WorldBankURLBuilder:
        """Builds URLs for World Bank Pink Sheet commodity prices"""
        
        # Real World Bank Commodity Markets (Pink Sheet) data
        PINK_SHEET_URL = "https://thedocs.worldbank.org/en/doc/5d903e848db1d1b83e0ec8f744e55570-0350012021/related/CMO-Historical-Data-Monthly.xlsx"
        
        @staticmethod
        def build_pink_sheet_url() -> str:
            """Build URL for World Bank Pink Sheet (monthly commodity prices)"""
            return WorldBankURLBuilder.PINK_SHEET_URL



    def _detect_item_code_column(df: pd.DataFrame) -> Optional[str]:
        """
        Try to find the FAOSTAT 'item code' column in a robust way.
        FAOSTAT sometimes uses 'item_code' (lowercase) or 'Item Code' (title case).
        """
        candidates = ["item_code", "Item Code", "itemCode", "itemcode"]
        for col in df.columns:
            if col in candidates:
                return col
        # Fallback: anything that looks like an item code
        for col in df.columns:
            if "item" in col.lower() and "code" in col.lower():
                return col
        return None


    def fetch_fao_food_prices(start_date=None, end_date=None) -> pd.DataFrame:
        """
        Fetch FAOSTAT Producer Prices (PP) using the bulk ZIP file.
        This avoids all API instability (521 errors) completely.
        """
        print(f"Downloading FAOSTAT PP bulk file: {FAO_PP_BULK_URL}")

        try:
            r = requests.get(FAO_PP_BULK_URL, timeout=300)
            r.raise_for_status()
        except Exception as e:
            print(f"✗ Error downloading FAOSTAT PP bulk ZIP: {e}")
            return pd.DataFrame()

        # Read ZIP
        try:
            with zipfile.ZipFile(BytesIO(r.content)) as zf:
                csv_name = [x for x in zf.namelist() if x.endswith(".csv")][0]
                print(f"✓ Found CSV in ZIP: {csv_name}")
                with zf.open(csv_name) as f:
                    df = pd.read_csv(f)
        except Exception as e:
            print(f"✗ Error reading FAOSTAT ZIP: {e}")
            return pd.DataFrame()

        print(f"✓ Loaded {len(df)} FAOSTAT PP rows")

        # Detect item code column
        item_code_col = None
        for c in df.columns:
            if "item" in c.lower() and "code" in c.lower():
                item_code_col = c
                break

        if item_code_col is None:
            print(f"✗ Could not find item_code column in FAOSTAT PP data. Columns: {df.columns.tolist()}")
            return pd.DataFrame()

        # Filter to your item codes
        code_to_name = {v: k for k, v in FAO_COMMODITY_CODES.items()}

        df = df[df[item_code_col].isin(code_to_name.keys())].copy()
        df["commodity"] = df[item_code_col].map(code_to_name)

        # Standardise
        rename_map = {
            "Area": "country_name",
            "Area Code": "country_code",
            "Item": "commodity_name",
            "Year": "year",
            "Value": "price",
            "Unit": "unit"
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        # Build date
        if "year" in df.columns:
            df["date"] = pd.to_datetime(df["year"].astype(str) + "-01-01")
        elif "Year" in df.columns:
            df["date"] = pd.to_datetime(df["Year"].astype(str) + "-01-01")

        # Range filter
        if start_date:
            df = df[df["date"] >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df["date"] <= pd.to_datetime(end_date)]

        df["source"] = "FAOSTAT_PP"

        print(f"✓ Final FAO commodity rows: {len(df)}")
        print(f"  Commodities: {df['commodity'].nunique()}")
        print(f"  Countries: {df['country_name'].nunique() if 'country_name' in df.columns else 'unknown'}")

        return df


    def write_food_batch_csv(bronze_root: str, batch_dir: str = "food/Batch"):
        """
        Concatenate all bronze CSVs for fao_food and worldbank_food into one batch CSV.
        """
        pattern1 = str(Path(bronze_root) / "fao_food" / "dt=*/part-*.csv")
        pattern2 = str(Path(bronze_root) / "worldbank_food" / "dt=*/part-*.csv")
        files = glob.glob(pattern1) + glob.glob(pattern2)
        if not files:
            print("No bronze CSVs found to concatenate for batch.")
            return

        dfs = []
        for f in files:
            try:
                dfs.append(pd.read_csv(f))
            except Exception as e:
                print(f"Error reading {f}: {e}")

        if dfs:
            batch_path = Path(bronze_root) / batch_dir
            batch_path.mkdir(parents=True, exist_ok=True)
            out_file = batch_path / "food_prices_all.csv"
            pd.concat(dfs, ignore_index=True).to_csv(out_file, index=False)
            print(f"✓ Batch CSV written: {out_file}")
        else:
            print("No dataframes to concatenate.")

    def fetch_worldbank_pink_sheet(start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        Fetch World Bank Pink Sheet commodity prices
        
        Args:
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
        
        Returns:
            DataFrame with World Bank commodity price data
        """
        url = WorldBankURLBuilder.build_pink_sheet_url()
        
        print(f"Fetching World Bank Pink Sheet from: {url}")
        
        try:
            # Check if openpyxl is available
            try:
                import openpyxl
            except ImportError:
                print("⚠️  openpyxl not installed. Installing now...")
                import subprocess
                subprocess.check_call([sys.executable, "-m", "pip", "install", "openpyxl"])
                import openpyxl
            
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            
            # Read Excel file (Pink Sheet is in Excel format)
            # The actual commodity names are in the HEADER row, not column names
            df = pd.read_excel(
                BytesIO(response.content),
                sheet_name="Monthly Prices",
                header=None,  # Read without headers first
                engine='openpyxl'
            )
            
            print(f"✓ Fetched World Bank Pink Sheet data")
            print(f"    Shape: {df.shape}")
            
            # Find the row with commodity names (usually has "Crude oil" or similar)
            # Typically row 5 or 6 contains commodity names
            commodity_row_idx = None
            for idx in range(min(10, len(df))):
                row_str = str(df.iloc[idx].tolist()).lower()
                if any(kw in row_str for kw in ['crude oil', 'wheat', 'maize', 'sugar']):
                    commodity_row_idx = idx
                    break
            
            if commodity_row_idx is None:
                print("✗ Could not find commodity names row")
                return pd.DataFrame()
            
            # Extract commodity names
            commodity_names = df.iloc[commodity_row_idx].tolist()
            print(f"    Found {len(commodity_names)} commodities at row {commodity_row_idx}")
            
            # Find date column start (first column after metadata)
            date_start_idx = None
            for idx, val in enumerate(commodity_names):
                if pd.notna(val) and str(val).lower() in ['date', 'month', 'year']:
                    date_start_idx = idx
                    break
            
            if date_start_idx is None:
                date_start_idx = 0  # Assume first column is date
            
            # Read data starting after commodity row
            data_start_row = commodity_row_idx + 1
            df_data = df.iloc[data_start_row:].reset_index(drop=True)
            
            # Set commodity names as column headers
            df_data.columns = commodity_names
            
            # Get date column name
            date_col = df_data.columns[date_start_idx]
            
            print(f"    Date column: '{date_col}'")
            print(f"    Sample commodities: {[c for c in commodity_names[1:6] if pd.notna(c)]}")
            
            # Melt to long format
            value_cols = [col for col in df_data.columns if col != date_col and pd.notna(col)]
            
            df_long = df_data.melt(
                id_vars=[date_col],
                value_vars=value_cols,
                var_name="commodity",
                value_name="price"
            )
            
            # Parse date column (handle different formats)
            # World Bank uses formats like "1960M01" or datetime objects
            if df_long[date_col].dtype == 'object':
                # Try parsing as "YYYYMMM" format
                df_long["date"] = pd.to_datetime(
                    df_long[date_col].astype(str).str.replace('M', '-'),
                    format='%Y-%m',
                    errors='coerce'
                )
            else:
                df_long["date"] = pd.to_datetime(df_long[date_col], errors="coerce")
            
            df_long = df_long.dropna(subset=["date"])
            
            # Filter by date range if provided
            if start_date:
                start_dt = pd.to_datetime(start_date)
                df_long = df_long[df_long["date"] >= start_dt]
                print(f"    Filtered from {start_date}")
            
            if end_date:
                end_dt = pd.to_datetime(end_date)
                df_long = df_long[df_long["date"] <= end_dt]
                print(f"    Filtered to {end_date}")
            
            # Convert price to numeric
            df_long["price"] = pd.to_numeric(df_long["price"], errors="coerce")
            
            # Add metadata
            df_long["source"] = "WorldBank_PinkSheet"
            df_long["currency"] = "USD"
            
            # Clean up
            df_long = df_long[["date", "commodity", "price", "currency", "source"]]
            df_long = df_long.dropna(subset=["price"])
            
            print(f"✓ Processed {len(df_long)} price points")
            print(f"    Date range: {df_long['date'].min()} to {df_long['date'].max()}")
            
            return df_long
            
        except requests.exceptions.RequestException as e:
            print(f"✗ Error fetching World Bank data: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"✗ Error processing World Bank data: {e}")
            import traceback
            print(traceback.format_exc())
            return pd.DataFrame()

    def filter_relevant_commodities(
        df: pd.DataFrame,
        commodities: List[str] = None
    ) -> pd.DataFrame:
        """
        Filter dataset for relevant food commodities
        
        Args:
            df: DataFrame with commodity prices
            commodities: List of commodity names to filter (case-insensitive)
        """
        if commodities is None:
            # Default food commodities relevant to inflation
            commodities = [
                "wheat", "maize", "rice", "sugar", "palm oil",
                "soybean", "beef", "poultry", "fish", "milk",
                "coffee", "cocoa", "tea", "corn"
            ]
        
        if "commodity" not in df.columns:
            return df
        
        # Case-insensitive filtering
        mask = df["commodity"].str.lower().str.contains(
            "|".join(commodities),
            case=False,
            na=False
        )
        
        filtered_df = df[mask].copy()
        print(f"✓ Filtered to {len(filtered_df)} rows for relevant commodities")
        
        return filtered_df


    def transform_to_bronze_schema(df: pd.DataFrame, dataset_name: str) -> List[dict]:
        """
        Transform DataFrame to Bronze schema format
        
        Args:
            df: DataFrame with food price data
            dataset_name: Name of the dataset (e.g., "fao_fpma", "worldbank_food")
        
        Returns:
            List of dicts matching Bronze schema (dt, dataset, series_id, geo, value, source, ingest_ts)
        """
        records = []
        
        for _, row in df.iterrows():
            # Create series_id from commodity and additional attributes
            series_parts = [str(row.get("commodity", "unknown"))]
            
            if "product_type" in row and pd.notna(row["product_type"]):
                series_parts.append(str(row["product_type"]))
            
            if "unit" in row and pd.notna(row["unit"]):
                series_parts.append(str(row["unit"]))
            
            series_id = "_".join(series_parts).lower().replace(" ", "_")
            
            record = {
                "dt": row["date"].strftime("%Y-%m-%d") if pd.notna(row.get("date")) else None,
                "dataset": dataset_name,
                "series_id": series_id,
                "geo": row.get("country_code", "GLOBAL"),  # Use country_code or GLOBAL for world prices
                "value": float(row["price"]) if pd.notna(row.get("price")) else None,
                "source": row.get("source", dataset_name),
                "ingest_ts": None  # Will be set by bronze_io
            }
            
            # Only add records with valid data
            if record["dt"] and record["value"]:
                records.append(record)
        
        return records


    def main():
        """Main execution function"""
        
        print("=" * 60)
        print("Starting FAO Food Price Data Extraction")
        print("=" * 60)
        
        # Configuration - MODIFY DATES HERE
        START_DATE = "2000-01-01"  # Change this to your desired start date
        END_DATE = datetime.now().strftime("%Y-%m-%d")  # Current date, or set specific date
        
        print(f"Date range: {START_DATE} to {END_DATE}")
        
        # Bronze root path
        bronze_root = str(Path(__file__).parent.parent.parent / "data" / "bronze")
        
        # Fetch FAO FPMA data
        print("\n[1/2] Fetching FAO FPMA Tool data...")
        fao_df = fetch_fao_food_prices(start_date=START_DATE, end_date=END_DATE)
        
        if not fao_df.empty:
            print(f"    FAO dataset: {len(fao_df)} rows")
            if "commodity" in fao_df.columns:
                print(f"    Commodities: {fao_df['commodity'].nunique()}")
            if "country_name" in fao_df.columns:
                print(f"    Countries: {fao_df['country_name'].nunique()}")
            
            # Filter and transform to Bronze schema
            fao_filtered = filter_relevant_commodities(fao_df)
            fao_records = transform_to_bronze_schema(fao_filtered, "fao_food")
            
            if fao_records:
                # Write to Bronze layer using shared bronze_io
                written_files = write_bronze_by_dt(fao_records, dataset="fao_food", root=bronze_root)
                print(f"✓ Written {len(written_files)} partition(s) for FAO data")
            else:
                print("⚠️  No valid FAO records to write")
        
        # Fetch World Bank Pink Sheet
        print("\n[2/2] Fetching World Bank Pink Sheet...")
        wb_df = fetch_worldbank_pink_sheet(start_date=START_DATE, end_date=END_DATE)
        
        if not wb_df.empty:
            print(f"    World Bank dataset: {len(wb_df)} rows")
            if "commodity" in wb_df.columns:
                print(f"    Commodities: {wb_df['commodity'].nunique()}")
            
            # Filter and transform to Bronze schema
            wb_filtered = filter_relevant_commodities(wb_df)
            wb_records = transform_to_bronze_schema(wb_filtered, "worldbank_food")
            
            if wb_records:
                # Write to Bronze layer using shared bronze_io
                written_files = write_bronze_by_dt(wb_records, dataset="worldbank_food", root=bronze_root)
                print(f"✓ Written {len(written_files)} partition(s) for World Bank data")
            else:
                print("⚠️  No valid World Bank records to write")
        
        print("\n" + "=" * 60)
        print("Extraction completed successfully")
        print("=" * 60)


    if __name__ == "__main__":
        main()
        bronze_root = str(Path(__file__).parent.parent.parent / "data" / "bronze")
        write_food_batch_csv(bronze_root)