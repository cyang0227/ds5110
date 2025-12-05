import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import duckdb
import sys
import os
import subprocess
import vectorbt as vbt
from pathlib import Path

# Add project root to path to allow imports
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.append(str(project_root / "src"))

from utils.factor_data import get_all_tickers, get_all_factors, load_ohlcv_wide, load_factor_values_wide
from backtest.engine import FactorBacktester

# Database Connection
DB_PATH = str(project_root / "data" / "warehouse" / "data.duckdb")

@st.cache_resource
def get_db_connection():
    if not os.path.exists(DB_PATH):
        return None
    return duckdb.connect(DB_PATH, read_only=True)

# Page Config
st.set_page_config(
    page_title="DS5110 Factor Tool",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Sidebar
st.sidebar.title("DS5110 – Factor-based Stock Analysis Tool")

# Custom CSS for Sidebar Styling
st.markdown("""
<style>
    /* Target the sidebar radio group */
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] {
        gap: 12px;
    }
    
    /* Style each radio option (label) */
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label {
        background-color: transparent;
        border: 1px solid rgba(128, 128, 128, 0.2);
        border-radius: 10px;
        padding: 12px 15px;
        transition: all 0.2s ease-in-out;
        margin-bottom: 4px;
        cursor: pointer;
    }

    /* Hover effect */
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label:hover {
        background-color: rgba(128, 128, 128, 0.1);
        border-color: rgba(128, 128, 128, 0.4);
        transform: translateX(4px);
    }

    /* Active/Checked state using :has() selector */
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label:has(input:checked) {
        background-color: #FF4B4B; /* Streamlit Primary Color */
        border-color: #FF4B4B;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    /* Force text color to white when selected */
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label:has(input:checked) * {
        color: white !important;
    }

    /* Hide the default radio circle */
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label > div:first-child {
        display: none;
    }
    
    /* Increase font size for the text */
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] > label > div[data-testid="stMarkdownContainer"] p {
        font-size: 1.1rem;
        font-weight: 600;
        margin: 0;
    }
</style>
""", unsafe_allow_html=True)

# Navigation Options with Emojis
page_map = {
    "Stock Analysis": "Stock Analysis",
    "Technical Backtest": "Technical Backtest",
    "Factor Backtest": "Factor Backtest",
    "Data Management": "Data Management"
}

selection = st.sidebar.radio(
    "Navigation", 
    list(page_map.keys()), 
    label_visibility="collapsed"
)

# Map back to internal page name
page = page_map[selection]

# Helpers
@st.cache_data
def load_tickers():
    con = get_db_connection()
    if con is None:
        return pd.DataFrame()
    return get_all_tickers(con)

@st.cache_data
def load_factors():
    con = get_db_connection()
    if con is None:
        return pd.DataFrame()
    return get_all_factors(con)

# ==========================================
# Page: Stock Analysis
# ==========================================
if page == "Stock Analysis":
    st.title("Stock Analysis")
    
    tickers_df = load_tickers()
    if tickers_df.empty:
        st.warning("Database not found or empty. Please go to **Data Management** to run the ETL pipeline.")
        st.info("Navigate to: Sidebar > Data Management > Run ETL Pipeline")
    else:
        # Ticker Selector
        ticker_options = tickers_df.apply(lambda x: f"{x['symbol']} - {x['name']}", axis=1).tolist()
        selected_ticker_str = st.selectbox("Select Stock", ticker_options)
        selected_symbol = selected_ticker_str.split(" - ")[0]
        selected_sid = tickers_df[tickers_df['symbol'] == selected_symbol]['security_id'].iloc[0]
        
        # Load Data
        con = get_db_connection()
        try:
            # Load OHLCV for this specific ticker
            df_wide = load_ohlcv_wide(con, security_ids=[selected_sid])
            
            if df_wide.empty:
                st.warning(f"No price data found for {selected_symbol}")
            else:
                # df_wide columns are MultiIndex (Variable, SecurityID)
                df_stock = df_wide.xs(selected_sid, axis=1, level=1)
                
                # Date Range Filter
                min_date = df_stock.index.min().date()
                max_date = df_stock.index.max().date()
                
                col1, col2 = st.columns(2)
                start_date = col1.date_input("Start Date", min_date)
                end_date = col2.date_input("End Date", max_date)
                
                mask = (df_stock.index.date >= start_date) & (df_stock.index.date <= end_date)
                df_plot = df_stock.loc[mask]
                
                # Plot
                fig = go.Figure()
                
                # Candlestick
                fig.add_trace(go.Candlestick(
                    x=df_plot.index,
                    open=df_plot['open'],
                    high=df_plot['high'],
                    low=df_plot['low'],
                    close=df_plot['close'],
                    name='OHLC'
                ))
                
                # Moving Averages
                if st.checkbox("Show SMA 50"):
                    sma50 = df_plot['close'].rolling(window=50).mean()
                    fig.add_trace(go.Scatter(x=df_plot.index, y=sma50, mode='lines', name='SMA 50'))
                    
                if st.checkbox("Show SMA 200"):
                    sma200 = df_plot['close'].rolling(window=200).mean()
                    fig.add_trace(go.Scatter(x=df_plot.index, y=sma200, mode='lines', name='SMA 200'))

                # Calculate missing dates (breaks)
                dt_all = pd.date_range(start=df_plot.index[0], end=df_plot.index[-1])
                dt_obs = [d.strftime("%Y-%m-%d") for d in df_plot.index]
                dt_breaks = [d.strftime("%Y-%m-%d") for d in dt_all if d.strftime("%Y-%m-%d") not in dt_obs]

                fig.update_layout(
                    title=f"{selected_symbol} Price Chart", 
                    xaxis_title="Date", 
                    yaxis_title="Price",
                    xaxis_rangeslider_visible=False,
                    xaxis_rangebreaks=[
                        dict(values=dt_breaks) # hide weekends and holidays
                    ]
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Volume
                fig_vol = go.Figure()
                fig_vol.add_trace(go.Bar(x=df_plot.index, y=df_plot['volume'], name='Volume'))
                fig_vol.update_layout(
                    title="Volume", 
                    xaxis_title="Date", 
                    yaxis_title="Volume", 
                    height=300,
                    xaxis_rangebreaks=[
                        dict(values=dt_breaks) # hide weekends and holidays
                    ]
                )
                st.plotly_chart(fig_vol, use_container_width=True)
                
        except Exception as e:
            st.error(f"Error loading data: {e}")

# ==========================================
# Page: Technical Backtest
# ==========================================
elif page == "Technical Backtest":
    st.title("Technical Backtest (Single Stock)")
    
    tickers_df = load_tickers()
    if tickers_df.empty:
        st.warning("Database not found or empty. Please go to **Data Management** to run the ETL pipeline.")
        st.info("Navigate to: Sidebar > Data Management > Run ETL Pipeline")
    else:
        col1, col2 = st.columns(2)
        
        with col1:
            ticker_options = tickers_df['symbol'].tolist()
            symbol = st.selectbox("Select Stock", ticker_options)
            
        with col2:
            strategy = st.selectbox("Strategy", ["SMA Crossover", "RSI", "MACD"])
            
        # Strategy Params
        params = {}
        if strategy == "SMA Crossover":
            fast = st.number_input("Fast Window", 10, 100, 20)
            slow = st.number_input("Slow Window", 20, 200, 50)
            params = {'fast': fast, 'slow': slow}
        elif strategy == "RSI":
            window = st.number_input("RSI Window", 5, 50, 14)
            lower = st.number_input("Lower Threshold", 10, 40, 30)
            upper = st.number_input("Upper Threshold", 60, 90, 70)
            params = {'window': window, 'lower': lower, 'upper': upper}
            
        # Date Range
        col3, col4 = st.columns(2)
        start_date = col3.date_input("Start Date", pd.to_datetime("2017-01-01"))
        end_date = col4.date_input("End Date", pd.to_datetime("today"))
        
        if st.button("Run Backtest"):
            sid = tickers_df[tickers_df['symbol'] == symbol]['security_id'].iloc[0]
            con = get_db_connection()
            df_wide = load_ohlcv_wide(con, security_ids=[sid])
            
            if df_wide.empty:
                st.error("No data found.")
            else:
                close_price = df_wide.xs(sid, axis=1, level=1)['close']
                
                # Filter by date
                mask = (close_price.index.date >= start_date) & (close_price.index.date <= end_date)
                close_price = close_price.loc[mask]
                
                if close_price.empty:
                    st.error("No data for selected date range.")
                else:
                    pf = None
                    if strategy == "SMA Crossover":
                        fast_ma = vbt.MA.run(close_price, params['fast'])
                        slow_ma = vbt.MA.run(close_price, params['slow'])
                        entries = fast_ma.ma_crossed_above(slow_ma)
                        exits = fast_ma.ma_crossed_below(slow_ma)
                        pf = vbt.Portfolio.from_signals(close_price, entries, exits, freq='1D', init_cash=100000)
                        
                    elif strategy == "RSI":
                        rsi = vbt.RSI.run(close_price, window=params['window'])
                        entries = rsi.rsi_crossed_below(params['lower'])
                        exits = rsi.rsi_crossed_above(params['upper'])
                        pf = vbt.Portfolio.from_signals(close_price, entries, exits, freq='1D', init_cash=100000)
                    
                    elif strategy == "MACD":
                         macd = vbt.MACD.run(close_price)
                         entries = macd.macd_crossed_above(macd.signal)
                         exits = macd.macd_crossed_below(macd.signal)
                         pf = vbt.Portfolio.from_signals(close_price, entries, exits, freq='1D', init_cash=100000)
                    
                    if pf:
                        st.session_state['tech_pf'] = pf
                        st.session_state['tech_symbol'] = symbol
                        st.session_state['tech_strategy'] = strategy
                        st.session_state['tech_params'] = params

        if 'tech_pf' in st.session_state:
            pf = st.session_state['tech_pf']
            symbol = st.session_state['tech_symbol']
            strategy = st.session_state['tech_strategy']
            
            st.subheader("Results")
            st.metric("Total Return", f"{pf.total_return():.2%}")
            st.metric("Sharpe Ratio", f"{pf.sharpe_ratio():.2f}")
            st.metric("Max Drawdown", f"{pf.max_drawdown():.2%}")
            
            st.plotly_chart(pf.plot(), use_container_width=True)
            
            # Technical Indicator Charts
            st.subheader("Technical Indicators")
            
            if strategy == "MACD":
                # Calculate MACD for plotting
                macd_ind = vbt.MACD.run(pf.close)
                
                fig_macd = go.Figure()
                fig_macd.add_trace(go.Scatter(x=macd_ind.macd.index, y=macd_ind.macd, mode='lines', name='MACD'))
                fig_macd.add_trace(go.Scatter(x=macd_ind.signal.index, y=macd_ind.signal, mode='lines', name='Signal'))
                fig_macd.add_trace(go.Bar(x=macd_ind.hist.index, y=macd_ind.hist, name='Histogram'))
                
                fig_macd.update_layout(title="MACD (12, 26, 9)", xaxis_title="Date", yaxis_title="Value", height=400)
                st.plotly_chart(fig_macd, use_container_width=True)
                
            elif strategy == "RSI":
                params = st.session_state.get('tech_params', {'window': 14, 'lower': 30, 'upper': 70})
                window = params.get('window', 14)
                lower = params.get('lower', 30)
                upper = params.get('upper', 70)
                
                rsi_ind = vbt.RSI.run(pf.close, window=window)
                
                fig_rsi = go.Figure()
                fig_rsi.add_trace(go.Scatter(x=rsi_ind.rsi.index, y=rsi_ind.rsi, mode='lines', name='RSI'))
                
                # Add Thresholds
                fig_rsi.add_hline(y=upper, line_dash="dash", line_color="red", annotation_text="Overbought")
                fig_rsi.add_hline(y=lower, line_dash="dash", line_color="green", annotation_text="Oversold")
                
                fig_rsi.update_layout(title=f"RSI ({window})", xaxis_title="Date", yaxis_title="Value", yaxis_range=[0, 100], height=400)
                st.plotly_chart(fig_rsi, use_container_width=True)
                
            elif strategy == "SMA Crossover":
                # Retrieve params
                params = st.session_state.get('tech_params', {'fast': 20, 'slow': 50})
                fast = params.get('fast', 20)
                slow = params.get('slow', 50)
                
                # Calculate SMAs
                fast_ma = vbt.MA.run(pf.close, window=fast)
                slow_ma = vbt.MA.run(pf.close, window=slow)
                
                fig_sma = go.Figure()
                fig_sma.add_trace(go.Scatter(x=pf.close.index, y=pf.close, mode='lines', name='Close Price', line=dict(color='gray', width=1)))
                fig_sma.add_trace(go.Scatter(x=fast_ma.ma.index, y=fast_ma.ma, mode='lines', name=f'SMA {fast}', line=dict(color='orange')))
                fig_sma.add_trace(go.Scatter(x=slow_ma.ma.index, y=slow_ma.ma, mode='lines', name=f'SMA {slow}', line=dict(color='blue')))
                
                fig_sma.update_layout(title=f"SMA Crossover ({fast}, {slow})", xaxis_title="Date", yaxis_title="Price", height=400)
                st.plotly_chart(fig_sma, use_container_width=True)

            # Downloads
            st.subheader("Downloads")
            col_d1, col_d2 = st.columns(2)
            
            # Orders
            orders_df = pf.orders.records_readable
            if 'Column' in orders_df.columns:
                orders_df = orders_df.rename(columns={'Column': 'Ticker'})
            # Ensure Ticker column has the symbol name (for single stock it might be generic)
            if 'Ticker' in orders_df.columns:
                orders_df['Ticker'] = symbol
            
            csv_orders = orders_df.to_csv().encode('utf-8')
            col_d1.download_button(
                label="Download Orders (CSV)",
                data=csv_orders,
                file_name=f"{symbol}_{strategy}_orders.csv",
                mime="text/csv",
            )
            
            # Trades (Positions)
            trades_df = pf.trades.records_readable
            if 'Column' in trades_df.columns:
                trades_df = trades_df.rename(columns={'Column': 'Ticker'})
            if 'Ticker' in trades_df.columns:
                trades_df['Ticker'] = symbol
                
            csv_trades = trades_df.to_csv().encode('utf-8')
            col_d2.download_button(
                label="Download Trade Log (CSV)",
                data=csv_trades,
                file_name=f"{symbol}_{strategy}_trades.csv",
                mime="text/csv",
            )

# ==========================================
# Page: Factor Backtest
# ==========================================
elif page == "Factor Backtest":
    st.title("Factor Backtest (Portfolio)")
    
    factors_df = load_factors()
    if factors_df.empty:
        st.warning("Database not found or empty. Please go to **Data Management** to run the ETL pipeline.")
        st.info("Navigate to: Sidebar > Data Management > Run ETL Pipeline")
    else:
        # Inputs
        col1, col2 = st.columns(2)
        with col1:
            selected_factors = st.multiselect("Select Factors", factors_df['name'].tolist(), default=[factors_df['name'].iloc[0]])
        with col2:
            st.write("Strategy: Top N Long-Only")
            # strategy_type = st.selectbox("Strategy Type", ["Top N Long-Only", "Long-Short"])
            
        # Factor Weights
        factor_weights = {}
        if len(selected_factors) > 1:
            st.write("Factor Weights")
            cols = st.columns(len(selected_factors))
            total_weight = 0.0
            for i, factor in enumerate(selected_factors):
                weight = cols[i].number_input(f"Weight: {factor}", value=1.0 / len(selected_factors), step=0.1)
                factor_weights[factor] = weight
                total_weight += weight
            
            # Validation
            if abs(total_weight - 1.0) > 0.01:
                st.warning(f"Total Weight is {total_weight:.2f}. It will be normalized to 1.0 automatically.")
                # Normalize weights for calculation
                for f in factor_weights:
                    factor_weights[f] = factor_weights[f] / total_weight
        elif len(selected_factors) == 1:
            factor_weights[selected_factors[0]] = 1.0
            
        col3, col4, col5 = st.columns(3)
        top_n = col3.number_input("Top N Stocks", 5, 100, 20)
        rebalance = col4.selectbox("Rebalance Freq", ["M", "W", "Q"], index=0)
        weighting = col5.selectbox("Weighting", ["equal", "factor"])
        
        # Normalization Mode
        col_n1, col_n2 = st.columns(2)
        norm_mode = col_n1.radio("Normalization Mode", ["Market-Wide", "Sector-Neutral"], horizontal=True)
        score_type = col_n2.radio("Score Type", ["Z-Score", "Rank"], horizontal=True)

        # Fees and Slippage
        col_f1, col_f2 = st.columns(2)
        fees_bps = col_f1.number_input("Trading Fees (bps)", min_value=0, max_value=500, value=10, step=1)
        slippage_bps = col_f2.number_input("Slippage (bps)", min_value=0, max_value=500, value=10, step=1)
        
        fees = fees_bps / 10000.0
        slippage = slippage_bps / 10000.0
        
        # Date Range
        col6, col7 = st.columns(2)
        start_date = col6.date_input("Start Date", pd.to_datetime("2017-01-01"), key="factor_start")
        end_date = col7.date_input("End Date", pd.to_datetime("today"), key="factor_end")
        
        if st.button("Run Factor Backtest"):
            with st.spinner("Loading data and running backtest..."):
                con = get_db_connection()
                
                # 1. Load Factor Data
                try:
                    if not selected_factors:
                        st.error("Please select at least one factor.")
                    else:
                        combined_factor_vals = None
                        
                        # Determine which column to load
                        if score_type == "Z-Score":
                            if norm_mode == "Market-Wide":
                                target_col = "zscore_cross"
                            else:
                                target_col = "zscore_cross_sector"
                        else: # Rank
                            if norm_mode == "Market-Wide":
                                target_col = "rank_cross"
                            else:
                                target_col = "rank_cross_sector"

                        # Load and combine factors
                        for factor_name in selected_factors:
                            # Load specific Z-score column from DB
                            df_zscore = load_factor_values_wide(con, factor_name, value_col=target_col)
                            
                            if df_zscore.empty:
                                st.warning(f"No data for factor: {factor_name} (Column: {target_col})")
                                continue
                                
                            # Filter by date immediately to reduce size
                            mask_dates = (df_zscore.index.date >= start_date) & (df_zscore.index.date <= end_date)
                            df_zscore = df_zscore.loc[mask_dates]
                            
                            if df_zscore.empty:
                                st.warning(f"No data for factor {factor_name} in selected date range.")
                                continue
                            
                            # Apply Weight
                            weight = factor_weights.get(factor_name, 1.0)
                            df_weighted = df_zscore * weight
                            
                            if combined_factor_vals is None:
                                combined_factor_vals = df_weighted
                            else:
                                # Align and add (fill_value=0 might be risky if coverage differs, but standard for combination)
                                combined_factor_vals = combined_factor_vals.add(df_weighted, fill_value=0)
                        
                        if combined_factor_vals is None or combined_factor_vals.empty:
                            st.error("Could not combine factors (no data found).")
                        else:
                            factor_vals = combined_factor_vals
                            
                            # 2. Load Price Data (All stocks that have factor data)
                            sids = factor_vals.columns.tolist()
                            prices_wide = load_ohlcv_wide(con, security_ids=sids)
                            
                            if prices_wide.empty:
                                st.error("No price data found matching factor securities.")
                            else:
                                # Extract close prices
                                # prices_wide is (Variable, SecurityID)
                                close_prices = prices_wide.xs('adj_close', axis=1, level=0)
                                
                                # filter prices to match
                                close_prices = close_prices.loc[close_prices.index.date >= start_date]
                                close_prices = close_prices.loc[close_prices.index.date <= end_date]
                            
                                
                                if factor_vals.empty:
                                    st.error("No factor data for selected date range after alignment.")
                                else:
                                    # Load Benchmark (SPY = 504)
                                    spy_df = load_ohlcv_wide(con, security_ids=[504])
                                    benchmark_series = None
                                    if not spy_df.empty:
                                        # spy_df is (Variable, SecurityID) -> flatten to Series
                                        benchmark_series = spy_df.xs(504, axis=1, level=1)['adj_close']
                                        # Align benchmark to close_prices index
                                        benchmark_series = benchmark_series.reindex(close_prices.index).ffill()
                                    
                                    # 3. Initialize Engine
                                    backtester = FactorBacktester(prices=close_prices, factor_values=factor_vals)
                                    
                                    # 4. Run Strategy
                                    # Default to Top N Long-Only
                                    pf = backtester.run_top_n_strategy(
                                        top_n=top_n, 
                                        rebalance_freq=rebalance, 
                                        weighting=weighting,
                                        benchmark_prices=benchmark_series,
                                        fees=fees,
                                        slippage=slippage
                                    )
                                        
                                    st.session_state['factor_pf'] = pf
                                
                except Exception as e:
                    st.error(f"Backtest failed: {e}")

        if 'factor_pf' in st.session_state:
            pf = st.session_state['factor_pf']
            
            st.success("Backtest Complete!")
            
            st.subheader("Performance Metrics")
            stats = pf.stats()
            st.dataframe(stats.astype(str))
            
            st.subheader("Equity Curve")
            fig = pf.plot(subplots=['value'])
            
            # Manually add benchmark if available
            bm_attr = getattr(pf, '_benchmark_close', None)
            if bm_attr is not None:
                bm = bm_attr
                # Rebase benchmark to match portfolio initial value
                port_value = pf.value()
                
                if isinstance(port_value, pd.Series):
                    initial_value = port_value.iloc[0]
                    initial_bm = bm.iloc[0]
                    
                    if initial_bm > 0:
                        bm_rebased = bm / initial_bm * initial_value
                        fig.add_trace(go.Scatter(
                            x=bm.index, 
                            y=bm_rebased, 
                            mode='lines', 
                            name='Benchmark (SPY)',
                            line=dict(color='gray', dash='dash'),
                            opacity=0.7
                        ))
            
            st.plotly_chart(fig, use_container_width=True)
            
            # =======================
            # Downloads
            # =======================
            st.subheader("Downloads")
            col_d1, col_d2 = st.columns(2)
            
            # Load tickers for mapping (security_id -> symbol)
            tickers_df = load_tickers()
            id_map = tickers_df.set_index('security_id')['symbol'].to_dict()
            
            # Orders
            # For Factor Backtest, orders might be large, but useful
            orders_df = pf.orders.records_readable
            if 'Column' in orders_df.columns:
                orders_df = orders_df.rename(columns={'Column': 'Ticker'})
            
            # Map security_id to symbol
            if 'Ticker' in orders_df.columns:
                orders_df['Ticker'] = orders_df['Ticker'].map(id_map)
                
            csv_orders = orders_df.to_csv().encode('utf-8')
            col_d1.download_button(
                label="Download Orders (CSV)",
                data=csv_orders,
                file_name=f"factor_backtest_orders.csv",
                mime="text/csv",
            )
            
            # Trades
            trades_df = pf.trades.records_readable
            if 'Column' in trades_df.columns:
                trades_df = trades_df.rename(columns={'Column': 'Ticker'})
                
            # Map security_id to symbol
            if 'Ticker' in trades_df.columns:
                trades_df['Ticker'] = trades_df['Ticker'].map(id_map)
                
            csv_trades = trades_df.to_csv().encode('utf-8')
            col_d2.download_button(
                label="Download Trade Log (CSV)",
                data=csv_trades,
                file_name=f"factor_backtest_trades.csv",
                mime="text/csv",
            )
            
            # Positions (Rebalance Holdings)
            # pf.assets() returns a DataFrame/Series of asset holdings (shares)
            positions_df = pf.assets()
            
            # 1. Identify Rebalance Dates (Dates where trades occurred)
             
            # Debugging
            # st.write(f"Debug: orders_df columns: {orders_df.columns.tolist()}")
            
            # Check for 'Date' or 'index' or whatever holds the timestamp
            date_col = None
            if 'Date' in orders_df.columns:
                date_col = 'Date'
            elif 'Timestamp' in orders_df.columns:
                date_col = 'Timestamp'
            elif 'index' in orders_df.columns:
                date_col = 'index'
            elif isinstance(orders_df.index, pd.DatetimeIndex):
                # If date is in index and not reset
                orders_df = orders_df.reset_index()
                date_col = orders_df.columns[0] # Assume first col is date after reset
            
            if not orders_df.empty and date_col:
                # Get unique dates from orders
                # Ensure dates are datetime for comparison
                trade_dates = pd.to_datetime(orders_df[date_col]).dt.normalize().unique()
                
                # st.write(f"Debug: Found {len(trade_dates)} unique trade dates.")
                
                # Filter positions to only these dates
                # positions_df index is datetime
                mask = positions_df.index.normalize().isin(trade_dates)
                positions_df = positions_df.loc[mask]
                
                # st.write(f"Debug: Filtered positions to {len(positions_df)} rows.")
            else:
                st.warning(f"Debug: Could not filter by date. Columns: {orders_df.columns.tolist()}")
            
            # 2. Rename columns (SecurityID -> Ticker)
            col_map = {sid: id_map.get(sid, str(sid)) for sid in positions_df.columns}
            positions_df = positions_df.rename(columns=col_map)
            
            # 3. Melt to Long Format
            # Ensure index is named 'Date' so reset_index creates a 'Date' column
            positions_df.index.name = 'Date'
            positions_df = positions_df.reset_index()
            
            # Melt
            positions_long = positions_df.melt(
                id_vars=['Date'], 
                var_name='Ticker', 
                value_name='Shares'
            )
            
            # 4. Filter out zero positions
            positions_long = positions_long[positions_long['Shares'] != 0]
            
            # Sort by Date and Ticker
            positions_long = positions_long.sort_values(['Date', 'Ticker'])
            
            csv_positions = positions_long.to_csv(index=False).encode('utf-8')
            
            st.download_button(
                label="Download Rebalance Positions",
                data=csv_positions,
                file_name=f"factor_backtest_positions.csv",
                mime="text/csv",
            )
            


# ==========================================
# Page: Data Management
# ==========================================
elif page == "Data Management":
    st.title("Data Management")
    
    st.header("1. ETL Pipeline (Data Ingestion)")
    
    st.write("Trigger data updates manually.")
    
    col1, col2 = st.columns(2)
    only_prices = col1.checkbox("Only Prices")
    only_fundamentals = col2.checkbox("Only Fundamentals")
    incremental = st.checkbox("Incremental Update (Faster)")
    
    if st.button("Run ETL Pipeline"):
        # Close DB connection to release lock
        try:
            con = get_db_connection()
            con.close()
            st.cache_resource.clear()
            st.warning("Database connection closed for ETL. App will reconnect on next action.")
        except Exception as e:
            st.warning(f"Could not close connection: {e}")

        cmd = ["python", "-u", str(project_root / "src/etl/run_etl.py")]
        if only_prices:
            cmd.append("--only-prices")
        if only_fundamentals:
            cmd.append("--only-fundamentals")
        if incremental:
            cmd.append("--incremental")
            
        st.info(f"Running command: {' '.join(cmd)}")
        
        # Run subprocess and stream output
        try:
            process = subprocess.Popen(
                cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT, 
                text=True,
                cwd=str(project_root / "src/etl")
            )
            
            log_area = st.empty()
            logs = []
            
            for line in process.stdout:
                logs.append(line)
                # Update log area every few lines or just append
                log_area.code("".join(logs[-20:])) # Show last 20 lines
                
            process.wait()
            
            if process.returncode == 0:
                st.success("ETL Pipeline Completed Successfully!")
            else:
                st.error("ETL Pipeline Failed.")
                
        except Exception as e:
            st.error(f"Failed to run process: {e}")

    st.markdown("---")
    st.header("2. Factor Pipeline (Calculation)")
    st.write("Calculate and store factors (etc. Momentum, Value) into the database.")
    
    if st.button("Run Factor Pipeline"):
        # Close DB connection to release lock
        try:
            con = get_db_connection()
            con.close()
            st.cache_resource.clear()
            st.warning("Database connection closed. App will reconnect on next action.")
        except Exception as e:
            st.warning(f"Could not close connection: {e}")
            
        # Use -u for unbuffered output to see logs in real-time
        cmd = ["python", "-u", str(project_root / "src/pipelines/run_factor_pipeline.py")]
        st.info(f"Running command: {' '.join(cmd)}")
        
        # Run subprocess and stream output
        try:
            process = subprocess.Popen(
                cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT, 
                text=True,
                cwd=str(project_root)
            )
            
            log_area = st.empty()
            logs = []
            
            for line in process.stdout:
                logs.append(line)
                # Update log area every few lines or just append
                log_area.code("".join(logs[-20:])) # Show last 20 lines
                
            process.wait()
            
            if process.returncode == 0:
                st.success("Factor Pipeline Completed Successfully!")
                # Clear cache to reload factors
                st.cache_data.clear()
            else:
                st.error("Factor Pipeline Failed.")
                
        except Exception as e:
            st.error(f"Failed to run process: {e}")