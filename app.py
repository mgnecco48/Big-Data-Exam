import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from collections import Counter

# Set page config
st.set_page_config(
    page_title="Student Learning Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom styling
st.markdown(
    """
    <style>
    .main {
        padding: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 0.5rem;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data
def load_data():
    """Load the processed dataset"""
    return pd.read_csv("processed.csv")


def format_number(num):
    """Format large numbers with commas"""
    return f"{int(num):,}"


# Load data
df = load_data()

# Sidebar navigation
st.sidebar.title("📊 Navigation")
page = st.sidebar.radio(
    "Select a section:",
    [
        "🏠 Overview",
        "📈 Preprocessing",
        "🔍 Analysis",
        "💭 Sentiment Analysis",
        "📋 Data Explorer",
    ],
)

# Main title and description
st.title("Student Learning Analytics Dashboard")

if page == "🏠 Overview":
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Records", format_number(len(df)), delta=None)
    with col2:
        st.metric("Total Columns", len(df.columns), delta=None)
    with col3:
        st.metric("Missing Values", int(df.isnull().sum().sum()), delta=None)
    with col4:
        st.metric("Complete Records", format_number(df.dropna().shape[0]), delta=None)

    st.markdown("---")
    st.subheader("Quick Summary")

    col1, col2 = st.columns(2)

    with col1:
        st.write("""
        **Dataset Overview:**
        - Student learning behavior dataset with 10,000 synthetic records
        - Covers education levels, learning methods, challenges, and motivations
        - Includes sentiment analysis on online learning opinions
        """)

    with col2:
        st.write("""
        **Key Metrics:**
        - Study hours per day: 0-12 hours
        - 6 categorical learning attributes
        - Sentiment scores (-1 to 1) for online opinions
        - Device usage patterns tracked
        """)

elif page == "📈 Preprocessing":
    st.markdown("---")
    st.subheader("Data Quality Assessment")

    col1, col2 = st.columns(2)

    with col1:
        st.write("**Missing Values by Column:**")
        missing = df.isnull().sum()
        if missing.sum() == 0:
            st.success("✅ No missing values detected")
        else:
            missing_df = pd.DataFrame(
                {
                    "Column": missing[missing > 0].index,
                    "Missing Count": missing[missing > 0].values,
                }
            )
            st.dataframe(missing_df, use_container_width=True, hide_index=True)

    with col2:
        st.write("**Column Data Types:**")
        dtype_df = pd.DataFrame(
            {"Column": df.columns, "Data Type": df.dtypes.astype(str)}
        )
        st.dataframe(dtype_df, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Numerical Columns Summary")

    numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    if numerical_cols:
        summary_stats = df[numerical_cols].describe().T
        summary_stats.columns = [
            "Count",
            "Mean",
            "Std Dev",
            "Min",
            "25%",
            "50%",
            "75%",
            "Max",
        ]
        st.dataframe(summary_stats.round(2), use_container_width=True)

elif page == "🔍 Analysis":
    st.markdown("---")

    # Get categorical columns
    categorical_cols = [
        "education_level",
        "preferred_learning_method",
        "main_learning_challenge",
        "motivation_level",
        "online_learning_opinion",
        "device_used_for_study",
    ]

    # Filter to only existing columns
    existing_cat_cols = [col for col in categorical_cols if col in df.columns]

    # Select which column to analyze
    selected_col = st.selectbox(
        "Select a categorical attribute to analyze:", existing_cat_cols, index=0
    )

    col1, col2 = st.columns(2)

    with col1:
        # Bar chart
        value_counts = df[selected_col].value_counts()
        fig = px.bar(
            x=value_counts.index,
            y=value_counts.values,
            title=f"Distribution: {selected_col.replace('_', ' ').title()}",
            labels={"x": selected_col.replace("_", " ").title(), "y": "Count"},
            color=value_counts.values,
            color_continuous_scale="Viridis",
        )
        fig.update_layout(showlegend=False, height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Pie chart
        value_counts = df[selected_col].value_counts()
        fig = px.pie(
            values=value_counts.values,
            names=value_counts.index,
            title=f"Percentage Distribution: {selected_col.replace('_', ' ').title()}",
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Two-Variable Analysis")

    col1, col2 = st.columns(2)

    with col1:
        var1 = st.selectbox("First variable:", existing_cat_cols, key="var1")

    with col2:
        var2 = st.selectbox("Second variable:", existing_cat_cols, key="var2", index=1)

    if var1 != var2:
        cross_tab = pd.crosstab(df[var1], df[var2])
        fig = px.imshow(
            cross_tab,
            labels=dict(
                x=var2.replace("_", " ").title(),
                y=var1.replace("_", " ").title(),
                color="Count",
            ),
            title=f"Relationship: {var1.replace('_', ' ').title()} vs {var2.replace('_', ' ').title()}",
            color_continuous_scale="Blues",
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Please select two different variables")

elif page == "💭 Sentiment Analysis":
    st.markdown("---")

    if "compound_score" in df.columns and "sentiment_label" in df.columns:
        st.subheader("Online Learning Opinion - Sentiment Analysis")

        col1, col2, col3 = st.columns(3)

        sentiment_counts = df["sentiment_label"].value_counts()

        with col1:
            st.metric(
                "Positive",
                format_number(sentiment_counts.get("positive", 0)),
                f"{sentiment_counts.get('positive', 0) / len(df) * 100:.1f}%",
            )

        with col2:
            st.metric(
                "Neutral",
                format_number(sentiment_counts.get("neutral", 0)),
                f"{sentiment_counts.get('neutral', 0) / len(df) * 100:.1f}%",
            )

        with col3:
            st.metric(
                "Negative",
                format_number(sentiment_counts.get("negative", 0)),
                f"{sentiment_counts.get('negative', 0) / len(df) * 100:.1f}%",
            )

        st.markdown("---")

        col1, col2 = st.columns(2)

        with col1:
            # Sentiment distribution pie chart
            fig = px.pie(
                values=sentiment_counts.values,
                names=sentiment_counts.index,
                title="Sentiment Distribution",
                color_discrete_map={
                    "positive": "#2ecc71",
                    "neutral": "#95a5a6",
                    "negative": "#e74c3c",
                },
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Compound score distribution
            fig = px.histogram(
                df,
                x="compound_score",
                nbins=50,
                title="Sentiment Score Distribution (VADER)",
                labels={"compound_score": "Compound Score", "count": "Frequency"},
                color_discrete_sequence=["#3498db"],
            )
            fig.add_vline(
                x=0, line_dash="dash", line_color="red", annotation_text="Neutral"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        st.subheader("Sample Opinions by Sentiment")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.write("**Positive Examples:**")
            pos_samples = df[df["sentiment_label"] == "positive"][
                "online_learning_opinion"
            ].head(3)
            for i, text in enumerate(pos_samples, 1):
                st.caption(f"✅ {text}")

        with col2:
            st.write("**Neutral Examples:**")
            neu_samples = df[df["sentiment_label"] == "neutral"][
                "online_learning_opinion"
            ].head(3)
            for i, text in enumerate(neu_samples, 1):
                st.caption(f"⚪ {text}")

        with col3:
            st.write("**Negative Examples:**")
            neg_samples = df[df["sentiment_label"] == "negative"][
                "online_learning_opinion"
            ].head(3)
            for i, text in enumerate(neg_samples, 1):
                st.caption(f"❌ {text}")
    else:
        st.warning("Sentiment analysis columns not found in data")

elif page == "📋 Data Explorer":
    st.markdown("---")
    st.subheader("Browse Dataset")

    col1, col2 = st.columns([3, 1])

    with col1:
        search_term = st.text_input("Filter by text (searches all columns):", "")

    with col2:
        rows_to_show = st.number_input(
            "Rows to display:", min_value=5, max_value=100, value=10
        )

    if search_term:
        mask = (
            df.astype(str)
            .apply(lambda x: x.str.contains(search_term, case=False))
            .any(axis=1)
        )
        filtered_df = df[mask]
        st.write(f"Found {len(filtered_df)} matching records")
        st.dataframe(filtered_df.head(rows_to_show), use_container_width=True)
    else:
        st.dataframe(df.head(rows_to_show), use_container_width=True)

    st.markdown("---")
    st.subheader("Column Selector")

    selected_columns = st.multiselect(
        "Choose columns to display:", df.columns, default=list(df.columns[:5])
    )

    if selected_columns:
        st.dataframe(df[selected_columns], use_container_width=True)

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #888; font-size: 0.8rem; margin-top: 2rem;'>
    Student Learning Analytics Dashboard | Built by Vahid, Martin, Kjetil
    </div>
    """,
    unsafe_allow_html=True,
)
