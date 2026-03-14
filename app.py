import streamlit as st
import os

st.set_page_config(
    page_title="BI Portfolio - Camilo Rojas",
    page_icon="📊",
    layout="wide",
)

st.title("Business Intelligence Portfolio")
st.markdown("Public professional portfolio of BI projects using Tableau and Power BI")

st.divider()

# Define project details
projects = [
    {
        "name": "Advanced CALCULATE",
        "file": "Advanced CALCULATE.pbix",
        "description": "Demonstrates advanced usage of the CALCULATE function in DAX, "
        "including complex filter context manipulation and nested CALCULATE patterns.",
        "tags": ["DAX", "CALCULATE", "Filter Context"],
    },
    {
        "name": "Advanced Time Intelligence",
        "file": "Advanced Time Intelligence.pbix",
        "description": "Explores time intelligence functions for year-over-year comparisons, "
        "rolling averages, and custom fiscal calendar calculations.",
        "tags": ["DAX", "Time Intelligence", "Date Tables"],
    },
    {
        "name": "Calculated Table Joins",
        "file": "Calculated Table Joins.pbix",
        "description": "Showcases techniques for creating calculated tables and using them "
        "to build flexible data model relationships.",
        "tags": ["DAX", "Calculated Tables", "Data Modeling"],
    },
    {
        "name": "Iterators",
        "file": "Iterators.pbix",
        "description": "Covers iterator functions like SUMX, AVERAGEX, and MAXX for "
        "row-by-row evaluation across tables.",
        "tags": ["DAX", "Iterators", "SUMX", "AVERAGEX"],
    },
    {
        "name": "Performance Tuning",
        "file": "Performance Tuning.pbix",
        "description": "Best practices for optimizing DAX queries and Power BI report "
        "performance, including storage mode strategies.",
        "tags": ["Performance", "Optimization", "DAX"],
    },
    {
        "name": "Relationship Functions",
        "file": "Relationship Functions.pbix",
        "description": "Demonstrates RELATED, RELATEDTABLE, USERELATIONSHIP, and other "
        "functions for navigating data model relationships.",
        "tags": ["DAX", "Relationships", "Data Modeling"],
    },
    {
        "name": "Scalar Functions",
        "file": "Scalar Functions.pbix",
        "description": "A comprehensive look at scalar DAX functions for calculations "
        "that return single values.",
        "tags": ["DAX", "Scalar Functions"],
    },
    {
        "name": "Table & Filter Functions",
        "file": "Table & Filter Functions.pbix",
        "description": "Explores table manipulation and filter functions including "
        "FILTER, ALL, VALUES, and DISTINCT.",
        "tags": ["DAX", "Table Functions", "Filter Functions"],
    },
]

# Display projects in a grid
cols = st.columns(2)

for i, project in enumerate(projects):
    with cols[i % 2]:
        with st.container(border=True):
            st.subheader(project["name"])
            st.write(project["description"])
            st.caption(" | ".join(project["tags"]))

            file_path = os.path.join("power_bi_portfolio", project["file"])
            if os.path.exists(file_path):
                with open(file_path, "rb") as f:
                    st.download_button(
                        label="Download .pbix",
                        data=f,
                        file_name=project["file"],
                        mime="application/octet-stream",
                        key=f"download_{i}",
                    )

st.divider()
st.caption("Built with Streamlit | Camilo Rojas - BI Portfolio")
