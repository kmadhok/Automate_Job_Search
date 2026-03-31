#!/usr/bin/env python3
"""
Generates a tailored PDF resume for Kanu Madhok matching the exact format
of the original resume, with bullet points and skills reordered based on
the target job description keywords.

Usage:
    python generate_tailored_resume.py \
        --company "Palantir" \
        --title "Forward Deployed AI Engineer" \
        --keywords "agentic,RAG,LLM,production,deployment,customer-facing,Gen AI" \
        --output "output_resume.pdf"
"""

import argparse
import json
import os
import re
import sys
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether
)
from reportlab.lib.colors import black, HexColor


# ── Resume content ──────────────────────────────────────────────────────────

CONTACT_LINE = "952-303-1045 \u2022 madhok.kanu@gmail.com \u2022 github.com/kmadhok \u2022 linkedin/in/kanu-madhok"

# Each role has bullets that can be reordered. The first bullet in each list
# is the "default lead" but will be moved if another bullet scores higher for
# the target job's keywords.

# Advanced AI bullets from master experience — surfaced for AI-heavy roles
WALMART_AI_ADVANCED_BULLETS = [
    "Designed and built a full agentic AI stack: knowledge graph of table joins and relationships, an MCP server exposing RAG and knowledge graph as callable tools, and a CodePuppy agent integration \u2014 creating a unified AI-assisted data analysis infrastructure.",
    "Built an autonomous multi-agent data analyst system with specialized agents (explorer, analyst, auditor, fixer) using coordinated handoffs and quality control loops for end-to-end automated data analysis workflows.",
    "Developed an MCP (Model Context Protocol) server enabling coding agents and CLI tools to programmatically query the knowledge graph for join paths and send analytical questions to the RAG system.",
]

# Config flag to control when advanced bullets are included
try:
    from config import INCLUDE_ADVANCED_AI_BULLETS
except ImportError:
    INCLUDE_ADVANCED_AI_BULLETS = True

AI_HEAVY_KEYWORDS = [
    "agentic", "agent", "multi-agent", "MCP", "knowledge graph",
    "tool-calling", "tool use", "RAG", "copilot", "LLM",
]

WALMART_BULLETS = [
    "Led build and launch of a GenAI text-to-SQL copilot over Walmart BigQuery schemas (RAG + 12K-query corpus) enabling users to self-serve analysis; 95% SQL accuracy, ~80% faster query creation, saving ~20 hours per 100 queries.",
    "Decreased survey development and targeting time by 30% by engineering RFM and demographic profiles in BigQuery, analyzing 270M+ customer records to precisely align niche behavioral segments with supplier criteria.",
    "Developed a Logistic Regression propensity-to-respond model on 700K+ panelists, increasing survey completion by 22% while reducing survey fatigue by cutting outreach volume by 40%.",
    "Developed a Power BI dashboard with DAX-based KPI definitions and segmentation views, integrated with BigQuery + a text-to-SQL copilot; enabled supplier teams to self-serve targeting decisions, driving 32% more launches, ~$50K incremental revenue, and +10% responses.",
]

UCHICAGO_RESEARCH_BULLETS = [
    "Built a persona-driven multi-LLM (OpenAI + Gemini) with temperature/persona tuning chatbot for a staged donation experiment (500+ participants), identifying political concerns and persuading 15% to donate to an opposing cause via controlled A/B tests.",
]

INNOVARE_BULLETS = [
    "Delivered analytics and Looker Studio dashboards for 10+ districts (5,000+ students), aligning KPIs and intervention reporting.",
    "Built a logistic regression risk model and surfaced weekly risk scores in dashboards, improving retention +7% and reducing failures 15%.",
    "Built an automated ELT pipeline to BigQuery (Dataprep, SQL, Python) with QA checks, improving reporting accuracy 23% and reducing manual prep.",
]

FTI_BULLETS = [
    "Developed and deployed a RAG application that ingests prior proposals to generate new drafts; cut proposal time ~30% (3+ hours saved per project) and integrated into team workflow.",
    "Improved generation by 34% through query rewriting and retrieval by 42% through adaptive chunking; packaged as a Dockerized Streamlit app deployed on AWS ec2 machines.",
    "Won Best in Show in University of Chicago\u2019s MS in Applied Data Science Capstone",
]

PROJECT_BULLETS = [
    "Built a Streamlit text-to-SQL copilot (RAG over historical queries) that cuts analysis turnaround ~80% by retrieving relevant SQL + schema context, generating validated BigQuery SQL, executing it, and returning answers with auto-visualizations (Gemini 2.5).",
]
PROJECT_LINK = "Deployed Project: https://sql-rag-frontend-simple-481433773942.us-central1.run.app/"

# Skills grouped by category — order within each group can be adjusted
SKILLS = {
    "LLM/NLP and Retrieval": [
        "RAG", "NL->SQL", "evaluation pipelines", "agentic workflows", "context engineering"
    ],
    "ML/Stats": [
        "Python", "SQL", "R", "PySpark", "TensorFlow", "PyTorch", "scikit-learn",
        "A/B testing", "time series", "clustering", "Bayesian", "ensembles"
    ],
    "Data and Infra": [
        "BigQuery", "Databricks", "MySQL", "Hive", "Docker", "Streamlit",
        "AWS/GCP", "Tableau", "Power BI", "Looker Studio"
    ],
}


# ── Keyword scoring ────────────────────────────────────────────────────────

def score_text(text: str, keywords: list[str]) -> int:
    """Score a text block by how many keywords it matches (case-insensitive)."""
    text_lower = text.lower()
    return sum(1 for kw in keywords if kw.lower() in text_lower)


def reorder_bullets(bullets: list[str], keywords: list[str]) -> list[str]:
    """Return bullets sorted by keyword relevance (descending), preserving
    relative order among ties."""
    scored = [(score_text(b, keywords), i, b) for i, b in enumerate(bullets)]
    scored.sort(key=lambda x: (-x[0], x[1]))
    return [b for _, _, b in scored]


def reorder_skills(skills_dict: dict, keywords: list[str]) -> dict:
    """Reorder skills within each category by keyword relevance."""
    reordered = {}
    for category, items in skills_dict.items():
        scored = [(score_text(s, keywords), i, s) for i, s in enumerate(items)]
        scored.sort(key=lambda x: (-x[0], x[1]))
        reordered[category] = [s for _, _, s in scored]
    return reordered


def extract_keywords_from_description(description: str) -> list[str]:
    """Pull resume-relevant keywords from a job description using KEYWORD_LIBRARY."""
    try:
        from generate_resume_tailoring import KEYWORD_LIBRARY
    except ImportError:
        KEYWORD_LIBRARY = [
            "python", "sql", "machine learning", "llm", "rag",
            "agents", "prompt engineering", "a/b testing",
            "deep learning", "large language models",
        ]

    found = []
    desc_lower = description.lower()
    for kw in KEYWORD_LIBRARY:
        if kw.lower() in desc_lower:
            found.append(kw)
    return found


# ── PDF generation ─────────────────────────────────────────────────────────

def build_styles():
    """Create paragraph styles matching the original resume format."""
    styles = {}

    styles['Name'] = ParagraphStyle(
        'Name', fontName='Helvetica-Bold', fontSize=20,
        alignment=TA_CENTER, spaceAfter=2, leading=24,
    )
    styles['Contact'] = ParagraphStyle(
        'Contact', fontName='Helvetica', fontSize=9,
        alignment=TA_CENTER, spaceAfter=4, leading=11,
        textColor=black,
    )
    styles['SectionHeader'] = ParagraphStyle(
        'SectionHeader', fontName='Helvetica-Bold', fontSize=11,
        spaceBefore=6, spaceAfter=2, leading=13,
        borderWidth=0, borderPadding=0,
    )
    styles['CompanyLine'] = ParagraphStyle(
        'CompanyLine', fontName='Helvetica-Bold', fontSize=9.5,
        spaceAfter=0, leading=12,
    )
    styles['TitleLine'] = ParagraphStyle(
        'TitleLine', fontName='Helvetica-Bold', fontSize=9,
        spaceAfter=1, leading=11,
    )
    styles['Bullet'] = ParagraphStyle(
        'Bullet', fontName='Helvetica', fontSize=9,
        leftIndent=18, firstLineIndent=-10,
        spaceAfter=1.5, leading=11.5,
        alignment=TA_LEFT,
    )
    styles['BulletText'] = ParagraphStyle(
        'BulletText', fontName='Helvetica', fontSize=9,
        leftIndent=18,
        spaceAfter=1.5, leading=11.5,
        alignment=TA_LEFT,
    )
    styles['SkillCategory'] = ParagraphStyle(
        'SkillCategory', fontName='Helvetica', fontSize=9,
        leftIndent=18, firstLineIndent=-10,
        spaceAfter=1.5, leading=11.5,
    )
    styles['Education'] = ParagraphStyle(
        'Education', fontName='Helvetica', fontSize=9,
        spaceAfter=2, leading=12,
    )
    return styles


def two_col_line(left_text: str, right_text: str, style_name: str, styles: dict,
                 left_bold=True, right_bold=False) -> Table:
    """Create a two-column line (e.g. Company | Location)."""
    left_font = 'Helvetica-Bold' if left_bold else 'Helvetica'
    right_font = 'Helvetica-Bold' if right_bold else 'Helvetica'
    fs = styles[style_name].fontSize

    left_para = Paragraph(
        f'<font face="{left_font}" size="{fs}">{left_text}</font>',
        styles[style_name]
    )
    right_para = Paragraph(
        f'<font face="{right_font}" size="{fs}">{right_text}</font>',
        ParagraphStyle('right', parent=styles[style_name], alignment=TA_RIGHT)
    )
    t = Table([[left_para, right_para]], colWidths=[4.2 * inch, 2.55 * inch])
    t.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ]))
    return t


def section_divider():
    """Horizontal rule under section headers."""
    return HRFlowable(width="100%", thickness=0.75, color=black,
                       spaceBefore=0, spaceAfter=3)


def bullet_paragraph(text: str, styles: dict) -> Paragraph:
    """Create a bullet point paragraph."""
    return Paragraph(f"\u2022  {text}", styles['Bullet'])


def generate_resume_pdf(output_path: str, keywords: list[str]):
    """Generate the full tailored resume PDF."""
    styles = build_styles()

    doc = SimpleDocTemplate(
        output_path,
        pagesize=letter,
        topMargin=0.45 * inch,
        bottomMargin=0.4 * inch,
        leftMargin=0.65 * inch,
        rightMargin=0.65 * inch,
    )

    story = []

    # ── Header ──
    story.append(Paragraph("Kanu Madhok", styles['Name']))
    story.append(Paragraph(CONTACT_LINE, styles['Contact']))

    # ── Professional Experience ──
    story.append(Paragraph("Professional Experience", styles['SectionHeader']))
    story.append(section_divider())

    # Walmart
    story.append(two_col_line("Walmart", "Chicago, IL", 'CompanyLine', styles))
    story.append(two_col_line("Senior Data Analyst", "September 2024 - Present", 'TitleLine', styles))

    walmart_bullets = list(WALMART_BULLETS)
    if INCLUDE_ADVANCED_AI_BULLETS:
        # Check if the keywords suggest an AI-heavy role
        ai_keyword_count = sum(1 for kw in AI_HEAVY_KEYWORDS if kw.lower() in " ".join(keywords).lower())
        if ai_keyword_count >= 2:
            walmart_bullets.extend(WALMART_AI_ADVANCED_BULLETS)

    for b in reorder_bullets(walmart_bullets, keywords):
        story.append(bullet_paragraph(b, styles))

    story.append(Spacer(1, 3))

    # UChicago Research
    story.append(two_col_line(
        "Data Science Institute at The University of Chicago", "Chicago, IL",
        'CompanyLine', styles))
    story.append(two_col_line(
        "Graduate Student Researcher for Data &amp; Democracy Research Initiative",
        "January 2024 - September 2024", 'TitleLine', styles))
    for b in reorder_bullets(UCHICAGO_RESEARCH_BULLETS, keywords):
        story.append(bullet_paragraph(b, styles))

    story.append(Spacer(1, 3))

    # Innovare
    story.append(two_col_line("Innovare (EdTech Startup)", "Chicago, IL", 'CompanyLine', styles))
    story.append(two_col_line("Data Scientist", "October 2022 - March 2023", 'TitleLine', styles))
    for b in reorder_bullets(INNOVARE_BULLETS, keywords):
        story.append(bullet_paragraph(b, styles))

    story.append(Spacer(1, 3))

    # FTI Consulting
    story.append(two_col_line("FTI Consulting", "Chicago, IL", 'CompanyLine', styles))
    story.append(two_col_line(
        "Consulting Project Proposal for FTI Consulting",
        "January 2024 - August 2024", 'TitleLine', styles))
    for b in reorder_bullets(FTI_BULLETS, keywords):
        story.append(bullet_paragraph(b, styles))

    # ── Projects ──
    story.append(Paragraph("Projects", styles['SectionHeader']))
    story.append(section_divider())
    for b in PROJECT_BULLETS:
        story.append(bullet_paragraph(b, styles))
    story.append(Paragraph(
        f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;{PROJECT_LINK}",
        ParagraphStyle('link', parent=styles['BulletText'], textColor=HexColor('#1155CC'))
    ))

    # ── Skills ──
    story.append(Paragraph("Skills", styles['SectionHeader']))
    story.append(section_divider())
    reordered = reorder_skills(SKILLS, keywords)
    for category, items in reordered.items():
        text = f"\u2022  <b>{category}:</b> {', '.join(items)}"
        story.append(Paragraph(text, styles['SkillCategory']))

    # ── Education ──
    story.append(Paragraph("Education", styles['SectionHeader']))
    story.append(section_divider())
    story.append(Paragraph(
        "<b>The University of Chicago</b> \u2013 Master of Science in Applied Data Science | GPA 4.0",
        styles['Education']))
    story.append(Paragraph(
        "<b>Loyola University Chicago</b> \u2013 Bachelor of Science in Information Systems and Economics | GPA 3.5",
        styles['Education']))

    doc.build(story)
    return output_path


# ── CLI ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Generate a tailored resume PDF")
    parser.add_argument("--company", required=True, help="Target company name")
    parser.add_argument("--title", required=True, help="Target job title")
    parser.add_argument("--keywords", required=True,
                        help="Comma-separated keywords from the job description")
    parser.add_argument("--output", required=True, help="Output PDF path")
    args = parser.parse_args()

    keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
    path = generate_resume_pdf(args.output, keywords)
    print(f"Resume generated: {path}")


if __name__ == "__main__":
    main()
