"""
WHO Publications source config.

WHO's publications listing page is rendered client-side by JavaScript,
so a plain HTTP fetch of the page never sees the actual list. The page
itself calls this JSON API to populate the list (visible in browser
DevTools > Network tab) — we call it directly instead.

This is the ENTIRE amount of WHO-specific code in the system. Adding
CDC or NIH later means adding a sibling file here — the crawler engine
itself never changes.
"""
from who_crawler.api_models import ApiListingSourceConfig

WHO_PUBLICATIONS_API_CONFIG = ApiListingSourceConfig(
    source_id="who_publications",
    business_unit="health-intelligence",
    api_url=(
        "https://www.who.int/api/hubs/publications"
        "?sf_site=15210d59-ad60-47ff-a542-7ed76645f0c7"
        "&sf_provider=OpenAccessProvider"
        "&sf_culture=en"
        "&$orderby=PublicationDateAndTime%20desc"
        "&$select=Title,ItemDefaultUrl,FormatedDate,Tag,ThumbnailUrl,DownloadUrl,TrimmedTitle"
    ),
    page_size=50,
    max_items=200,
    items_path="value",
    title_field="Title",
    url_field="ItemDefaultUrl",
    url_base="https://www.who.int/publications/i/item",
    date_field="FormatedDate",
    download_field="DownloadUrl",
)
