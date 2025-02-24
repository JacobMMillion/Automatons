import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from apify_client import ApifyClient
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
from datetime import datetime
from zoneinfo import ZoneInfo

from db_manager import DailyVideoDataDB

# Load environment variables from a .env file
load_dotenv()

# ----------------------------
# Environment & Credentials Setup
# ----------------------------

SERVICE_ACCOUNT_INFO = {
    "type": "service_account",
    "project_id": os.environ.get("GCLOUD_PROJECT_ID"),
    "private_key_id": os.environ.get("GCLOUD_PRIVATE_KEY_ID"),
    "private_key": os.environ.get("GCLOUD_PRIVATE_KEY").replace("\\n", "\n"),
    "client_email": os.environ.get("GCLOUD_CLIENT_EMAIL"),
    "client_id": os.environ.get("GCLOUD_CLIENT_ID"),
    "auth_uri": os.environ.get("GCLOUD_AUTH_URI"),
    "token_uri": os.environ.get("GCLOUD_TOKEN_URI"),
    "auth_provider_x509_cert_url": os.environ.get("GCLOUD_AUTH_PROVIDER_X509_CERT_URL"),
    "client_x509_cert_url": os.environ.get("GCLOUD_CLIENT_X509_CERT_URL")
}

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

credentials = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=SCOPES)
gs_client = gspread.authorize(credentials)

APIFY_API_KEY = os.environ.get("APIFY_API_KEY")
APIFY_CLIENT = ApifyClient(APIFY_API_KEY)

# APPS AND THE ASSOCIATED ASSOCIATES WITH INFLUENCER MANAGEMENT PAGES

PROJECTS = {"Astra": ["Cara", "Chad", "Gray", "Dylano", "Jake"]}

# PROJECTS = {
#     "Astra": ["Cara", "Chad", "Gray", "Dylano", "Jake"],
#     "Haven": ["Jake", "Dylano", "Blaise"],
#     "Berry": ["Cara", "Alina", "Ashley"],
#     "Saga": ["Alina", "Avi", "Dylan", "Jake"]
# }

# SHEETS (lowercased) THAT SHOULD BE SKIPPED DURING PROCESSING
SKIP_TABS = [
"current deals",
"vidhistory",
"vidhistoryandprojected",
"changelog",
"stats",
"template",
"all",
"schedule",
"content schedule",
"example",
"active",
"sheet17",
"blank"
]

# STARTING ROW AND COLUMN DEFINITIONS
# Note: gspread indexes columns numerically starting at 1.
START_ROW = 6

URL_COL_LETTER = "G"
URL_COL_NUM = 7

VIEW_COL_LETTER = "H"

# ----------------------------
# HELPER FUNCTIONS
# ----------------------------
def reformat_date_to_est(date_str, fmt="%Y-%m-%d %H:%M:%S"):
    """
    Converts an ISO 8601 UTC date string (ending in 'Z') into an EST formatted string.
    
    Example:
      "2024-10-22T18:09:31.000Z" -> "2024-10-22 14:09:31" (if EST is UTC-4 at that date)
    """
    if not date_str:
        return None
    # Replace trailing "Z" with "+00:00" so that Python can parse it as UTC.
    if date_str.endswith("Z"):
        date_str = date_str.replace("Z", "+00:00")
    # Parse the ISO date (it will be timezone-aware)
    dt = datetime.fromisoformat(date_str)
    # Convert to Eastern Time
    dt_est = dt.astimezone(ZoneInfo("America/New_York"))
    return dt_est.strftime(fmt)
# ----------------------------
# ----------------------------


# ----------------------------
# Core Functionality
#
# orchestrate_all_scraping -> 
# run_individual_scrape ->
# iterate_over_tabs ->
# process_tab ->
# hit_apify -> log
# ----------------------------


# ----------------------------
# LOGS DATA TO DATABASE
# ----------------------------
def log(url, username, associate, app, view_count, comment_count, caption, created_at, insert_time):
    
    # log_message = (
    #     "========== LOG INFO ==========\n"
    #     f"URL: {url}\n"
    #     f"Username: {username}\n"
    #     f"Associate: {associate}\n"
    #     f"App: {app}\n"
    #     f"View Count: {view_count}\n"
    #     f"Comment Count: {comment_count}\n"
    #     f"Caption: {caption}\n"
    #     f"Created At: {created_at}\n"
    #     f"Insert Time: {insert_time}\n"
    #     "=============================="
    # )
    # print(log_message)

    db = DailyVideoDataDB()

    db.ensure_table_exists()

    try:
        view_id = db.insert_row(
            url, 
            username, 
            associate, 
            app, 
            view_count, 
            comment_count, 
            caption, 
            created_at, 
            insert_time
        )
        print(f"Inserted record with id: {view_id}")
    except Exception as e:
        print(f"Error inserting record into DailyVideoData: {e}")


# ----------------------------
# HITS APIFY API FOR MULTIPLE URLS (exlusively tiktok or exclusively insta)
# RETURNS THE NUMBER OF VIEWS (list)
# CALLS FUNCTION TO LOG FURTHER DATA TO DB FOR EACH
# ----------------------------
# def hit_apify_batch(workbook, urls, url_type):
#     print("BATCH PROCESSING")
#     try:
#         # Prepare the actor input and select the actor based on URL type.
#         if url_type == "tiktok":
#             run_input = {
#                 "excludePinnedPosts": True,
#                 "postURLs": urls,  # Batch processing: list of URLs
#                 "resultsPerPage": 1,
#                 "shouldDownloadCovers": False,
#                 "shouldDownloadSlideshowImages": False,
#                 "shouldDownloadSubtitles": False,
#                 "shouldDownloadVideos": False,
#                 "searchSection": "",
#                 "maxProfilesPerQuery": 10
#             }
#             actor_link = "clockworks/free-tiktok-scraper"
#         elif url_type == "instagram":
#             run_input = {
#                 "addParentData": False,
#                 "directUrls": urls,  # Batch processing: list of URLs
#                 "enhanceUserSearchWithFacebookPage": False,
#                 "isUserReelFeedURL": False,
#                 "isUserTaggedFeedURL": False,
#                 "resultsLimit": 1,
#                 "resultsType": "details",
#                 "searchLimit": 1,
#                 "searchType": "hashtag"
#             }
#             actor_link = "apify/instagram-scraper"
#         else:
#             print(f"Unsupported url_type: {url_type}")
#             return {url: 0 for url in urls}

#         # Set keys based on URL type.
#         if url_type == "tiktok":
#             view_key = "playCount"
#             timestamp_key = "createTimeISO"
#             comment_key = "commentCount"
#             caption_key = "text"
#         elif url_type == "instagram":
#             view_key = "videoPlayCount"
#             timestamp_key = "timestamp"
#             comment_key = "commentsCount"
#             caption_key = "caption"

#         # Run the actor with the batch input.
#         run = APIFY_CLIENT.actor(actor_link).call(run_input=run_input)
#         dataset_id = run.get("defaultDatasetId")
        
#         # Convert the dataset items into a list to preserve order.
#         items = list(APIFY_CLIENT.dataset(dataset_id).iterate_items())
#         results = {}
        
#         # Iterate over each input URL using its index.
#         for idx, input_url in enumerate(urls):
#             # Default values if no corresponding item is returned.
#             view_count = 0
#             created_at = None
#             comment_count = None
#             caption = None
#             username = None

#             # If an item exists at this index, extract the values.
#             if idx < len(items):
#                 item = items[idx]
#                 view_count = item.get(view_key, 0)
#                 raw_created_at = item.get(timestamp_key, None)
#                 created_at = reformat_date_to_est(raw_created_at) if raw_created_at else None
#                 comment_count = item.get(comment_key, None)
#                 caption = item.get(caption_key, None)
#                 if caption is not None:
#                     caption = caption.replace("\n", " ").replace("\r", " ")
#                 # Extract username.
#                 if url_type == "tiktok":
#                     author_meta = item.get("authorMeta", {})
#                     username = author_meta.get("name", None)
#                 else:
#                     username = item.get("ownerUsername", None)

#             # Extract app and associate from workbook.title.
#             parts = workbook.title.split()
#             app = parts[0] if parts else ""
#             associate = parts[-1] if parts else ""
#             insert_time = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S")

#             # Log the details using the input URL.
#             log(input_url, username, associate, app, view_count, comment_count, caption, created_at, insert_time)

#             # Save the view count result keyed by the input URL.
#             results[input_url] = view_count

#         # Ensure every input URL is represented in the results.
#         for url in urls:
#             if url not in results:
#                 results[url] = 0

#         return results

#     except Exception as e:
#         print(f"Error processing batch for {url_type}: {str(e)}")
#         return {url: 0 for url in urls}


# ----------------------------
# HITS APIFY API FOR A URL (for tiktok or insta)
# RETURNS THE NUMBER OF VIEWS
# CALLS FUNCTION TO LOG FURTHER DATA TO DB
# ----------------------------
def hit_apify(workbook, url):

    tiktok_regex = r"tiktok"
    instagram_regex = r"instagram"

    if re.search(tiktok_regex, url):
        url_type = "tiktok"
    elif re.search(instagram_regex, url):
        url_type = "instagram"
    else:
        print(f"URL '{url}' does not match TikTok or Instagram. Skipping.")
        return 0    

    try:
        # Prepare the Actor input based on URL type
        if url_type == "tiktok":  # Tiktok  
            run_input = {
                "excludePinnedPosts": True,
                "postURLs": [url],           # Process a single URL
                "resultsPerPage": 1,
                "shouldDownloadCovers": False,
                "shouldDownloadSlideshowImages": False,
                "shouldDownloadSubtitles": False,
                "shouldDownloadVideos": False,
                "searchSection": "",
                "maxProfilesPerQuery": 10
            }
        elif url_type == "instagram":  # Instagram
            run_input = {
                "addParentData": False,
                "directUrls": [url],         # Process a single URL
                "enhanceUserSearchWithFacebookPage": False,
                "isUserReelFeedURL": False,
                "isUserTaggedFeedURL": False,
                "resultsLimit": 1,
                "resultsType": "details",
                "searchLimit": 1,
                "searchType": "hashtag"
            }

        # Choose the correct actor based on URL type
        if url_type == "tiktok":
            actor_link = "clockworks/free-tiktok-scraper"
        elif url_type == "instagram":
            actor_link = "apify/instagram-scraper"

        # Set keys based on URL type
        if url_type == "tiktok":
            view_key = "playCount"
            timestamp_key = "createTimeISO"
            comment_key = "commentCount"
            caption_key = "text"
        elif url_type == "instagram":
            view_key = "videoPlayCount"
            timestamp_key = "timestamp"
            comment_key = "commentsCount"
            caption_key = "caption"

        # Run the Actor for the single URL and wait for it to finish
        run = APIFY_CLIENT.actor(actor_link).call(run_input=run_input)

        # Retrieve and update the view count for the current URL
        item = next(APIFY_CLIENT.dataset(run["defaultDatasetId"]).iterate_items(), None)

        if item:

            view_count = item.get(view_key, 0)
            comment_count = item.get(comment_key, None)

            # Convert to EST with consistant format
            raw_created_at = item.get(timestamp_key, None)
            created_at = reformat_date_to_est(raw_created_at)
    
            caption = item.get(caption_key, None)
            if caption is not None:
                caption = caption.replace("\n", " ").replace("\r", " ")

            # Extract the username based on platform
            if url_type == "tiktok":
                author_meta = item.get("authorMeta", {})
                username = author_meta.get("name", None)
            elif url_type == "instagram":
                username = item.get("ownerUsername", None)

            parts = workbook.title.split()
            app = parts[0] if parts else ""
            associate = parts[-1] if parts else ""
            insert_time = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S")

            log(url, username, associate, app, view_count, comment_count, caption, created_at, insert_time)

            return view_count
        
        else:
            return 0

    except Exception as e:
        print(f"Error processing url {url}: {str(e)}")
        return 0

# ----------------------------
# PROCESSES A SINGLE TAB IN AN ASSOCIATES GOOGLE SHEET
# WRITES THE UPDATED VIEW FOR EACH URL BACK TO THE TAB
# ----------------------------
def process_tab(workbook, sheet):
    print(f"Beginning to process {sheet} in {workbook.title}")
    # Determine the last row in column G by getting all values.
    col_values = sheet.col_values(URL_COL_NUM)
    last_filled_row = max(START_ROW, len(col_values))

    # Retrieve all cells in that range in one API call
    read_range = f"{URL_COL_LETTER}{START_ROW}:{URL_COL_LETTER}{last_filled_row}"
    urls_data = sheet.get(read_range)

    # # Categorize URLs by platform
    # tiktok_urls = []
    # instagram_urls = []
    # row_numbers = []  # store original row positions for individual processing
    
    # for idx, row in enumerate(urls_data):
    #     if row and row[0]:
    #         url = row[0]
    #         row_numbers.append(idx)
    #         if "tiktok.com" in url.lower():
    #             tiktok_urls.append(url)
    #         elif "instagram.com" in url.lower():
    #             instagram_urls.append(url)
    #         else:
    #             # If URL doesn't match TikTok or Instagram, treat it as "other"
    #             # so that we fall back to individual processing
    #             tiktok_urls.append(url)  # Force individual processing
    #             instagram_urls.append(url)  # (by ensuring both lists are non-empty)
    
    view_counts = [[""] for _ in urls_data]  # default empty results

    # # Check if all URLs are from a single platform:
    # if tiktok_urls and not instagram_urls:
    #     # All URLs are TikTok
    #     results = hit_apify_batch(workbook, tiktok_urls, "tiktok")
    #     # Populate view_counts for TikTok URLs in order
    #     for i, url in enumerate(urls_data):
    #         if url and url[0] in results:
    #             view_counts[i] = [results[url[0]]]
    # elif instagram_urls and not tiktok_urls:
    #     # All URLs are Instagram
    #     results = hit_apify_batch(workbook, instagram_urls, "instagram")
    #     # Populate view_counts for Instagram URLs in order
    #     for i, url in enumerate(urls_data):
    #         if url and url[0] in results:
    #             view_counts[i] = [results[url[0]]]
    # else:

    # Mixed platforms or non-standard URLs; process individually.
    for i, row in enumerate(urls_data):
        if row and row[0]:
            count = hit_apify(workbook, row[0])
            view_counts[i] = [count]

    # Write all view counts back to column I in one API call
    update_range = f"{VIEW_COL_LETTER}{START_ROW}:{VIEW_COL_LETTER}{last_filled_row}"

    try:
        sheet.update(values=view_counts, range_name=update_range)
    except Exception as e:
        print("ERROR writing to sheet: ", e)

    print(f"{sheet} range {update_range} updated with view counts: {view_counts}")

    # After processing the tab, update cell H5 with a hover note that shows the last update time
    update_time = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S")
    note_text = f"Last updated: {update_time}"
    try:
        sheet.update_note("H5", note_text)
    except Exception as e:
        print(f"Error updating note on cell H5 in sheet '{sheet.title}': {e}")


# ----------------------------
# ITERATES OVER ALL TABS IN AN ASSOCIATES GOOGLE SHEET
# ----------------------------
def iterate_over_tabs(workbook):
    # Filter out sheets that should be skipped
    sheets_to_process = [
        sheet for sheet in workbook.worksheets()
        if sheet.title.lower() not in SKIP_TABS
    ]

    # Process each sheet concurrently
    with ThreadPoolExecutor(max_workers=6) as executor:
        future_to_sheet = {
            executor.submit(process_tab, workbook, sheet): sheet
            for sheet in sheets_to_process
        }
        for future in as_completed(future_to_sheet):
            sheet = future_to_sheet[future]
            try:
                future.result()
                print(f"Finished processing tab: {sheet.title}")
            except Exception as e:
                print(f"Error processing tab {sheet.title}: {e}")


# ----------------------------
# SCRAPE A SPECIFIC ASSOCIATE FOR AN APP
# ----------------------------
def run_individual_scrape(name, project, client):

    # Try to open the google sheet
    workbook_name = f"{project} - Influencer Management - {name}"
    try:
        workbook = client.open(workbook_name)
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"ERROR: Workbook '{workbook_name}' not found. Skipping.")
        return

    # Make it here => successfully accessed the google sheet3
    print(f"Successfully accessed the associate workbook: {workbook.title}")

    iterate_over_tabs(workbook)


# ----------------------------
# START HERE
# ----------------------------
def orchestrate_all_scraping():

    client = gspread.authorize(credentials)

    # Iterate through each project, associates dictionary
    for project, employees in PROJECTS.items():

        # Iterate over each associate within a specific project
        for employee in employees:
            run_individual_scrape(employee, project, client)


# ----------------------------
# MAIN, kickoff
# ----------------------------
orchestrate_all_scraping()