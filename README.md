# Automatons

Automatons is a project designed to manage and update influencer data. The project consists of two main components:

- **Website:** A user interface to view and interact with the database.
- **Scripts:** Backend scripts to update and log data related to influencer engagement.

## Project Structure

- **Website:**  
  The website allows users to view and interact with the database. This interface displays updated views, engagement metrics, comments, captions, and other data logged in the database.

- **db_manager.py:**  
  This module handles all interactions with the database. It includes functions to:
  - Log additional data for each URL such as comments and captions.

- **run_apify_update.py:**  
  This script uses `db_manager.py` to iterate over data and update influencer metrics. Specifically, it:
  - Iterates over each influencer.
  - For each marketing associate and for each app, it updates the view counts in their respective sheets.
  - Logs other engagement data (such as comments and captions) to the database.

## Getting Started

### Prerequisites
- requirements.txt can be recursively install all prerequisites

env note: using vertenv
