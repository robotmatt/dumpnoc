# Firebase/Firestore Setup Guide

It looks like your Google Cloud project (`cmt-tools`) exists, but the Firestore database itself hasn't been initialized yet. Follow these steps to fix the "database (default) does not exist" error.

## 1. Initialize Firestore
1.  Go to the [Firestore Console](https://console.cloud.google.com/firestore).
2.  Ensure your project `cmt-tools` is selected in the top dropdown.
3.  Click **Create Database**.
4.  **Database ID**: Select **(default)**. (This is essential, as the app currently looks for the default database).
5.  **Location**: Choose a region close to you (e.g., `nam5` for United States).
6.  **Secure Rules**: Start in **Test Mode** (allows immediate reads/writes for 30 days) or **Production Mode**.
    *   *Note: If you use Production Mode, you must ensure your Service Account has the "Cloud Datastore User" role.*
7.  Click **Create**.

## 2. Verify Service Account Permissions
1.  Go to **IAM & Admin > IAM** in the GCP Console.
2.  Find the Service Account associated with your `firestore_key.json`.
3.  Ensure it has the **Cloud Datastore User** role assigned.

## 3. Enable the API
If you haven't already, make sure the **Firestore API** is enabled:
- [Enable Firestore API](https://console.cloud.google.com/apis/library/firestore.googleapis.com)

## 4. Test the Sync
Once the database is created in the console:
1.  Restart the NOC Scraper app.
2.  Go to the **Sync Data** tab.
3.  Ensure **Enable Cloud Sync** is toggled **ON** in the sidebar.
4.  Click **Upload Scraped Flights History**.

The error should now be resolved, and you should see documents appearing in the `flights` collection in your Firestore console.
