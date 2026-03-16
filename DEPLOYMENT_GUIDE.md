# Trade Guardian Browser-Only Deployment

This setup gives you:
- Daily 2-file upload flow in `Trade_Guardian_Xlsx.html`
- Snapshot persistence to SQLite
- Automatic Google Drive backup through backend API
- Historical replay via `Trade_Guardian_Fixed.html` + DB download

## 1. Deploy backend on Render

1. Push this repo to GitHub.
2. In Render, create a new Web Service from this repo.
3. Use:
   - Build command: `pip install -r requirements.txt`
   - Start command: `gunicorn -w 2 -b 0.0.0.0:$PORT api_server:app`
4. Set environment variables in Render:
   - `GCP_SERVICE_ACCOUNT_JSON` = full JSON for your service account
   - `DRIVE_DB_FILE_NAME` = `trade_guardian_v4.db` (or your preferred name)
   - `DRIVE_FOLDER_ID` = optional target folder in Google Drive
   - `CORS_ORIGINS` = comma-separated allowed origins (for example your GitHub Pages URL)
5. Ensure the target Google Drive file/folder is shared with the service account email.

## 2. Host HTML on GitHub Pages (Auto-Deploy via GitHub Actions)

This repo now includes:
- `.github/workflows/deploy-pages.yml` (auto deploy workflow)
- `index.html` (landing page linking both apps)

One-time setup:
1. Push this repo to GitHub.
2. In GitHub: **Settings → Pages**
3. Under **Build and deployment**, set:
   - **Source** = `GitHub Actions`
4. Push to `main` (or `master`) to trigger deployment.
5. After deploy, your site URL will be:
   - `https://<your-username>.github.io/<repo>/`
6. Open the site and click **Open Xlsx App**.
7. In the page’s **Cloud Sync** panel:
   - Paste Render base URL (example: `https://your-app.onrender.com`)
   - Click **Save API URL**
   - Click **Check Status**

## 3. Daily workflow

1. Open hosted `Trade_Guardian_Xlsx.html` from any browser/computer.
2. Upload `Active` workbook and `Expired/Closed` workbook.
3. App runs local analytics immediately.
4. App auto-calls `POST /api/sync/daily` and stores snapshots/history.
5. Backend uploads updated DB to Google Drive.

## 4. Historical analysis workflow

1. From Xlsx page, click **Download DB** (or use Google Drive copy).
2. Open hosted/local `Trade_Guardian_Fixed.html`.
3. Upload the downloaded `.db` file to replay lifecycle/snapshot analytics.

## 5. Recommended CORS setting for Render

Set `CORS_ORIGINS` in Render to your GitHub Pages origin, for example:

```text
https://<your-username>.github.io
```

If needed during setup/testing, you can temporarily keep `*`.

## API endpoints

- `POST /api/sync/daily`
  - Form fields: `active_file`, `closed_file`
- `GET /api/sync/status`
- `GET /api/db/meta`
- `GET /api/db/download`

## Local test run (optional)

```bash
python3 -m pip install -r requirements.txt
python3 api_server.py
```

Then open:
- `http://localhost:8000/` (Xlsx UI)
- `http://localhost:8000/fixed` (Fixed UI)
