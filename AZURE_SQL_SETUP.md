# ✅ Azure SQL Database Setup for Shared Class Project (Power BI + Mac/Windows)

This guide explains how to set up a shared **Azure SQL Database** that:
- Works on both **Mac & Windows**
- Can be connected to using **Power BI Desktop (Windows)**
- Allows multiple teammates to access the same dataset
- Doesn't require everyone to install a full database locally

---

## ✅ PART 1 — Create the Azure SQL Database (Free Tier)

### 1. Sign in or create an Azure account
- Go to https://portal.azure.com
- Sign in or create a free account

### 2. Create a new SQL Database
1. Click **Create a Resource**
2. Select **Databases → SQL Database**
3. Fill in:
   - **Subscription:** Free Tier (or your active one)
   - **Resource Group:** e.g., `TechMapGroup`
   - **Database Name:** `UnemploymentAnalyticsDB`
   - **Server:** Click *Create new*
     - Server name: e.g., `techmapsqlserver`
     - Region: pick something close to you
     - Authentication: SQL login (create username + password)
4. Under **Compute + Storage**, choose:
   ✅ *Basic tier* OR *Free tier* option
5. Click **Review + Create** → then **Create**

Deployment takes about 1–3 minutes.

---

## ✅ PART 2 — Enable Public Access (Firewall Rules)

### 3. Allow teammates to connect
1. Open the **SQL Server** resource (not just the database)
2. Go to **Networking → Firewall rules**
3. Add a rule:
   - **Name:** `AllowAllGroupMembers` (or per-user)
   - **Start IP:** `0.0.0.0`
   - **End IP:** `255.255.255.255`
   ✅ (or specify exact teammate IPs for more security)
4. Enable **“Allow Azure Services”** if visible
5. Click **Save**

---

## ✅ PART 3 — Connect via Azure Data Studio (Mac & Windows)

### 4. Install Azure Data Studio
Download:  
https://learn.microsoft.com/sql/azure-data-studio/download

### 5. Connect to the database
In Azure Data Studio:
- Click **New Connection**
- Enter:
  - **Server:** `yourservername.database.windows.net`
  - **Database:** `UnemploymentAnalyticsDB`
  - **Auth Type:** SQL Login
  - **Username/Password:** the ones you created
- Click **Connect**

---

## ✅ PART 4 — Run the SQL Script to Build Tables & Data

### 6. Run your `.sql` file
Open the script file (e.g. `UnemploymentAnalyticsDB_clean_with_full_seeds.sql`) in Azure Data Studio.

Click **Run** ▶️

Make sure:
- No red errors appear
- All tables are created

---

## ✅ PART 5 — Verify Tables and Seed Data

Run the following in Azure Data Studio:

```sql
SELECT COUNT(*) FROM dbo.dim_date;
SELECT COUNT(*) FROM dbo.dim_geography;
SELECT COUNT(*) FROM dbo.dim_industry;
SELECT COUNT(*) FROM dbo.dim_demographics;
SELECT TOP 10 * FROM dbo.unemployment_statistics;
```

You should see row counts and sample rows confirming data was inserted correctly.

---

## ✅ PART 6 — Connect Power BI Desktop to Azure SQL

> ⚠️ Power BI Desktop is **Windows-only**. Mac users can collaborate via shared datasets, Parallels, or a VM.

### 7. Steps in Power BI Desktop:
1. Open Power BI Desktop
2. Click **Get Data → SQL Server**
3. Enter:
   - **Server:** `yourservername.database.windows.net`
   - **Database:** `UnemploymentAnalyticsDB`
4. For authentication:
   - Choose **Database Authentication**
   - Enter the same username/password
5. Click **OK** → Choose tables → **Load**

Now you can build reports and visuals from the shared Azure SQL database.

---

## ✅ PART 7 — Let Your Group Members Connect

Share this info with teammates:
- **Server:** `yourservername.database.windows.net`
- **Database:** `UnemploymentAnalyticsDB`
- **Username & Password**
- Their IP must be allowed (update firewall rules if needed)

If someone cannot connect:
1. Get their public IP
2. Add it in the Azure Portal under Firewall Rules

---

## ✅ Optional Next Steps
You can extend this setup with:
- A “teammate setup” guide
- Automated firewall IP updates
- Fact data loading helpers
- Power BI views and measures

Just ask when you're ready!
