# Public ETL Demo: Vehicle Sensors (comma2k19)
### Author: Rocio Radulescu 

A small, reproducible ETL that reads **public vehicle sensor data** from the
Hugging Face Datasets API (**commaai/comma2k19**), saves a **raw**
layer, performs tidy **transforms**, computes **metrics**, and writes **gold** layer with
outputs.

> Why this dataset?
> - It’s **public** and accessible via API (no credentials needed).
> - It contains **GNSS, IMU, CAN, pose** style signals, close to real telematics.
>
> References:
> - Dataset card (API entry): https://huggingface.co/datasets/commaai/comma2k19  
> - Streaming docs (HF Datasets API): https://huggingface.co/docs/datasets/en/stream  
> - Paper (background on signals): https://arxiv.org/abs/1812.05752

---

## What the pipeline does

1. **Extract** (via API)  
   - Streams records from `commaai/comma2k19` using the HF **Datasets** Python SDK.  
   - Saves each record to **RAW** as newline-delimited JSON (**JSONL**).  
   - Ref: HF streaming API — https://huggingface.co/docs/datasets/en/stream

2. **Transform**  
   - Parses RAW JSONL to tidy **pandas** DataFrames by modality (**gnss**, **imu**, **can**, **pose**).  
   - Normalizes timestamps (UTC), coalesces common field names, derives `speed_kph`.  
   - Ref: Dataset card + paper for signal meaning —  
     - https://huggingface.co/datasets/commaai/comma2k19  
     - https://arxiv.org/abs/1812.05752

3. **Metrics (KPIs & Analytics)**  
   - Computes key performance indicators for vehicle sensor data, including:  
     - **Speed statistics**: Daily average and maximum speed, derived from CAN bus signals.  
     - **GNSS signal quality**: Average number of satellites tracked and percentage of time with a 3D position fix, from GNSS data.  
     - **Driving events**: Detection and count of hard acceleration and braking events using IMU longitudinal acceleration (`ax`).  
   - These KPIs are commonly used in fleet management and telematics analytics to assess vehicle usage, location accuracy, and driving behavior.

--- 

## How to Run the Pipeline

Follow these steps to set up your environment and run the ETL pipeline:

1. **Create and activate a virtual environment, then install dependencies**
   ```bash
   python3.13 -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

2. **Extract data (streams public data → RAW)**
   ```bash
   python -m etl_comma2k19.cli extract --limit 2000
   ```

3. **Transform data (RAW → SILVER)**
   ```bash
   python -m etl_comma2k19.cli transform
   ```

4. **Compute metrics (SILVER → GOLD)**
   ```bash
   python -m etl_comma2k19.cli metrics
   ```

5. **Or run the entire pipeline in one step**
   ```bash
   python -m etl_comma2k19.cli all --limit 2000
   ```

