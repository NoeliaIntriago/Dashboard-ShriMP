# Dashboard-ShriMP

# Setup
## venv
Create virtual environment (if created skip this step)

```
    py -m venv .venv
```

Activate virtual environment

```
    .venv\Scripts\activate.bat
```

Install required packages for the project inside venv:

```
    pip install -r requirements.txt 
```

## App

```
    streamlit run 0_📊_Dashboard.py
```

# Docs
Make sure you have some TeX distribution for your OS.

After that:

## On Windows

```
    cd docs
    .\make.bat latexpdf    
```

## On Linux
```
    cd docs
    make latexpdf
```

Then use some LaTeX Editor, upload the required files and get the PDF