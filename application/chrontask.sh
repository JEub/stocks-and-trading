#!/usr/bin/env bash
cd /home/james/Work/Stocks
source venv/bin/activate
python application/assemble_data.py
deactivate
