FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y git

COPY tools/ tools/
COPY requirements.txt .
COPY process_report/process_report.py process_report/
COPY process_report/institute_map.json process_report/

RUN pip install -r requirements.txt

CMD ["tools/clone_nonbillables_and_process_invoice.sh"]
