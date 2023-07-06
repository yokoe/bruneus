FROM python:3.9
WORKDIR /code
COPY . .
RUN pip install -r requirements.txt -r dev-requirements.txt
RUN pip install -e .
CMD ["sh", "run_test.sh"]
