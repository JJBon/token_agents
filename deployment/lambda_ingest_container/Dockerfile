FROM public.ecr.aws/lambda/python:3.11

# Install Python dependencies from precompiled wheels only
COPY requirements.txt .
RUN pip install --only-binary=:all: -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}

# Lambda entrypoint
CMD ["app.lambda_handler"]
