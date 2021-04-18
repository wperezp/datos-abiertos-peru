FROM public.ecr.aws/lambda/python:3.8

ADD . ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r requirements.txt

CMD [ "main.lambda_handler" ]